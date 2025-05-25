import re
import signal
import sys
import json
import socket
import argparse
import threading
import time

PORT = 55151
ACCEPTED_COMMANDS = {"add": "<ip> <weight>", "del": "<ip>", "trace": "<ip>"}
REMOVE_STALE_PERIOD_MULTIPLIER = 4
BUFFER_SIZE = 65536  # 2 bytes


class Utils:
    @staticmethod
    def json_as_str(data: object | str):
        if isinstance(data, str):
            return data

        return json.dumps(data)


class Message:
    def __init__(self, source: str, destination: str, type: str):
        self.source = source
        self.destination = destination
        self.type = type

    def to_json(self) -> dict:
        return {
            "type": self.type,
            "source": self.source,
            "destination": self.destination,
        }


class DataMessage(Message):
    def __init__(self, source: str, destination: str, payload: object | str):
        super().__init__(source, destination, "data")
        self.payload = Utils.json_as_str(payload)

    def to_json(self) -> dict:
        msg = super().to_json()
        msg["payload"] = self.payload
        return msg


class UpdateMessage(Message):
    def __init__(self, source: str, destination: str, distances: dict[str, int]):
        super().__init__(source, destination, "update")
        self.distances = distances

    def to_json(self) -> dict:
        msg = super().to_json()
        msg["distances"] = self.distances
        return msg


class TraceMessage(Message):
    def __init__(self, source: str, destination: str, routers: list[str]):
        super().__init__(source, destination, "trace")
        self.routers = routers

    def to_json(self) -> dict:
        msg = super().to_json()
        msg["routers"] = self.routers
        return msg


class CLICommand:
    def __init__(self, command: str):
        self.type, self.args = self.__parse_command(command)

    def __parse_command(self, command: str):
        if not command:
            raise Exception("Empty command")
        type, *args = command.split(" ")

        args_format = ACCEPTED_COMMANDS.get(type)

        if not args_format:
            raise Exception(
                f"Invalid command type. Available commands: {", ".join(ACCEPTED_COMMANDS.keys())}"
            )

        expected_args_len = len(args_format.split(" "))

        if expected_args_len != len(args):
            raise Exception(f"Usage: {args_format}")

        pattern = r"<(\w+)>"
        arg_names = re.findall(pattern, args_format)

        return (type, dict(zip(arg_names, args)))

    def __str__(self):
        return str(self.__dict__)


class Router:
    def __init__(self, ip_address: str, update_period: float):
        self.ip = ip_address
        self.period = update_period

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, PORT))

        # Best know routes: {<dest>: (<cost>, <next_hop>)}
        self.routes = {self.ip: (0, self.ip)}
        # Adjacent routers: {<ip>: <weight>}
        self.neighbors: dict[str, int] = {}
        # Timestamp of the last received update from each neighbor: {<ip>: <timestamp>}
        self.last_update: dict[str, float] = {}

        self.lock = threading.Lock()

        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.send_periodic_updates, daemon=True).start()
        threading.Thread(target=self.cleanup_expired_routes, daemon=True).start()

    def listen(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                self.handle_message(msg)

            except json.JSONDecodeError:
                print(f"Received malformed JSON from {addr}")
            except Exception as e:
                print(f"Error receiving/handling message: {e}")

    def handle_message(self, msg: dict):
        type = msg.get("type")
        if type == "update":
            self.handle_update_message(msg)
        elif type == "trace":
            self.handle_trace_message(msg)
        elif type == "data":
            self.handle_data_message(msg)
        else:
            # LOG
            print(f"Unknown message type: {type}")

    def handle_update_message(self, msg: dict):
        sender_ip = msg.get("source")
        if not sender_ip:
            return

        with self.lock:
            self.last_update[sender_ip] = time.time()

            distances = msg.get("distances", {})
            # LOG
            print(f"Received update from {sender_ip}: {distances}")

            cost_to_sender = self.neighbors.get(sender_ip)

            if cost_to_sender is None:
                return

            for dest, cost_from_sender in distances.items():
                if dest == self.ip:
                    continue

                total_cost = cost_from_sender + cost_to_sender

                current_cost, current_next_hop = self.routes.get(
                    dest, (float("inf"), None)
                )

                if (total_cost < current_cost) or (
                    current_next_hop == sender_ip and total_cost != current_cost
                ):
                    self.routes[dest] = (total_cost, sender_ip)

    def handle_trace_message(self, msg: dict):
        trace_routers = msg.get("routers", [])
        trace_routers.append(self.ip)
        msg["routers"] = trace_routers

        dest_ip = msg.get("destination")

        # LOG
        print(f"Received trace message from {msg['source']} to {dest_ip}")
        if dest_ip == self.ip:
            response = DataMessage(self.ip, msg["source"], msg).to_json()
            self.forward(response)
        else:
            self.forward(msg)

    def handle_data_message(self, msg: dict):
        dest_ip = msg.get("destination")
        if dest_ip == self.ip:
            print(msg.get("payload"))
        else:
            self.forward(msg)

    def forward(self, msg: dict):
        dest_ip = msg.get("destination")
        if dest_ip in self.routes:
            next_hop = self.routes[dest_ip][1]

            if next_hop == self.ip:
                print(
                    f"Routing error: next hop for {dest_ip} is self ({self.ip}) but not destination."
                )
                return

            if next_hop in self.neighbors:
                # LOG
                print(f"Forwarding message for {dest_ip} via next hop {next_hop}")
                self.__send(msg, next_hop)
            else:
                print(
                    f"Cannot forward to {dest_ip}: next hop {next_hop} is not a known neighbor."
                )
        else:
            # LOG
            print(f"Destination {dest_ip} not reachable. Dropping message.")
            # TODO: add control message logic (for extra credit)

    def __send(self, msg: dict, dest_ip: str):
        data = json.dumps(msg).encode()

        try:
            if not dest_ip:
                raise ValueError("Destination IP cannot be empty")

            self.sock.sendto(data, (dest_ip, PORT))
        except Exception as e:
            print(f"Error sending message to {dest_ip}: {e}")

    def trace(self, ip: str):
        msg = TraceMessage(self.ip, ip, [self.ip])
        self.forward(msg.to_json())

    def send_periodic_updates(self):
        while True:
            with self.lock:
                for neighbor_ip in self.neighbors:
                    # Split Horizon: do not advertise routes learned from the neighbor back to it
                    # i.e.: do not advertise neighbor if the route already passes through it
                    distances_to_advertise = {
                        dest: cost
                        for dest, (cost, next_hop) in self.routes.items()
                        if next_hop != neighbor_ip and dest != self.ip
                    }

                    if self.ip not in distances_to_advertise:
                        distances_to_advertise[self.ip] = 0

                    msg = UpdateMessage(self.ip, neighbor_ip, distances_to_advertise)
                    self.forward(msg.to_json())

            time.sleep(self.period)

    def cleanup_expired_routes(self):
        while True:
            now = time.time()
            with self.lock:
                expired_neighbors = [
                    neighbor
                    for neighbor, last_ts in self.last_update.items()
                    if now - last_ts > REMOVE_STALE_PERIOD_MULTIPLIER * self.period
                    and neighbor in self.neighbors
                ]
                for ip in expired_neighbors:
                    # LOG
                    print(f"Stale neighbor {ip} removed")
                    self.del_neighbor(ip)

            time.sleep(self.period)

    def startup(self, startup_file: str):
        try:
            with open(startup_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        try:
                            parsed_command = CLICommand(line)
                            if parsed_command.type == "add":
                                self.add_neighbor(
                                    parsed_command.args.get("ip"),
                                    parsed_command.args.get("weight"),
                                )
                            elif parsed_command.type == "del":
                                self.del_neighbor(parsed_command.args.get("ip"))
                        except Exception as e:
                            print(f"Error processing startup command '{line}': {e}")
        except FileNotFoundError:
            print(f"Startup file not found: {startup_file}")
            sys.exit(1)

    def add_neighbor(self, ip: str, weight: int | str):
        try:
            w = int(weight)
            if w < 0:
                raise ValueError("Weight must be non-negative")

            with self.lock:
                self.neighbors[ip] = w

                self.routes[ip] = (w, ip)
                # LOG
                print(f"Added link to {ip} with weight {w}")
        except ValueError as e:
            print(f"Invalid weight: {e}")

    def del_neighbor(self, ip: str):
        try:
            with self.lock:
                if ip in self.neighbors:
                    self.neighbors.pop(ip, None)
                    self.routes = {
                        dist: (cost, next_hop)
                        for dist, (cost, next_hop) in self.routes.items()
                        if next_hop != ip
                    }
                    self.last_update.pop(ip, None)
                    # LOG
                    print(f"Removed link to {ip} and its associated routes")
                else:
                    # LOG
                    print(f"No neighbor with ip {ip} to delete")
        except Exception as e:
            print(f"Error while trying to delete neighbor: {e}")

    def run(self):
        try:
            while True:
                command = input().strip().lower()

                if command == "quit":
                    print("Terminating router...")
                    break

                try:
                    parsed_command = CLICommand(command)
                    type, args = parsed_command.type, parsed_command.args

                    if type == "add":
                        self.add_neighbor(args.get("ip"), int(args.get("weight")))

                    elif type == "del":
                        self.del_neighbor(args.get("ip"))

                    elif type == "trace":
                        self.trace(args.get("ip"))

                except Exception as e:
                    print(f"Error executing command: {e}")
        except KeyboardInterrupt:
            print("\nTerminating router...")


def terminate_program():
    print("Program terminated.")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Router emulator CLI")

    parser.add_argument("ip_address", help="IP address on which the router will bind")
    parser.add_argument("period", help="Update period in seconds")
    parser.add_argument("startup", help="Startup file (optional)", nargs="?")

    args = parser.parse_args()

    router = Router(args.ip_address, float(args.period))

    if args.startup:
        router.startup(args.startup)

    router.run()

    terminate_program()


main()
