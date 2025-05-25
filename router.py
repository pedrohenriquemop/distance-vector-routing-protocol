import re
import sys
import json
import socket
import argparse
import threading
import time
import logging
import random

PORT = 55151
ACCEPTED_COMMANDS = {"add": "<ip> <weight>", "del": "<ip>", "trace": "<ip>"}
REMOVE_STALE_PERIOD_MULTIPLIER = 4
BUFFER_SIZE = 65536

router_logger = logging.getLogger("router_app")
router_logger.propagate = False


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


class ControlMessage(Message):
    def __init__(self, source: str, destination: str, unreachable: str):
        super().__init__(source, destination, "control")
        self.unreachable = unreachable

    def to_json(self) -> dict:
        msg = super().to_json()
        msg["unreachable"] = self.unreachable
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
            raise Exception(f"Usage: {type} {args_format}")  # Corrected usage string

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

        # Best know routes: {<dest>: (<cost>, [<next_hop>])}
        # List of next_hop's is used to implement load balancing
        self.routes = {self.ip: (0, [self.ip])}
        # Adjacent routers: {<ip>: <weight>}
        self.neighbors: dict[str, int] = {}
        # Timestamp of the last received update from each neighbor: {<ip>: <timestamp>}
        self.last_update: dict[str, float] = {}

        self.lock = threading.Lock()

        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.send_periodic_updates, daemon=True).start()
        threading.Thread(target=self.cleanup_expired_routes, daemon=True).start()

        router_logger.info(
            f"Router {self.ip} started with update period {self.period}s"
        )

    def listen(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                router_logger.debug(
                    f"[{self.ip}] Received raw message from {addr}: {msg}"
                )
                self.handle_message(msg)

            except json.JSONDecodeError:
                router_logger.warning(
                    f"[{self.ip}] Received malformed JSON from {addr}"
                )
            except Exception as e:
                router_logger.error(
                    f"[{self.ip}] Error receiving/handling message: {e}", exc_info=True
                )

    def handle_message(self, msg: dict):
        msg_type = msg.get("type")
        if msg_type == "update":
            self.handle_update_message(msg)
        elif msg_type == "trace":
            self.handle_trace_message(msg)
        elif msg_type == "data":
            self.handle_data_message(msg)
        elif msg_type == "control":
            self.handle_control_message(msg)
        else:
            router_logger.warning(f"[{self.ip}] Unknown message type: {msg_type}")

    def handle_update_message(self, msg: dict):
        sender_ip = msg.get("source")
        if not sender_ip:
            router_logger.warning(
                f"[{self.ip}] Received update message with no source IP: {msg}"
            )
            return

        with self.lock:
            self.last_update[sender_ip] = time.time()

            distances = msg.get("distances", {})
            router_logger.debug(
                f"[{self.ip}] Received update from {sender_ip}: {distances}"
            )

            cost_to_sender = self.neighbors.get(sender_ip)

            if cost_to_sender is None:
                router_logger.debug(
                    f"[{self.ip}] Ignoring update from {sender_ip}: not a direct neighbor."
                )
                return

            for dest, cost_from_sender in distances.items():
                if dest == self.ip:
                    continue

                total_cost = cost_from_sender + cost_to_sender

                current_cost, current_next_hops = self.routes.get(
                    dest, (float("inf"), [])
                )

                if total_cost < current_cost:
                    self.routes[dest] = (total_cost, [sender_ip])
                    router_logger.info(
                        f"[{self.ip}] Updated route to {dest}: Cost {total_cost} via {sender_ip}"
                    )
                elif total_cost == current_cost and sender_ip not in current_next_hops:
                    self.routes[dest][1].append(sender_ip)
                    router_logger.info(
                        f"[{self.ip}] Added equal-cost path to {dest}: Cost {total_cost} via {sender_ip}"
                    )
                elif sender_ip in current_next_hops and total_cost != current_cost:
                    self.routes[dest] = (total_cost, [sender_ip])
                    router_logger.info(
                        f"[{self.ip}] Re-evaluated route to {dest}: Cost {total_cost} via {sender_ip} (same next_hop)"
                    )
                else:
                    router_logger.debug(
                        f"[{self.ip}] No better route for {dest} via {sender_ip}. Current: {current_cost} via {current_next_hops}, Proposed: {total_cost} via {sender_ip}"
                    )

    def handle_trace_message(self, msg: dict):
        trace_routers = msg.get("routers", [])
        trace_routers.append(self.ip)
        msg["routers"] = trace_routers

        dest_ip = msg.get("destination")

        router_logger.info(
            f"[{self.ip}] Received trace message from {msg['source']} to {dest_ip}. Path: {msg['routers']}"
        )
        if dest_ip == self.ip:
            response = DataMessage(self.ip, msg["source"], msg).to_json()
            router_logger.debug(
                f"[{self.ip}] Trace destination reached. Sending response to {msg['source']}."
            )
            self.forward(response)
        else:
            router_logger.debug(f"[{self.ip}] Trace message not for self, forwarding.")
            self.forward(msg)

    def handle_data_message(self, msg: dict):
        dest_ip = msg.get("destination")
        if dest_ip == self.ip:
            print(msg.get("payload"))
            router_logger.info(
                f"[{self.ip}] Delivered data message for self. Payload printed."
            )
        else:
            router_logger.debug(
                f"[{self.ip}] Data message not for self, forwarding to {dest_ip}."
            )
            self.forward(msg)

    def handle_control_message(self, msg: dict):
        dest_ip = msg.get("destination")
        if dest_ip == self.ip:
            router_logger.info(f"[{self.ip}] {msg.get('unreachable')} is unreachable.")
        else:
            router_logger.debug(
                f"[{self.ip}] Control message not for self, forwarding to {dest_ip}."
            )
            self.forward(msg)

    def forward(self, msg: dict):
        dest_ip = msg.get("destination")
        msg_type = msg.get("type")

        with self.lock:
            if dest_ip in self.routes:
                possible_next_hops = self.routes[dest_ip][1]
                next_hop = random.choice(possible_next_hops)

                # If the next_hop is the router itself, it means the routing table
                # incorrectly points back to self for a destination that is not self.
                # This indicates a routing loop or error in route calculation.
                if next_hop == self.ip and dest_ip != self.ip:
                    router_logger.error(
                        f"[{self.ip}] Routing error: next hop for {dest_ip} is self ({self.ip}) but it's not the destination. Dropping."
                    )
                    return

                if next_hop in self.neighbors:
                    router_logger.debug(
                        f"[{self.ip}] Forwarding message for {dest_ip} via next hop {next_hop}"
                    )
                    self.__send(msg, next_hop)
                else:
                    router_logger.warning(
                        f"[{self.ip}] Cannot forward to {dest_ip}: next hop {next_hop} is not a known neighbor. Dropping."
                    )
            else:
                if msg_type != "control":
                    original_source = msg["source"]

                    if original_source in self.routes:
                        control_msg_obj = ControlMessage(
                            self.ip, original_source, dest_ip
                        )
                        router_logger.info(
                            f"[{self.ip}] Destination {dest_ip} not reachable. Sending control message back to {original_source}."
                        )
                        possible_next_hops_for_control = self.routes[original_source][1]
                        next_hop_for_control = random.choice(
                            possible_next_hops_for_control
                        )
                        self.__send(control_msg_obj.to_json(), next_hop_for_control)
                    else:
                        router_logger.warning(
                            f"[{self.ip}] Destination {dest_ip} unreachable, but original source {original_source} is also unreachable by this router. Dropping control message."
                        )
                else:
                    router_logger.warning(
                        f"[{self.ip}] Received control message for unreachable destination {dest_ip}. Cannot forward. Dropping to prevent loop."
                    )

    def __send(self, msg: dict, next_hop_ip: str):
        data = json.dumps(msg).encode()

        try:
            if not next_hop_ip:
                router_logger.error(
                    f"[{self.ip}] Attempted to send with empty next hop IP."
                )
                raise ValueError("Next hop IP cannot be empty")

            self.sock.sendto(data, (next_hop_ip, PORT))
            router_logger.debug(
                f"[{self.ip}] Successfully sent message (type: {msg.get('type')}) to {next_hop_ip}"
            )
        except Exception as e:
            router_logger.error(
                f"[{self.ip}] Error sending UDP packet to {next_hop_ip}: {e}",
                exc_info=True,
            )

    def trace(self, ip: str):
        msg = TraceMessage(self.ip, ip, [self.ip])
        router_logger.info(f"[{self.ip}] CLI: Initiating trace to {ip}.")
        self.forward(msg.to_json())

    def send_periodic_updates(self):
        while True:
            with self.lock:
                for neighbor_ip in self.neighbors:
                    # Split Horizon: do not advertise routes learned from the neighbor back to it
                    distances_to_advertise = {
                        dest: cost
                        for dest, (cost, next_hops) in self.routes.items()
                        if neighbor_ip not in next_hops
                        and dest
                        != self.ip  # Do not advertise route to self, or routes learned from this neighbor
                    }

                    # Always advertise the direct route to self with cost 0, for neighbors to learn how to reach me.
                    if self.ip not in distances_to_advertise:
                        distances_to_advertise[self.ip] = 0

                    msg = UpdateMessage(self.ip, neighbor_ip, distances_to_advertise)
                    router_logger.debug(
                        f"[{self.ip}] Sending update to {neighbor_ip}: {distances_to_advertise}"
                    )
                    self.__send(msg.to_json(), neighbor_ip)

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
                    router_logger.info(
                        f"[{self.ip}] Stale neighbor {ip} detected (no updates for >{REMOVE_STALE_PERIOD_MULTIPLIER * self.period}s). Removing routes."
                    )
                    self.del_neighbor(ip)

            time.sleep(self.period)

    def startup(self, startup_file: str):
        try:
            with open(startup_file, "r") as f:
                router_logger.info(
                    f"[{self.ip}] Processing startup file: {startup_file}"
                )
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        try:
                            parsed_command = CLICommand(line)
                            if parsed_command.type == "add":
                                self.add_neighbor(
                                    parsed_command.args.get("ip"),
                                    int(parsed_command.args.get("weight")),
                                )
                            elif parsed_command.type == "del":
                                self.del_neighbor(parsed_command.args.get("ip"))
                            router_logger.debug(
                                f"[{self.ip}] Executed startup command: {line}"
                            )
                        except Exception as e:
                            router_logger.error(
                                f"[{self.ip}] Error processing startup command '{line}': {e}",
                                exc_info=True,
                            )
        except FileNotFoundError:
            router_logger.critical(
                f"[{self.ip}] Startup file not found: {startup_file}. Exiting."
            )
            sys.exit(1)

    def add_neighbor(self, ip: str, weight: int):
        try:
            if weight < 0:
                raise ValueError("Weight must be non-negative")

            with self.lock:
                self.neighbors[ip] = weight
                self.routes[ip] = (weight, [ip])
                router_logger.info(
                    f"[{self.ip}] Added direct link to {ip} with weight {weight}"
                )
        except ValueError as e:
            router_logger.error(f"[{self.ip}] Invalid weight for {ip}: {e}")
        except Exception as e:
            router_logger.error(
                f"[{self.ip}] Error adding neighbor {ip}: {e}", exc_info=True
            )

    def del_neighbor(self, ip: str):
        try:
            with self.lock:
                if ip in self.neighbors:
                    self.neighbors.pop(ip, None)
                    # Rebuild routes, removing the deleted neighbor from any next_hop lists
                    new_routes = {}
                    for dest, (cost, next_hops_list) in self.routes.items():
                        if dest == ip:
                            continue

                        filtered_next_hops = [nh for nh in next_hops_list if nh != ip]

                        if filtered_next_hops:
                            new_routes[dest] = (cost, filtered_next_hops)

                    self.routes = new_routes

                    self.last_update.pop(ip, None)
                    router_logger.info(
                        f"[{self.ip}] Removed direct link to {ip} and its associated routes."
                    )
                else:
                    router_logger.warning(
                        f"[{self.ip}] No direct link to {ip} to delete."
                    )
        except Exception as e:
            router_logger.error(
                f"[{self.ip}] Error while trying to delete neighbor {ip}: {e}",
                exc_info=True,
            )

    def run(self):
        try:
            while True:
                command = input().strip().lower()

                if command == "quit":
                    router_logger.info(
                        f"[{self.ip}] CLI: Terminating router by user command."
                    )
                    break

                try:
                    parsed_command = CLICommand(command)
                    cmd_type, args = parsed_command.type, parsed_command.args

                    if cmd_type == "add":
                        self.add_neighbor(args.get("ip"), int(args.get("weight")))

                    elif cmd_type == "del":
                        self.del_neighbor(args.get("ip"))

                    elif cmd_type == "trace":
                        self.trace(args.get("ip"))

                except Exception as e:
                    router_logger.error(
                        f"[{self.ip}] Error executing CLI command '{command}': {e}",
                        exc_info=True,
                    )
        except KeyboardInterrupt:
            router_logger.info(
                f"[{self.ip}] Terminating router by KeyboardInterrupt (Ctrl+C)."
            )
        finally:
            self.sock.close()
            router_logger.info(f"[{self.ip}] Router socket closed. Exiting.")


def terminate_program():
    router_logger.info("Program terminated cleanly.")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="UDPRIP Router Emulator")
    parser.add_argument("ip_address", help="IP address on which the router will bind")
    parser.add_argument("period", type=float, help="Update period in seconds (π)")
    parser.add_argument("startup", help="Startup file (optional)", nargs="?")
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging output to console."
    )

    args = parser.parse_args()

    for handler in router_logger.handlers[:]:
        router_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(ip)s] %(message)s"
    )
    console_handler.setFormatter(formatter)

    if args.debug:
        router_logger.addHandler(console_handler)
        router_logger.setLevel(logging.DEBUG)
        router_logger.debug(f"Debug logging enabled for router {args.ip_address}.")
    else:
        router_logger.setLevel(logging.CRITICAL)
        router_logger.addHandler(logging.NullHandler())

    router = Router(args.ip_address, args.period)

    if args.startup:
        router.startup(args.startup)

    router.run()

    terminate_program()


if __name__ == "__main__":
    main()
