import re
import signal
import sys
import json
import socket
import argparse


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

        def to_json(self):
            return {
                "type": self.type,
                "source": self.source,
                "destination": self.destination,
            }


class DataMessage(Message):
    def __init__(self, source: str, destination: str, payload: object | str):
        super().__init__(source, destination, "data")
        self.payload = Utils.json_as_str(payload)

    def to_json(self):
        msg = super().to_json()
        msg["payload"] = self.payload
        return msg


class UpdateMessage(Message):
    def __init__(self, source: str, destination: str, distances: dict):
        super().__init__(source, destination, "update")
        self.distances = distances

    def to_json(self):
        msg = super().to_json()
        msg["distances"] = self.distances
        return msg


class TraceMessage(Message):
    def __init__(self, source: str, destination: str, routers: list[str]):
        super().__init__(source, destination, "trace")
        self.routers = routers

    def to_json(self):
        msg = super().to_json()
        msg["routers"] = self.routers
        return msg


ACCEPTED_COMMANDS = {"add": "<ip> <weight>", "del": "<ip>", "trace": "<ip>"}


class CLICommand:
    def __init__(self, command: str):
        self.type, *self.args = self.__parse_command(command)

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

        return (type, *dict(zip(arg_names, args)))

    def __str__(self):
        return str(self.__dict__)


class Router:
    def __init__(self, ip_address: str, update_period: float):
        self.ip_address = ip_address
        self.period = update_period

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
                                    parsed_command.args[0], parsed_command.args[1]
                                )
                            elif parsed_command.type == "del":
                                self.del_neighbor(parsed_command.args[0])
                        except Exception as e:
                            print(f"Error processing startup command '{line}': {e}")
        except FileNotFoundError:
            print(f"Startup file not found: {startup_file}")
            sys.exit(1)

    def add_neighbor(self, ip: str, weight: int):
        print(f"TODO: add neighbor with ip '{ip}' and weight {weight}")

    def del_neighbor(self, ip: str):
        print(f"TODO: del neighbor with ip '{ip}'")

    def run(self):
        try:
            while True:
                command = input("$ ").strip().lower()

                if command == "quit":
                    print("Terminating router...")
                    break

                try:
                    parsed_command = CLICommand(command)

                    print(parsed_command)
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
