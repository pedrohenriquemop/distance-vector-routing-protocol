import re
import signal
import sys
import json
import socket


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


class DataMessage(Message):
    def __init__(self, source: str, destination: str, payload: object | str):
        super().__init__(source, destination, "data")
        self.payload = Utils.json_as_str(payload)


class UpdateMessage(Message):
    def __init__(self, source: str, destination: str, distances: dict):
        super().__init__(source, destination, "update")
        self.distances = distances


class TraceMessage(Message):
    def __init__(self, source: str, destination: str, routers: list[str]):
        super().__init__(source, destination, "trace")
        self.routers = routers


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


def terminate_program():
    print("Program terminated.")
    sys.exit(0)


def main():
    try:
        while True:
            command = input("$ ").strip().lower()

            if command == "quit":
                break

            try:
                parsed_command = CLICommand(command)

                print(parsed_command)
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        # to avoid printing ^C in the same line as the "Program terminated."
        print("", flush=True)
        pass

    terminate_program()


main()
