import re
import sys
import json
import socket


ACCEPTED_COMMANDS = {"add": "<ip> <weight>", "del": "<ip>", "trace": "<ip>"}


def parse_command(command: str):
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

    return {"type": type, **dict(zip(arg_names, args))}


def main():
    try:
        while True:
            command = input("$ ").strip().lower()

            if command == "quit":
                break

            try:
                parsed_command = parse_command(command)

                print(parsed_command)
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        pass

    print("Program terminated.")
    sys.exit(0)


main()
