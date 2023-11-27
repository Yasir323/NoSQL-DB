"""Pyhton implementation of a simple NoSQL Database"""
import ast
import socket
from typing import Union

HOST = "localhost"
PORT = 8000
SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
STATS = {
    "PUT": {"success": 0, "error": 0},
    "GET": {"success": 0, "error": 0},
    "GETLIST": {"success": 0, "error": 0},
    "PUTLIST": {"success": 0, "error": 0},
    "INCREMENT": {"success": 0, "error": 0},
    "APPEND": {"success": 0, "error": 0},
    "DELETE": {"success": 0, "error": 0},
    "STATS": {"success": 0, "error": 0}
}
VALID_COMMANDS = {"PUT", "GET", "PUTLIST", "GETLIST", "INCREMENT", "APPEND", "DELETE", "STATS", "EXIT"}

def handle_put(key, value) -> tuple:
    DATA[key] = value
    return (True, f"Key [{key}] set to [{value}]")


def handle_get(key: str) -> tuple:
    if key not in DATA:
        return (False, f"ERROR: Key [{key}] not found.")
    else:
        return (True, DATA[key])


def handle_putlist(key: str, value: str) -> tuple:
    value = value.split(",")
    return handle_put(key, value)


def handle_getlist(key: str) -> tuple:
    return_value = exists, value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value, list):
        return (False, f"ERROR: Key [{key}] containes non-list value ([{value}])")
    return return_value


def handle_increment(key) -> tuple:
    return_value = exists, value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value, int):
        return (False, f"ERROR: Key [{key}] contains non-int value ([{value}])")
    else:
        DATA[key] = value + 1
        return (True, f"Key [{key}] incremented")


def handle_append(key: str, value: Union[str, int, float, bool]) -> tuple:
    return_value = exists, list_value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(list_value, list):
        return (False, f"ERROR: Key [{key}] contains non-list value ([{value}])")
    else:
        DATA[key].append(value)
        return (True, f"Key [{key}] had value [{value}] appended")


def handle_delete(key: str) -> tuple:
    if key not in DATA:
        return (False, f"ERROR: Key [{key}] not found and could not be deleted")
    else:
        DATA.pop(key)
        return (True, f"Key [{key}] removed successfully.")

def handle_stats() -> tuple:
    return (True, str(STATS))


COMMAND_HANDLERS = {
    "PUT": handle_put,
    "GET": handle_get,
    "GETLIST": handle_getlist,
    "PUTLIST": handle_putlist,
    "INCREMENT": handle_increment,
    "APPEND": handle_append,
    "DELETE": handle_delete,
    "STATS": handle_stats
}

DATA = {}

class CommandNotValidError(Exception):
    pass


class WrongNumberOfArgumentsError(Exception):
    pass


def parse_message(data):
    """Return a tuple containing the command, key, value."""
    data = data.strip().split(";")
    command = data[0].upper()
    if command not in VALID_COMMANDS:
        raise CommandNotValidError("Invalid command")
    if command in ("STATS", "EXIT"):
        if len(data) != 1:
            raise WrongNumberOfArgumentsError("Wrong number of arguments for {}".format(command))
        key = ""
        value = ""
    else:
        if command in ("GET", "GETLIST", "INCREMENT", "DELETE"):
            if len(data) != 2:
                raise WrongNumberOfArgumentsError("Wrong number of arguments for {}".format(command))
            key = data[1]
            value = ""
        else:
            if len(data) != 3:
                raise WrongNumberOfArgumentsError("Wrong number of arguments for {}".format(command))
            key = data[1]
            value = ast.literal_eval(data[2])
    return (command, key, value)


def get_response(command, key, value):
    if command == "STATS":
        response = COMMAND_HANDLERS[command]()
    elif command in ("GET", "GETLIST", "INCREMENT", "DELETE"):
        response = COMMAND_HANDLERS[command](key)
    else:
        response = COMMAND_HANDLERS[command](key, value)
    return response


def update_stats(command, status):
    if status:
        STATS[command]["success"] += 1
    else:
        STATS[command]["error"] += 1


def main():
    SOCKET.bind((HOST, PORT))
    SOCKET.listen(1)
    connection, address = SOCKET.accept()
    while True:
        try:
            command, key, value = parse_message(connection.recv(1024).decode())
            if command == "EXIT":
                break
            response = get_response(command, key, value)
            connection.sendall("{};{}\n".format(response[0], response[1]).encode())
        except WrongNumberOfArgumentsError as err:
            response = (False, str(err))
            connection.sendall("{};{}\n".format(response[0], response[1]).encode())
        except CommandNotValidError as err:
            response = (False, str(err))
            connection.sendall("{};{}\n".format(response[0], response[1]).encode())
        finally:
            if command != "EXIT":
                update_stats(command, response[0])
    connection.close()


if __name__ == "__main__":
    main()
