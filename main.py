import argparse
import json
import sys
from itertools import count
from typing import Optional, Dict

import rpyc

from general import Order, Message, General, Actions, State


class Client:
    def __init__(self, primary_port: Optional[int] = None, primary_id: Optional[int] = None):
        self.primary_port = primary_id
        self.primary_id = primary_port

    def set_primary(self, id: int, port: int):
        self.primary_id = id
        self.primary_port = port

    def send_order(self, order: str):
        with rpyc.connect("localhost", self.primary_port) as conn:
            votes = json.loads(
                Message.deserialize(
                    conn.root.message(Message(client_id, Actions.client_order, order).serialize())
                ).value
            )
            votes = {int(k): v for k, v in votes.items()}
        decision = votes[0]
        del votes[0]

        return decision, votes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Faulty General Detection')
    parser.add_argument('n_procs', metavar='N', type=int, help="The number of processes to create.")
    parser.add_argument('--starting-port', type=int, default=18812, help="First port of the processes.")

    args = parser.parse_args()
    N = args.n_procs
    if N < 1:
        raise Exception("N must be greater than 0")

    starting_port = args.starting_port
    client_id = 0  # reserved for client

    process_ports = tuple(starting_port + i for i in range(N))

    ports = count(starting_port + N + 1)
    ids = count(N + 1)

    id_to_port = {i: port for i, port in enumerate(process_ports, start=1)}
    primary_id = min(id_to_port)
    client = Client(primary_id, id_to_port[primary_id])

    generals = {}
    for id, port in id_to_port.items():
        gen = General(id, id_to_port.copy(), primary_id)
        gen.start()
        generals[id] = gen

    faulty_nodes = set()


    def remove_node(id: int, id_to_port: Dict[int, int]):
        if id not in id_to_port:
            print("A general with this id does not exist")
            return

        if len(id_to_port) == 1:
            print("Can't remove the requested node. The system must have at least 1 node.")
            return

        for _, general in generals.items():
            general.remove_node(id)
        generals[id].stop()

        del generals[id]
        del id_to_port[id]
        if id in faulty_nodes:
            faulty_nodes.remove(id)

        if id == client.primary_id:
            new_primary_id = min(generals)
            client.set_primary(new_primary_id, id_to_port[new_primary_id])


    def add_node(id_to_port: Dict[int, int]):
        port = next(ports)
        id = next(ids)

        id_to_port[id] = port
        gen = General(id, id_to_port.copy(), client.primary_id)
        gen.start()

        for general in generals.values():
            general.add_node(id, port)
        generals[id] = gen


    def send_order(order):
        actual_order, node_majorities = client.send_order(order)
        if len(faulty_nodes) * 3 + 1 > len(generals):
            print("Warning:  3k + 1 requirement for Byzantine Agreement not fulfilled.")
        if actual_order is None:
            print(f"Execute order: cannot be determined – not enough generals in the system! "
                  f"{len(faulty_nodes)} faulty node(s) in the system - "
                  f"{len(generals) // 2 + 1} out of {len(generals)} quorum not consistent")
        else:
            print(f"Execute order: {actual_order}! "
                  f"{len(faulty_nodes)} faulty node(s) in the system – "
                  f"{len(generals) // 2 + 1} out of {len(generals)} quorum suggest {actual_order}")
        print_system(node_majorities)


    def set_state(id: int, state: str):
        if state == "Faulty":
            faulty_nodes.add(id)
            generals[id].state = State.faulty
        elif state == "Non-faulty":
            if id in faulty_nodes:
                faulty_nodes.remove(id)
            generals[id].state = State.nonfaulty
        else:
            print("Invalid state. The state must be either Faulty or Non-faulty (case-sensitive).")


    def print_system(majority=None):
        for id, general in sorted(generals.items()):
            if majority is not None and id in majority:
                majority_value = "undefined" if majority[id] is None else majority[id]
                majority_text = f" majority={majority_value},"
            else:
                majority_text = ""

            role_text = 'primary' if general.id == general.primary_id else 'secondary'
            print(f"G{id}, {role_text},{majority_text} state={general.state.value}")



    while True:
        command, *arguments = input("Input command: ").split(" ")

        try:
            if len(arguments) > 2:
                print("Too many arguments")
            elif command == "actual-order":
                send_order(Order(arguments[0]))
            elif command == "g-kill":
                remove_node(int(arguments[0]), id_to_port)
                print_system()
            elif command == "g-add":
                for _ in range(int(arguments[0])):
                    add_node(id_to_port)
                print_system()
            elif command == "g-state":
                if len(arguments) > 0:
                    set_state(int(arguments[0]), arguments[1])
                print_system()
            elif command == "exit":
                sys.exit(0)
            else:
                print("Unknown command")
        except Exception as e:
            print(e)
