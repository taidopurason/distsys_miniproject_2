import json
from itertools import count
from typing import Optional, Dict

import rpyc

from general import Order, Message, General, Actions, majority, State


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
                    conn.root.message(Message("client", Actions.client_order, order).serialize())
                ).value
            )
        decision = majority(votes.values())
        return decision, votes


N = 3
client_port = 10010
process_ports = tuple(client_port + i + 1 for i in range(N))

ports = count(client_port + N + 1)
ids = map(lambda x: f"G{x}",count(N))

id_to_port = {f"G{i}": port for i, port in enumerate(process_ports)}
primary_id = min(id_to_port)

generals = {}
for id, port in id_to_port.items():
    gen = General(id, id_to_port.copy(), primary_id)
    gen.start()
    generals[id] = gen

client = Client(primary_id, id_to_port[primary_id])
faulty_nodes = set()


def remove_node(id: str, id_to_port: Dict[str, int]):
    if id not in id_to_port:
        print("A general with this id does not exist")
        return

    for _, general in generals.items():
        general.remove_node(id)
    generals[id].stop()

    del generals[id]
    del id_to_port[id]
    faulty_nodes.remove(id)

    if id == client.primary_id:
        new_primary_id = min(generals)
        client.set_primary(new_primary_id, id_to_port[new_primary_id])


def add_node(id_to_port):
    port = next(ports)
    id = next(ids)

    id_to_port[id] = port
    gen = General(id, id_to_port.copy(), primary_id)
    gen.start()

    for general in generals.values():
        general.add_node(id, port)
    generals[id] = gen


def send_order(order):
    print(client.send_order(order))


def set_state(id, state):
    if state == "Faulty":
        faulty_nodes.add(id)
        generals[id].state = State.faulty
    if state == "Non-faulty":
        faulty_nodes.remove(id)
        generals[id].state = State.nonfaulty


def show_majority(id, majority):
    print(
        f"{id} {'primary' if id == client.primary_id else 'secondary'} majority={majority.value} {'F' if id in faulty_nodes else 'NF'}")


def show_state(id):
    print(
        f"{id} {'primary' if id == client.primary_id else 'secondary'} {'F' if id in faulty_nodes else 'NF'}")


def print_system():
    for id, general in sorted(generals.items()):
        primary = 'primary' if general.id == general.primary_id else 'secondary'
        print(general.id, general.state.value, primary)


while True:
    arguments = input("Input command: ").split(" ")
    command = arguments[0]

    if len(arguments) > 3:
        print("Too many arguments")
    elif command == "actual-order":
        send_order(Order(arguments[1]))
    elif command == "g-kill":
        remove_node(arguments[1], id_to_port)
        print_system()
    elif command == "g-add":
        for _ in range(int(arguments[1])):
            add_node(id_to_port)
        print_system()
    elif command == "g-state":
        set_state(arguments[1], arguments[2])
        print_system()
    else:
        print("Unknown command")
