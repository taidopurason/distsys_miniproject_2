from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from enum import Enum
from random import random
from threading import Lock, Thread
from time import sleep
from typing import Optional, List, Dict, Callable, Set, Iterable

from collections import Counter

import rpyc
from rpyc import ThreadedServer, Service
from rpyc.utils.helpers import classpartial


class State(str, Enum):
    faulty = "F"
    nonfaulty = "NF"


class Order(str, Enum):
    attack = "attack"
    retreat = "retreat"
    undecided = "undecided"


class Actions(str, Enum):
    order = "order"
    client_order = "client_order"
    response = "response"


@dataclass(frozen=True)
class Message:
    sender: int
    action: str
    value: Optional[str] = None

    def serialize(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def deserialize(cls, serialized: str) -> Message:
        return cls(**json.loads(serialized))


def majority(received_values: Iterable) -> Optional[str]:
    top2 = Counter(received_values).most_common(2)

    if len(top2) == 0 or len(top2) > 1 and top2[0][1] == top2[1][1]:
        return None

    return top2[0][0]


class General(Thread):
    def __init__(self, id: int, id_to_port: Dict[int, int],
                 primary_id: Optional[int] = None):
        super().__init__()
        self.id = id
        self.id_to_port = id_to_port
        self.primary_id = primary_id

        self.communication_callback: Optional[Callable] = None
        self.received_values: Dict[int, str] = {}
        self.state: State = State.nonfaulty

        self.order_in_progress = False
        self.connections = {}
        self.lock = Lock()
        self.ready = False
        self.server = None

    @property
    def other_nodes(self) -> Set[int]:
        return set(self.id_to_port.keys()) - {self.id}

    @property
    def port(self) -> int:
        return self.id_to_port[self.id]

    def _process_order(self, order: str) -> str:
        if self.state == State.faulty:
            return Order.attack.value if random() < 0.5 else Order.retreat.value
        return order

    def _send_order(self, order: str):
        non_primary_other_nodes = self.other_nodes - {self.primary_id}
        for id in non_primary_other_nodes:
            self.communication_callback(id,
                                        Message(self.id, Actions.order.value, self._process_order(order)))
        if len(non_primary_other_nodes) == 0:
            self.order_in_progress = False

    def _handle_order(self, message: Message):
        if message.sender == self.primary_id:
            primary_order = message.value
            self.received_values[message.sender] = primary_order
            self._send_order(primary_order)
        else:
            self.received_values[message.sender] = message.value

        if len(self.received_values) == len(self.other_nodes):
            if self.id == self.primary_id:
                self.order_in_progress = False
            else:
                self.communication_callback(
                    self.primary_id,
                    Message(self.id, Actions.order.value, majority(self.received_values.values()))
                )
                self.received_values = {}

    def handle_message(self, message: Message) -> Optional[Message]:
        # wait for the node to be fully set up
        while not self.ready:
            sleep(0.1)

        if message.action == Actions.order:
            with self.lock:
                self._handle_order(message)

        elif message.action == Actions.client_order:
            order = message.value
            self.order_in_progress = True
            self._send_order(order)
            while self.order_in_progress:
                sleep(0.1)

            majorities = {**self.received_values, self.id: order}
            self.received_values = {}
            return Message(self.id, Actions.response, json.dumps(majorities))

        return None

    def _start_server(self):
        service = classpartial(GeneralService, self.handle_message)
        self.server = ThreadedServer(service, port=self.port)
        thread = Thread(target=self.server.start)
        thread.daemon = True
        thread.start()

    def _open_connections(self):
        for id, port in self.id_to_port.items():
            self.connections[id] = rpyc.connect("localhost", port)

    def _send_message(self, id: str, message: Message):
        reponse = self.connections[id].root.message(message.serialize())
        if reponse is not None:
            return Message.deserialize(reponse)
        return None

    def stop(self):
        if id in self.connections:
            self.connections[id].close()
        self.server.close()


    def add_node(self, id, port):
        assert id != self.id
        self.ready = False
        self.id_to_port[id] = port
        self.connections[id] = rpyc.connect("localhost", port)
        self.ready = True

    def remove_node(self, id):
        if id in self.connections:
            self.connections[id].close()
            del self.connections[id]
        if id in self.id_to_port:
            del self.id_to_port[id]
        if id == self.primary_id:  # automatically appoint new primary id
            self.primary_id = min(self.id_to_port.keys())

    def run(self):
        self._start_server()
        self._open_connections()
        self.communication_callback = self._send_message
        self.ready = True


class GeneralService(Service):
    def __init__(self, callback: Callable):
        self.callback = callback

    def exposed_message(self, message: str):
        response = self.callback(Message.deserialize(message))
        if isinstance(response, Message):
            return response.serialize()
        if response is None:
            return None
        else:
            raise Exception("Invalid response type")
