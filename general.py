from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from enum import Enum
from random import random
from threading import Lock, Thread
from time import sleep
from typing import Optional, List, Dict, Callable, Set

import rpyc
from rpyc import ThreadedServer, Service
from rpyc.utils.helpers import classpartial


class State(str, Enum):
    faulty = "faulty"
    nonfaulty = "nonfaulty"


class Order(str, Enum):
    attack = "attack"
    retreat = "retreat"
    undecided = "undecided"


class Actions(str, Enum):
    receive_order = "receive_order"
    receive_primary = "reveive_primary"
    receive_new_node = "reveive_new_node"



@dataclass(frozen=True)
class Message:
    sender: str
    action: str
    value: Optional[str]
    time: int

    def serialize(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def deserialize(cls, serialized: str) -> Message:
        return cls(**json.loads(serialized))


class GeneralLogic:
    def __init__(self, id: str):
        self.id = id
        self.state: State = State.nonfaulty

        self.received_values: Dict[str, Order] = {}

        self.primary_id: Optional[str] = None

        self.other_nodes: Set[str] = set()

        self.order_in_progress = False

        self.communication_callback: Optional[Callable] = None

    def _process_order(self, order: Order) -> str:
        if self.state == State.faulty:
            return Order.attack.value if random() < 0.5 else Order.retreat.value
        return order.value

    def _send_order(self, order: Order):
        for id in self.other_non_primary_nodes:
            self.communication_callback(id,
                                        Message(self.id, Actions.receive_order.value, self._process_order(order), 0))

    @property
    def other_non_primary_nodes(self):
        return {node for node in self.other_nodes if node != self.primary_id}

    @property
    def majority(self) -> Order:
        assert len(self.received_values) > 0
        values = {}
        for v in self.received_values.values():
            values[v] = 0 if v not in values else values[v] + 1

        top = tuple(reversed(sorted(values.items(), key=lambda x: x[1])))
        if len(top) == 1:
            return Order(top[0][0])

        if top[0][1] == top[1][1]:
            return Order.undecided

    def receive_message(self, message: Message) -> Optional[Message]:
        if message.action == Actions.receive_order:
            print(self.id, "received", message.value, "from", message.sender)
            if message.sender == self.primary_id:
                primary_order = Order(message.value)
                self.received_values[message.sender] = primary_order
                self._send_order(primary_order)
            else:
                self.received_values[message.sender] = Order(message.value)

            if len(self.received_values) == len(self.other_nodes):
                if self.id == self.primary_id:
                    self.order_in_progress = False
                else:
                    self.communication_callback(self.primary_id,
                                                Message(self.id, Actions.receive_order.value, self.majority.value, 0))

        elif message.action == Actions.receive_primary:
            self.primary_id = message.value
        elif message.action == Actions.receive_new_node:
            self.other_nodes.add(message.value)

        return None

    def send_order(self, order: Order):
        assert self.id == self.primary_id
        self.order_in_progress = True
        self._send_order(order)

        while self.order_in_progress:
            pass
        return self.majority, self.received_values.copy()


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


class General(Thread):
    def __init__(self, id, id_to_port):
        super().__init__()
        self.logic = GeneralLogic(id)
        self.id_to_port = id_to_port
        self.connections = {}
        self.lock = Lock()

    def add_node(self, id, port):
        assert id != self.logic.id
        self.id_to_port[id] = port
        self.logic.other_nodes.add(id)
        self.connections[id] = rpyc.connect("localhost", port)

    def _callback(self, message: Message):
        with self.lock:
            self.logic.receive_message(message)

    def _start_server(self):
        service = classpartial(GeneralService, self._callback)
        server = ThreadedServer(service, port=self.id_to_port[self.logic.id])
        thread = Thread(target=server.start)
        thread.daemon = True
        thread.start()

    def _open_connections(self):
        for id, port in self.id_to_port.items():
            self.connections[id] = rpyc.connect("localhost", port)

    def _send_message(self, id, message):
        return self.connections[id].root.message(message)

    def run(self):
        self._start_server()
        self._open_connections()
        self.logic.communication_callback = self._send_message

    def send_order(self, order):
        self.logic.send_order(order)
