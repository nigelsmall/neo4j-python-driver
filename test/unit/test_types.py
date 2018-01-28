#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from unittest import TestCase

from neo4j.packstream import Structure
from neo4j.v1.result import PackStreamValueSystem


class HydrationTestCase(TestCase):

    def setUp(self):
        self.value_system = PackStreamValueSystem()

    def test_can_hydrate_node_structure(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice, = self.value_system.hydrate([struct])
        assert alice.id == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_hydrating_unknown_structure_returns_same(self):
        struct = Structure(1, b'X')
        struct.append("foo")
        mystery = self.value_system.hydrate(struct)
        assert mystery == struct

    def test_can_hydrate_in_list(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_list, = self.value_system.hydrate([[struct]])
        assert isinstance(alice_in_list, list)
        alice, = alice_in_list
        assert alice.id == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    def test_can_hydrate_in_dict(self):
        struct = Structure(3, b'N')
        struct.append(123)
        struct.append(["Person"])
        struct.append({"name": "Alice"})
        alice_in_dict, = self.value_system.hydrate([{"foo": struct}])
        assert isinstance(alice_in_dict, dict)
        alice = alice_in_dict["foo"]
        assert alice.id == 123
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"
