#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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


from json import JSONEncoder

from neotime import Date, Time, DateTime, Duration

from neo4j.types.graph import Node, Relationship, Path
from neo4j.types.spatial import Point


Z_LO = -(2 ** 31)
Z_HI = (2 ** 31) - 1

POSITIVE_INFINITY = float("+inf")
NEGATIVE_INFINITY = float("-inf")


class JoltEncoder(JSONEncoder):
    always_safe = False

    def default(self, o):
        if isinstance(o, bytearray):
            return self.jolt_byte_array(o)
        elif isinstance(o, Node):
            return self.jolt_node(o)
        elif isinstance(o, Relationship):
            return self.jolt_relationship(o)
        elif isinstance(o, Path):
            return self.jolt_path(o)
        elif isinstance(o, (Date, Time, DateTime, Duration)):
            return self.jolt_temporal(o)
        else:
            return JSONEncoder.default(self, o)

    def encode(self, o):
        return JSONEncoder.encode(self, self.jolt(o))

    def jolt(self, o):
        if o is None or o is True or o is False:
            return o
        elif isinstance(o, int):
            return self.jolt_int(o)
        elif isinstance(o, float):
            return self.jolt_float(o)
        elif isinstance(o, list):
            return self.jolt_list(o)
        elif isinstance(o, dict):
            return self.jolt_dict(o)
        elif isinstance(o, Point):
            return self.jolt_spatial_point(o)
        else:
            return o

    def jolt_int(self, o):
        if self.always_safe or o < Z_LO or o > Z_HI:
            return {"Z": str(o)}
        else:
            return o

    def jolt_float(self, o):
        if o != o:
            return {"R": "NaN"}
        elif o == POSITIVE_INFINITY:
            return {"R": "Infinity"}
        elif o == NEGATIVE_INFINITY:
            return {"R": "-Infinity"}
        elif self.always_safe or (o.is_integer() and Z_LO <= o <= Z_HI):
            return {"R": str(o)}
        else:
            return o

    def jolt_temporal(self, o):
        try:
            s = o.isoformat()
        except AttributeError:
            raise ValueError("Temporal value %r does not have an 'isoformat' attribute")
        else:
            return {"T": s}

    def jolt_spatial_point(self, o):
        return {"@%s" % o.srid: {"POINT": list(o)}}

    def jolt_byte_array(self, o):
        return {"#": "".join("%02X" % b for b in o)}

    def jolt_list(self, o):
        return list(map(self.jolt, o))

    def jolt_raw_dict(self, o):
        return {str(k): self.jolt(v) for k, v in o.items()}

    def jolt_dict(self, o):
        if self.always_safe or len(o) == 1:
            return {"{}": self.jolt_raw_dict(o)}
        else:
            return self.jolt_raw_dict(o)

    def jolt_raw_node(self, o):
        return [self.jolt_int(o.id), self.jolt_list(o.labels), self.jolt_raw_dict(o)]

    def jolt_raw_node_2(self, o):
        return {str(o.id): [
            self.jolt_list(o.labels),
            self.jolt_raw_dict(o),
        ]}

    def jolt_node(self, o):
        return {"G": self.jolt_raw_node_2(o)}

    def jolt_relationship(self, o):
        return {"G": {(str(o.id)): [
            str(type(o).__name__),
            self.jolt_raw_dict(o),
            str(o.start_node.id),
            str(o.end_node.id),
        ]}, }

    def jolt_path(self, o):
        last_node = o.start_node
        nodes = self.jolt_raw_node_2(last_node)
        relationships = {}
        sequence = [str(last_node.id)]
        for r in o.relationships:
            if r.start_node == last_node:
                last_node = r.end_node
            else:
                last_node = r.start_node
            nodes.update(self.jolt_raw_node_2(last_node))
            relationships.update({str(r.id): [
                str(type(r).__name__),
                self.jolt_raw_dict(r),
                str(r.start_node.id),
                str(r.end_node.id),
            ]})
            sequence.append(str(r.id))
        return {"G": [nodes, relationships, sequence]}
