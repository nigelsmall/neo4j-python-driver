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
from collections import Mapping, Sequence
from json import JSONDecoder
from re import compile as re_compile

from neotime import Date, Time, DateTime, Duration, \
    DATE_ISO_PATTERN, TIME_ISO_PATTERN, DURATION_ISO_PATTERN

from neo4j.types.graph import Graph
from neo4j.types.spatial import hydrate_point


DATETIME_ISO_PATTERN = re_compile(DATE_ISO_PATTERN.pattern[:-1] + r'.' + TIME_ISO_PATTERN.pattern[1:])


class JoltDecoder(JSONDecoder):

    def __init__(self, *args, **kwargs):
        # TODO: something with original object_hook
        kwargs["object_hook"] = self._object_hook
        super(JoltDecoder, self).__init__(*args, **kwargs)
        self.graph = None

    def _object_hook(self, o):
        if len(o) == 1:
            k, v = list(o.items())[0]
            if k == "Z":
                return int(v)
            elif k == "R":
                return float(v)
            elif k == "T":
                return self._temporal_hook(v)
            elif k.startswith("@"):
                try:
                    srid = int(k[1:])
                except TypeError:
                    raise JoltDecodeError(o)
                else:
                    return self._spatial_hook(srid, v)
            elif k == "#":
                return bytearray(int(v[x:(x + 2)], 16) for x in range(0, len(v), 2))
            elif k == "{}":
                return dict(v)
            elif k == "G":
                return self._graph_hook(v)
            else:
                return o
        else:
            return o

    def _spatial_hook(self, srid, content):
        try:
            k, v = list(content.items())[0]
        except (IndexError, KeyError):
            raise JoltDecodeError(content)
        if k == "POINT":
            return hydrate_point(srid, *v)
        else:
            raise JoltDecodeError(k)

    def _temporal_hook(self, s):
        if DATE_ISO_PATTERN.match(s):
            return Date.from_iso_format(s)
        elif TIME_ISO_PATTERN.match(s):
            return Time.from_iso_format(s)
        elif DATETIME_ISO_PATTERN.match(s):
            return DateTime.from_iso_format(s)
        elif DURATION_ISO_PATTERN.match(s):
            return Duration.from_iso_format(s)
        else:
            raise JoltDecodeError("Unrecognized temporal format for value %r" % s)

    def _graph_hook(self, g):
        """

        // Element -- G({})
        {"G": {"1": [["Person"], {"name": "Alice"}]]}}
        {"G": {"12": ["KNOWS", {"since": 1999}, "1", "2"]]}}

        // Subgraph -- G([{}, {}])
        {"G": [
          {
            "1": [["Person"], {"name": "Alice"}]],
            "2": [["Person"], {"name": "Bob"}]]
          },
          {
            "12": ["KNOWS", {"since": 1999}, "1", "2"]
          },
          ["1", "12"]
        ]}

        // Path -- G([{}, {}, []])
        {"G": [
          {
            "1": [["Person"], {"name": "Alice"}]],
            "2": [["Person"], {"name": "Bob"}]]
          },
          {
            "12": ["KNOWS", {"since": 1999}, "1", "2"]
          },
          ["1", "12"]
        ]}

        :param g:
        :return:
        """
        if self.graph is None:
            self.graph = Graph()
        if isinstance(g, Mapping):
            # single element
            pass
        elif isinstance(g, Sequence):
            # subgraph/path
            pass
        else:
            raise JoltDecodeError("Cannot decode graph value %r" % g)


class JoltDecodeError(ValueError):

    pass
