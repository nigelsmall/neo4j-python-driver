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


from collections import OrderedDict
from unittest import TestCase

from neotime import Date, Time, DateTime
from pytz import timezone

from neo4j.jolt import jolt_dumps, jolt_loads
from neo4j.types.graph import Graph, Path
from neo4j.types.spatial import WGS84Point, CartesianPoint


class JoltCoreTypeEncodingTestCase(TestCase):

    def test_none(self):
        self.assertEqual("null", jolt_dumps(None))

    def test_true(self):
        self.assertEqual("true", jolt_dumps(True))

    def test_false(self):
        self.assertEqual("false", jolt_dumps(False))

    def test_small_integers(self):
        for i in range(-32768, 32768):
            encoded = str(i)
            self.assertEqual(encoded, jolt_dumps(i))

    def test_integers_around_31_bit_positive_boundary(self):
        self.assertEqual('2147483647', jolt_dumps(0x7FFFFFFF))
        self.assertEqual('{"Z": "2147483648"}', jolt_dumps(0x80000000))
        self.assertEqual('{"Z": "2147483649"}', jolt_dumps(0x80000001))

    def test_integers_around_31_bit_negative_boundary(self):
        self.assertEqual('-2147483647', jolt_dumps(-0x7FFFFFFF))
        self.assertEqual('-2147483648', jolt_dumps(-0x80000000))
        self.assertEqual('{"Z": "-2147483649"}', jolt_dumps(-0x80000001))

    def test_small_whole_number_float(self):
        self.assertEqual('{"R": "1.0"}', jolt_dumps(1.0))

    def test_large_whole_number_float(self):
        self.assertEqual('2147483648.0', jolt_dumps(2147483648.0))

    def test_fractional_float(self):
        self.assertEqual('1.5', jolt_dumps(1.5))

    def test_string(self):
        self.assertEqual('"hello, world"', jolt_dumps("hello, world"))

    def test_empty_string(self):
        self.assertEqual('""', jolt_dumps(""))

    def test_byte_array(self):
        self.assertEqual('{"#": "0F1011"}', jolt_dumps(bytearray([15, 16, 17])))

    def test_empty_byte_array(self):
        self.assertEqual('{"#": ""}', jolt_dumps(bytearray()))

    def test_empty_list(self):
        self.assertEqual('[]', jolt_dumps([]))

    def test_singleton_list(self):
        self.assertEqual('[1]', jolt_dumps([1]))

    def test_regular_list(self):
        self.assertEqual('[1, 2, 3]', jolt_dumps([1, 2, 3]))

    def test_nested_lists(self):
        self.assertEqual('[1, [2.1, 2.2, 2.3], 3]', jolt_dumps([1, [2.1, 2.2, 2.3], 3]))

    def test_escaped_values_in_list(self):
        encoded = '[2147483647, {"Z": "2147483648"}, {"Z": "2147483649"}]'
        self.assertEqual(encoded, jolt_dumps([0x7FFFFFFF, 0x80000000, 0x80000001]))

    def test_nested_escaped_values_in_list(self):
        encoded = '[1, [2147483647, {"Z": "2147483648"}, {"Z": "2147483649"}], 3]'
        self.assertEqual(encoded, jolt_dumps([1, [0x7FFFFFFF, 0x80000000, 0x80000001], 3]))

    def test_empty_dict(self):
        self.assertEqual('{}', jolt_dumps({}))

    def test_singleton_dict(self):
        self.assertEqual('{"{}": {"one": 1}}', jolt_dumps({"one": 1}))

    def test_regular_dict(self):
        value = OrderedDict([("one", 1), ("two", 2)])
        self.assertEqual('{"one": 1, "two": 2}', jolt_dumps(value))

    def test_nested_dicts(self):
        value = OrderedDict([("one", 1), ("two", OrderedDict([("one", 2.1), ("two", 2.2)]))])
        self.assertEqual('{"one": 1, "two": {"one": 2.1, "two": 2.2}}', jolt_dumps(value))

    def test_escaped_values_in_dict(self):
        value = OrderedDict([("short", 1), ("long", 0x80000000)])
        encoded = '{"short": 1, "long": {"Z": "2147483648"}}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_list_in_dict(self):
        value = OrderedDict([("short", 1), ("long", [0x80000000, 0x80000001])])
        encoded = '{"short": 1, "long": [{"Z": "2147483648"}, {"Z": "2147483649"}]}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_dict_in_list(self):
        value = [1, OrderedDict([("short", 1), ("long", 0x80000000)]), 3]
        encoded = '[1, {"short": 1, "long": {"Z": "2147483648"}}, 3]'
        self.assertEqual(encoded, jolt_dumps(value))


class JoltCoreTypeDecodingTestCase(TestCase):

    def assert_is_integer(self, encoded, actual):
        self.assertIsInstance(actual, int)
        self.assertEqual(encoded, actual)

    def assert_is_float(self, encoded, actual):
        self.assertIsInstance(actual, float)
        self.assertEqual(encoded, actual)

    def assert_is_string(self, encoded, actual):
        self.assertIsInstance(actual, str)
        self.assertEqual(encoded, actual)

    def assert_is_byte_array(self, encoded, actual):
        self.assertIsInstance(actual, bytearray)
        self.assertEqual(encoded, actual)

    def assert_is_list(self, encoded, actual):
        self.assertIsInstance(actual, list)
        self.assertEqual(encoded, actual)

    def assert_is_dict(self, encoded, actual):
        self.assertIsInstance(actual, dict)
        self.assertEqual(encoded, actual)

    def test_none(self):
        self.assertIsNone(jolt_loads("null"))

    def test_true(self):
        self.assertIs(True, jolt_loads("true"))

    def test_false(self):
        self.assertIs(False, jolt_loads("false"))

    def test_small_integers(self):
        for value in range(-32768, 32768):
            encoded = str(value)
            self.assertEqual(value, jolt_loads(encoded))

    def test_small_integers_with_trailing_dot_zero(self):
        for value in range(-32768, 32768):
            encoded = str(float(value))
            self.assertEqual(value, jolt_loads(encoded))

    def test_integers_around_31_bit_positive_boundary(self):
        self.assert_is_integer(0x7FFFFFFF, jolt_loads('2147483647'))
        self.assert_is_integer(0x7FFFFFFF, jolt_loads('{"Z": "2147483647"}'))
        self.assert_is_integer(0x80000000, jolt_loads('{"Z": "2147483648"}'))
        self.assert_is_integer(0x80000001, jolt_loads('{"Z": "2147483649"}'))

    def test_integers_around_31_bit_negative_boundary(self):
        self.assert_is_integer(-0x7FFFFFFF, jolt_loads('-2147483647'))
        self.assert_is_integer(-0x80000000, jolt_loads('-2147483648'))
        self.assert_is_integer(-0x80000001, jolt_loads('{"Z": "-2147483649"}'))

    def test_small_whole_number_float(self):
        self.assert_is_float(1.0, jolt_loads('{"R": "1.0"}'))

    def test_large_whole_number_float(self):
        self.assert_is_float(2147483648.0, jolt_loads('2147483648.0'))

    def test_fractional_float(self):
        self.assert_is_float(1.5, jolt_loads('1.5'))

    def test_string(self):
        self.assert_is_string("hello, world", jolt_loads('"hello, world"'))

    def test_empty_string(self):
        self.assert_is_string("", jolt_loads('""'))

    def test_byte_array(self):
        self.assert_is_byte_array(bytearray([15, 16, 17]), jolt_loads('{"#": "0F1011"}'))

    def test_empty_byte_array(self):
        self.assert_is_byte_array(bytearray(), jolt_loads('{"#": ""}'))

    def test_empty_list(self):
        self.assert_is_list([], jolt_loads('[]'))

    def test_singleton_list(self):
        self.assert_is_list([1], jolt_loads('[1]'))

    def test_regular_list(self):
        self.assert_is_list([1, 2, 3], jolt_loads('[1, 2, 3]'))

    def test_nested_lists(self):
        self.assert_is_list([1, [2.1, 2.2, 2.3], 3], jolt_loads('[1, [2.1, 2.2, 2.3], 3]'))

    def test_escaped_values_in_list(self):
        value = [0x7FFFFFFF, 0x80000000, 0x80000001]
        encoded = '[2147483647, {"Z": "2147483648"}, {"Z": "2147483649"}]'
        self.assert_is_list(value, jolt_loads(encoded))

    def test_nested_escaped_values_in_list(self):
        value = [1, [0x7FFFFFFF, 0x80000000, 0x80000001], 3]
        encoded = '[1, [2147483647, {"Z": "2147483648"}, {"Z": "2147483649"}], 3]'
        self.assert_is_list(value, jolt_loads(encoded))

    def test_empty_dict(self):
        value = {}
        encoded = '{}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_singleton_dict(self):
        value = {"one": 1}
        encoded = '{"{}": {"one": 1}}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_regular_dict(self):
        value = OrderedDict([("one", 1), ("two", 2)])
        encoded = '{"one": 1, "two": 2}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_nested_dicts(self):
        value = OrderedDict([("one", 1), ("two", OrderedDict([("one", 2.1), ("two", 2.2)]))])
        encoded = '{"one": 1, "two": {"one": 2.1, "two": 2.2}}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_escaped_values_in_dict(self):
        value = OrderedDict([("short", 1), ("long", 0x80000000)])
        encoded = '{"short": 1, "long": {"Z": "2147483648"}}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_list_in_dict(self):
        value = OrderedDict([("short", 1), ("long", [0x80000000, 0x80000001])])
        encoded = '{"short": 1, "long": [{"Z": "2147483648"}, {"Z": "2147483649"}]}'
        self.assert_is_dict(value, jolt_loads(encoded))

    def test_dict_in_list(self):
        value = [1, OrderedDict([("short", 1), ("long", 0x80000000)]), 3]
        encoded = '[1, {"short": 1, "long": {"Z": "2147483648"}}, 3]'
        self.assert_is_list(value, jolt_loads(encoded))


class JoltGraphTypeEncodingTestCase(TestCase):

    def setUp(self):
        self.g = g = Graph()
        self.a = a = g.put_node(1, ["Person"], {"name": "Alice"})
        self.b = b = g.put_node(2, ["Person"], {"name": "Bob", "date_of_birth": Date(1970, 1, 1)})
        self.c = c = g.put_node(3, ["Person"], {"name": "Carol"})
        self.d = d = g.put_node(4, ["Person"], {"name": "Dave"})
        self.ab = ab = g.put_relationship(7, a, b, "KNOWS", {"since": 1999})
        self.cb = cb = g.put_relationship(8, c, b, "KNOWS", {})
        self.cd = cd = g.put_relationship(9, c, d, "KNOWS", {})
        self.path = Path(a, ab, cb, cd)

    def test_node(self):
        encoded = '{"G": {"1": [["Person"], {"name": "Alice"}]}}'
        self.assertEqual(encoded, jolt_dumps(self.a))

    def test_node_with_non_basic_property(self):
        encoded = '{"G": {"2": [["Person"], {"date_of_birth": {"T": "1970-01-01"}, "name": "Bob"}]}}'
        self.assertEqual(encoded, jolt_dumps(self.b, sort_keys=True))

    def test_relationship_with_properties(self):
        encoded = '{"G": {"7": ["KNOWS", {"since": 1999}, "1", "2"]}}'
        self.assertEqual(encoded, jolt_dumps(self.ab))

    def test_relationship_without_properties(self):
        encoded = '{"G": {"8": ["KNOWS", {}, "3", "2"]}}'
        self.assertEqual(encoded, jolt_dumps(self.cb))

    def test_path(self):
        encoded = ('{"G": [{'
                   '"1": [["Person"], {"name": "Alice"}], '
                   '"2": [["Person"], {"name": "Bob", "date_of_birth": {"T": "1970-01-01"}}], ' 
                   '"3": [["Person"], {"name": "Carol"}], '
                   '"4": [["Person"], {"name": "Dave"}]'
                   '}, {'
                   '"7": ["KNOWS", {"since": 1999}, "1", "2"], ' 
                   '"8": ["KNOWS", {}, "3", "2"], ' 
                   '"9": ["KNOWS", {}, "3", "4"]'
                   '}, '
                   '["1", "7", "8", "9"]'
                   ']}')
        self.assertEqual(encoded, jolt_dumps(self.path))


class JoltSpatialTypeEncodingTestCase(TestCase):

    def test_wgs84_point(self):
        value = WGS84Point([12.34, 56.78])
        encoded = '{"@4326": {"POINT": [12.34, 56.78]}}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_cartesian_point(self):
        value = CartesianPoint([12.34, 56.78])
        encoded = '{"@7203": {"POINT": [12.34, 56.78]}}'
        self.assertEqual(encoded, jolt_dumps(value))


class JoltSpatialTypeDecodingTestCase(TestCase):

    def test_wgs84_point(self):
        value = WGS84Point([12.34, 56.78])
        encoded = '{"@4326": {"POINT": [12.34, 56.78]}}'
        self.assertEqual(value, jolt_loads(encoded))

    def test_cartesian_point(self):
        value = CartesianPoint([12.34, 56.78])
        encoded = '{"@7203": {"POINT": [12.34, 56.78]}}'
        self.assertEqual(value, jolt_loads(encoded))


class JoltTemporalTypeEncodingTestCase(TestCase):

    def test_date(self):
        value = Date(2016, 6, 23)
        encoded = '{"T": "2016-06-23"}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_time(self):
        value = Time(12, 34, 56.789123456)
        encoded = '{"T": "12:34:56.789123456"}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_date_time(self):
        value = DateTime(2016, 6, 23, 12, 34, 56)
        encoded = '{"T": "2016-06-23T12:34:56.000000000"}'
        self.assertEqual(encoded, jolt_dumps(value))

    def test_date_time_with_tz(self):
        eastern = timezone("US/Eastern")
        value = eastern.localize(DateTime(2016, 6, 23, 12, 34, 56))
        encoded = '{"T": "2016-06-23T12:34:56.000000000-04:00"}'
        actual = jolt_dumps(value)
        self.assertEqual(encoded, actual)


class JoltTemporalTypeDecodingTestCase(TestCase):

    def test_date(self):
        value = Date(2016, 6, 23)
        encoded = '{"T": "2016-06-23"}'
        self.assertEqual(value, jolt_loads(encoded))

    def test_time(self):
        value = Time(12, 34, 56.789123456)
        encoded = '{"T": "12:34:56.789123456"}'
        self.assertEqual(value, jolt_loads(encoded))
