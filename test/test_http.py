#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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

from neo4j.v1 import GraphDatabase, Node, TransactionError, StatementResult
from neo4j.http import HTTPDriver, HTTPSession

from test.util import ServerTestCase

HTTP_URI = "http://localhost:7474"
AUTH_TOKEN = ("neotest", "neotest")


class AutoCommitTransactionTestCase(ServerTestCase):

    def setUp(self):
        self.driver = GraphDatabase.driver(HTTP_URI, auth=AUTH_TOKEN)

    def test_can_run_simple_statement(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS n")
            for record in result:
                assert record[0] == 1
                assert record["n"] == 1
                with self.assertRaises(KeyError):
                    _ = record["x"]
                assert record["n"] == 1
                with self.assertRaises(KeyError):
                    _ = record["x"]
                with self.assertRaises(TypeError):
                    _ = record[object()]
                assert repr(record)
                assert len(record) == 1

    def test_should_run_transaction(self):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run("RETURN 1")
            records = list(result)
            assert len(records) == 1
            record = records[0]
            assert len(record) == 1
            value = record[0]
            assert value == 1

    def test_should_hydrate_node(self):
        from neo4j.v1.types import Node
        with self.driver.session() as session:
            result = session.run("CREATE (a:Person {name:'Alice'}) RETURN a")
            record = result.single()
            assert len(record) == 1
            value = record[0]
            assert isinstance(value, Node)
            assert isinstance(value.id, int)
            assert value.labels == {"Person"}
            assert value.properties == {"name": "Alice"}

    def test_should_hydrate_relationship(self):
        from neo4j.v1.types import Relationship
        with self.driver.session() as session:
            result = session.run("CREATE (a)-[ab:KNOWS {since:1999}]->(b) RETURN ab")
            record = result.single()
            assert len(record) == 1
            value = record[0]
            assert isinstance(value, Relationship)
            assert isinstance(value.id, int)
            assert isinstance(value.start, int)
            assert isinstance(value.end, int)
            assert value.type == "KNOWS"
            assert value.properties == {"since": 1999}


class TransactionRunTestCase(ServerTestCase):

    def setUp(self):
        self.driver = GraphDatabase.driver(HTTP_URI, auth=AUTH_TOKEN)

    def test_can_run_single_statement_transaction(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            assert not tx.closed()
            cursor = tx.run("CREATE (a) RETURN a")
            tx.commit()
            records = list(cursor)
            assert len(records) == 1
            for record in records:
                assert isinstance(record[0], Node)
            assert tx.closed()

    def test_can_run_query_that_returns_map_literal(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            cursor = tx.run("RETURN {foo:'bar'}")
            tx.commit()
            value = cursor.single()[0]
            assert value == {"foo": "bar"}

    def test_can_run_transaction_as_with_statement(self):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                assert not tx.closed()
                tx.run("CREATE (a) RETURN a")
            assert tx.closed()

    def test_can_run_multi_statement_transaction(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            assert not tx.closed()
            cursor_1 = tx.run("CREATE (a) RETURN a")
            cursor_2 = tx.run("CREATE (a) RETURN a")
            cursor_3 = tx.run("CREATE (a) RETURN a")
            tx.commit()
            for cursor in (cursor_1, cursor_2, cursor_3):
                records = list(cursor)
                assert len(records) == 1
                for record in records:
                    assert isinstance(record[0], Node)
            assert tx.closed()

    def test_can_run_multi_sync_transaction(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            for i in range(10):
                assert not tx.closed()
                cursor_1 = tx.run("CREATE (a) RETURN a")
                cursor_2 = tx.run("CREATE (a) RETURN a")
                cursor_3 = tx.run("CREATE (a) RETURN a")
                tx.sync()
                for cursor in (cursor_1, cursor_2, cursor_3):
                    records = list(cursor)
                    assert len(records) == 1
                    for record in records:
                        assert isinstance(record[0], Node)
            tx.commit()
            assert tx.closed()

    def test_can_rollback_transaction(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            for i in range(10):
                assert not tx.closed()
                cursor_1 = tx.run("CREATE (a) RETURN a")
                cursor_2 = tx.run("CREATE (a) RETURN a")
                cursor_3 = tx.run("CREATE (a) RETURN a")
                tx.sync()
                for cursor in (cursor_1, cursor_2, cursor_3):
                    records = list(cursor)
                    assert len(records) == 1
                    for record in records:
                        assert isinstance(record[0], Node)
            tx.rollback()
            assert tx.closed()

    def test_cannot_append_after_transaction_finished(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            tx.rollback()
            with self.assertRaises(TransactionError):
                tx.run("CREATE (a) RETURN a")


class HTTPSessionConstructionTestCase(TestCase):

    def setUp(self):
        self.uri = "http://localhost:7474/db/data/transaction"

    def test_should_derive_begin_uri(self):
        # When
        session = HTTPSession(self.uri)
        # Then
        assert session.begin_uri == self.uri

    def test_should_derive_autocommit_uri(self):
        # When
        session = HTTPSession(self.uri)
        # Then
        assert session.autocommit_uri == self.uri + "/commit"

    def test_should_have_no_transaction_uri_by_default(self):
        # When
        session = HTTPSession(self.uri)
        # Then
        assert session.transaction_uri is None

    def test_should_have_no_commit_uri_by_default(self):
        # When
        session = HTTPSession(self.uri)
        # Then
        assert session.commit_uri is None

    def test_should_sync_with_autocommit_uri_by_default(self):
        # When
        session = HTTPSession(self.uri)
        # Then
        assert session.sync_uri == session.autocommit_uri


class HTTPSessionRunTestCase(TestCase):

    def setUp(self):
        self.session = HTTPSession("http://localhost:7474/db/data/transaction")

    def tearDown(self):
        self.session.close()

    def test_should_begin_with_no_statements(self):
        statements = self.session._statements
        assert len(statements) == 0

    def test_should_enqueue_statement_without_parameters(self):
        # When
        self.session.run("RETURN 1")
        # Then
        statements = self.session._statements
        assert len(statements) == 1
        assert statements[0]["statement"] == "RETURN 1"
        assert statements[0]["parameters"] == {}

    def test_should_enqueue_statement_with_parameters(self):
        # When
        self.session.run("RETURN $x", {"x": 1})
        # Then
        statements = self.session._statements
        assert len(statements) == 1
        assert statements[0]["statement"] == "RETURN $x"
        assert statements[0]["parameters"] == {"x": 1}

    def test_should_enqueue_statement_with_keyword_parameters(self):
        # When
        self.session.run("RETURN $x", x=1)
        # Then
        statements = self.session._statements
        assert len(statements) == 1
        assert statements[0]["statement"] == "RETURN $x"
        assert statements[0]["parameters"] == {"x": 1}

    def test_should_enqueue_statement_with_both_parameter_types(self):
        # When
        self.session.run("RETURN $x, $y", {"x": 1}, y=2)
        # Then
        statements = self.session._statements
        assert len(statements) == 1
        assert statements[0]["statement"] == "RETURN $x, $y"
        assert statements[0]["parameters"] == {"x": 1, "y": 2}

    def test_keyword_parameters_should_override_dict_parameters(self):
        # When
        self.session.run("RETURN $x", {"x": 1}, x=2)
        # Then
        statements = self.session._statements
        assert len(statements) == 1
        assert statements[0]["statement"] == "RETURN $x"
        assert statements[0]["parameters"] == {"x": 2}

    def test_should_return_result(self):
        # When
        result = self.session.run("RETURN $x", x=1)
        # Then
        assert isinstance(result, StatementResult)

    def test_result_should_be_online(self):
        # When
        result = self.session.run("RETURN $x", x=1)
        # Then
        assert result.online()

    def test_result_should_be_empty(self):
        # When
        result = self.session.run("RETURN $x", x=1)
        # Then
        records = result._records
        assert len(records) == 0


class HTTPSessionSyncTestCase(ServerTestCase):

    def setUp(self):
        self.driver = GraphDatabase.driver(HTTP_URI, auth=AUTH_TOKEN)
        self.session = self.driver.session()

    def tearDown(self):
        self.session.close()
        self.driver.close()

    def test_should_sync_against_sync_uri(self):
        pass  # TODO

    def test_should_retain_autocommit_uri_after_sync_if_no_transaction(self):
        # Given
        self.session.run("RETURN $x", x=1)
        assert self.session.sync_uri == self.session.autocommit_uri
        # When
        self.session.sync()
        # Then
        assert self.session.sync_uri == self.session.autocommit_uri

    def test_should_switch_to_begin_uri_after_begin_transaction(self):
        # Given
        assert self.session.sync_uri == self.session.autocommit_uri
        # When
        self.session.begin_transaction()
        # Then
        assert self.session.sync_uri == self.session.begin_uri

    def test_should_switch_to_transaction_uri_after_sync_if_in_transaction(self):
        # Given
        self.session.begin_transaction()
        assert self.session.sync_uri == self.session.begin_uri
        # When
        self.session.run("RETURN $x", x=1)
        self.session.sync()
        # Then
        assert self.session.sync_uri == self.session.transaction_uri
