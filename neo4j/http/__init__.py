from collections import OrderedDict
import requests
# import logging
#
# # Enabling debugging at http.client level (requests->urllib3->http.client)
# # you will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# # the only thing missing will be the response.body which is not logged.
# try: # for Python 3
#     from http.client import HTTPConnection
# except ImportError:
#     from httplib import HTTPConnection
# HTTPConnection.debuglevel = 1
#
# logging.basicConfig() # you need to initialize logging, otherwise you will not see anything from requests
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

from neo4j.compat import urlparse
from neo4j.v1 import ProtocolError, CypherError
from neo4j.v1.api import ValueSystem, GraphDatabase, Driver, Session, StatementResult
from neo4j.v1.types import Record, Node, Relationship, UnboundRelationship, Path


DEFAULT_PORT = 7474


class JSONValueSystem(ValueSystem):

    def hydrate(self, obj):
        if isinstance(obj, dict):
            if "self" in obj:
                metadata = obj.get("metadata", {})
                data = obj.get("data", {})
                id_ = metadata.get("id")
                if "type" in obj:
                    start = int(obj["start"].rpartition("/")[-1])
                    end = int(obj["end"].rpartition("/")[-1])
                    return Relationship.hydrate(id_, start, end, obj["type"], data)
                else:
                    return Node.hydrate(id_, metadata.get("labels", ()), data)
            if "nodes" in obj and "relationships" in obj:
                raise NotImplementedError("Path hydration not implemented")
            return {key: self.hydrate(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return list(map(self.hydrate, obj))
        else:
            return obj


GraphDatabase.value_systems["json"] = JSONValueSystem()


class HTTPDriver(Driver):

    def __init__(self, uri, **config):
        super(HTTPDriver, self).__init__(None)
        parsed = urlparse(uri)
        port = parsed.port or DEFAULT_PORT
        self.root_uri = "%s://%s:%s/" % (parsed.scheme, parsed.hostname, port)
        self.auth = config.get("auth")
        self.root_links = requests.get(self.root_uri, auth=self.auth).json()
        self.data_uri = self.root_links.get("data")
        self.data_links = requests.get(self.data_uri, auth=self.auth).json()

    def session(self, access_mode=None):
        tx_uri = self.data_links.get("transaction")
        return HTTPSession(tx_uri, auth=self.auth)


GraphDatabase.uri_schemes["http"] = HTTPDriver


class HTTPResultLoader(object):

    def load(self, result):
        pass

    def fail(self):
        pass


class HTTPSession(Session):

    #: e.g. http://localhost:7474/db/data/transaction
    begin_uri = None

    #: e.g. http://localhost:7474/db/data/transaction/commit
    autocommit_uri = None

    #: e.g. http://localhost:7474/db/data/transaction/1
    transaction_uri = None

    #: e.g. http://localhost:7474/db/data/transaction/1/commit
    commit_uri = None

    def __init__(self, uri, **config):
        self.begin_uri = uri
        self.autocommit_uri = "%s/commit" % self.begin_uri
        self.sync_uri = self.autocommit_uri
        self._statements = []
        self._result_loaders = []
        self._session = requests.Session()
        self._session.auth = config.get("auth")

    def close(self):
        super(HTTPSession, self).close()
        self._session.close()

    def run(self, statement, parameters=None, **kwparameters):
        self._statements.append(OrderedDict([
            ("statement", statement),
            ("parameters", dict(parameters or {}, **kwparameters)),
            ("resultDataContents", ["REST"]),
        ]))
        result_loader = HTTPResultLoader()
        self._result_loaders.append(result_loader)
        return HTTPStatementResult(self, result_loader)

    def fetch(self):
        return self.sync()

    def sync(self):
        count = 0
        try:
            response = self._session.post(self.sync_uri, json={
                "statements": self._statements,
            })
            status_code = response.status_code
            if status_code // 100 == 2:
                if status_code == 201:
                    self.transaction_uri = response.headers["Location"]
                    self.commit_uri = "%s/commit" % self.transaction_uri
                    self.sync_uri = self.transaction_uri
                content = response.json()
                errors = content["errors"]
                for i, result_loader in enumerate(self._result_loaders):
                    try:
                        count += result_loader.load(content["results"][i])
                    except IndexError:
                        result_loader.fail()
                return count
            else:
                raise ProtocolError("HTTP request failed: %r" % response)
        finally:
            self._statements[:] = ()
            self._result_loaders[:] = ()

    def begin_transaction(self, bookmark=None):
        transaction = super(HTTPSession, self).begin_transaction(bookmark)
        self.sync_uri = self.begin_uri
        return transaction

    def commit_transaction(self):
        super(HTTPSession, self).commit_transaction()
        self.sync_uri = self.commit_uri or self.autocommit_uri
        try:
            self.sync()
        finally:
            self.commit_uri = self.transaction_uri = None
            self.sync_uri = self.autocommit_uri

    def rollback_transaction(self):
        super(HTTPSession, self).rollback_transaction()
        try:
            if self.transaction_uri:
                self._session.delete(self.transaction_uri)
        finally:
            self.commit_uri = self.transaction_uri = None
            self.sync_uri = self.autocommit_uri


class HTTPStatementResult(StatementResult):

    value_system = GraphDatabase.value_systems["json"]

    zipper = Record

    def __init__(self, session, result_loader):
        super(HTTPStatementResult, self).__init__(session)

        def load(result):
            self._keys = tuple(result["columns"])
            self._records.extend(record["rest"] for record in result["data"])
            self._session = None
            return len(self._records)

        def fail():
            self._session = None

        result_loader.load = load
        result_loader.fail = fail
