"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communcations` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import asyncio
import logging
import sys
import struct
import threading

from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener
from .registry import Backend, backends
from .utils import ensure_concrete_host, to_frames, from_frames
from ..compatibility import PY3
from ..utils import ensure_ip, get_ip, get_ipv6, nbytes

import ucp_py as ucp

logger = logging.getLogger(__name__)
MAX_MSG_LOG = 23
PORT = 13337
IP = '10.33.225.160'
ADDRESS = f'ucx://{IP}:{PORT}'

ucp.init()


# ----------------------------------------------------------------------------
# Addressing
# TODO: Parts of these should probably be moved to `comm/addressing.py`
# ----------------------------------------------------------------------------


def _parse_address(addr: str, strict=False) -> tuple:
    """
    >>> _parse_address("ucx://10.33.225.160")
    """
    if not addr.startswith("ucx://"):
        raise ValueError("Invalid url scheme {}".format(addr))

    proto, address = addr.split("://", 1)
    return proto, address


def _parse_host_port(address: str, default_port=None) -> tuple:
    """
    Parse an endpoint address given in the form "host:port".

    >>> _parse_host_port("10.33.225.160:123337")
    ("10.33.225.160", 13337)
    """
    if address.startswith("ucx://"):
        _, address = _parse_address(address)

    return parse_host_port(address, default_port=PORT)


def _unparse_host_port(host, port=None):
    return unparse_host_port(host, port)


# ----------------------------------------------------------------------------
# Comm Interface
# ----------------------------------------------------------------------------


class UCX(Comm):
    """Comm object using UCP.

    Parameters
    ----------
    address : str
        The address, prefixed with `ucx://` to use.
    ...
    """
    def __init__(
        self,
        ep,
        address: str,
        # reader,  # stream? reader? writer?
        # writer,
        deserialize=True,
    ):
        logger.debug("UCX.__init__")
        self.ep = ep
        self._host, self._port = _parse_host_port(address)
        self._local_addr = None
        self._peer_addr = None
        self.deserialize = deserialize

        # finalizer

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    async def write(self, msg, serializers=None, on_error="message"):
        nbytes = sys.getsizeof(msg)
        await self.ep.send_msg(msg, nbytes)
        return nbytes

    async def read(self, deserializers=None):
        resp = await self.ep.recv_future()
        obj = ucp.get_obj_from_msg(resp)
        return obj

    def abort(self):
        if self.ep:
            ucp.destroy_ep(self.ep)
            self.ep = None

    async def close(self):
        # TODO: Handle in-flight messages?
        self.abort()

    def closed(self):
        return self.ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    client = ...  # TODO: add a client here?

    async def connect(self, address, deserialize=True, **connection_args):
        logger.debug("UCXConnector.connect")
        ip, port = _parse_host_port(address)
        ep = ucp.get_endpoint(ip.encode(), port)
        return self.comm_class(ep, "", "")


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(self, address, comm_handler=None, deserialize=False,
                 ucp_handler=None, **connection_args):
        logger.debug("UCXListener.__init__")
        self.address = address
        self.ip, self.port = _parse_host_port(address)
        self.comm_handler = comm_handler
        self.ucp_handler = ucp_handler
        self.deserialize = deserialize
        # deserialize?
        self.ep = None  # type: TODO

        # XXX: The init may be required to take args like
        # {'require_encryption': None, 'ssl_context': None}
        self.connection_args = connection_args

    def start(self):

        async def serve_forever(client_ep):
            # Still not sure about this... but maybe enough for now
            ucx = UCX(client_ep, self.address)
            if self.comm_handler:
                await self.comm_handler(ucx)

        server = ucp.start_server(serve_forever,
                                  server_port=self.port,
                                  is_coroutine=True)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        loop.create_task(server)

    def stop(self):
        # What all should this do?
        if self.ep:
            ucp.destroy_ep(self.ep)
        ucp.stop_server()  # ?

    def get_host_port(self):
        return self.ip, self.port
        # TODO

    @property
    def listen_address(self):
        return self.prefix + _unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)  # TODO: ensure_concrete_host
        return self.prefix + unparse_host_port(host, port)


class UCXBackend(Backend):
    # I / O

    def get_connector(self):
        return UCXConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return UCXListener(loc, handle_comm, deserialize, **connection_args)

    # Address handling
    # This is duplicative of BaseTCPBackend

    def get_address_host(self, loc):
        return _parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return _parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return _unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends["ucx"] = UCXBackend()
