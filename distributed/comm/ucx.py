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
        address: str,
        # reader,  # stream? reader? writer?
        # writer,
        local_addr: str,
        peer_addr: str,
        deserialize=True,
    ):
        logger.debug("UCX.__init__")
        self._host, self._port = _parse_host_port(address)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        # self.reader = reader
        # self.writer = writer
        self.deserialize = deserialize

        # finalizer

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    def _get_endpoint(self):
        logger.debug(f"connecting to {self._host}:{self._port}")
        ep = ucp.get_endpoint(self._host.encode(), self._port)
        return ep

    async def write(self, msg, serializers=None, on_error="message"):
        nbytes = sys.getsizeof(msg)
        ep = self._get_endpoint()
        await ep.send_msg(msg, nbytes)
        return nbytes

    async def read(self, deserializers=None):
        ep = self._get_endpoint()
        resp = await ep.recv_future()
        obj = ucp.get_obj_from_msg(resp)
        return obj

    def abort(self):
        pass

    async def close(self):
        # TODO
        pass

    def closed(self):
        pass


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    client = ...  # TODO: add a client here?

    async def connect(self, address, deserialize=True, **connection_args):
        logger.debug("UCXConnector.connect")
        # hmm so distributed.comm.core.connect says "This starts a thread",
        # "this" presumably torando. Maybe this should be done on a different
        # thread?
        ip, port = _parse_host_port(address)
        try:
            # TODO: MAX_BUFFER_SIZE
            # ep = ucp.get_endpoint(ip.encode(), port)
            pass
        except Exception as e:
            # TODO: cleanup
            raise e
        # local_address = self.prefix + get_stream_address(stream)
        return self.comm_class(address, "", "")


async def serve_forever(client_ep):
    while True:
        msg = await client_ep.recv_future()
        msg = ucp.get_obj_from_msg(msg)
        if msg == b'':
            break
        else:
            client_ep.send_msg(msg, sys.getsizeof(msg))

    # TODO: unclear if this should happen here. Probably
    # in stop
    ucp.destroy_ep(client_ep)
    ucp.stop_server()


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(self, address, comm_handler=None, deserialize=False,
                 ucp_handler=serve_forever):
        # XXX: comm_handler is wrong
        # Right now we pass it to ucp.start_server
        # The expectation seems to be a callback
        logger.debug("UCXListener.__init__")
        self.ip, self.port = _parse_host_port(address)
        self.comm_handler = comm_handler
        self.ucp_handler = ucp_handler
        self.deserialize = deserialize
        # deserialize?
        self.ep = None  # type: TODO

    def start(self):
        server = ucp.start_server(self.ucp_handler,
                                  server_port=self.port,
                                  is_coroutine=True)
        loop = asyncio.get_running_loop()
        loop.create_task(server)

    def stop(self):
        if self.ep:
            ucp.destroy_ep(self.ep)
        # If we take the serve_forever appoach, this would send the stop message.
        # TODO: ?
        # ucp.stop_server()

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
