"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communcations` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import asyncio
import logging
import sys
import struct

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
        self.ep = ep
        return ep

    async def write(self, msg, serializers=None, on_error="message"):
        logger.debug("UCX.write")
        stream = self._get_endpoint()
        frames = await to_frames(
            msg,
            serializers=serializers,
            on_error=on_error,
            context={"sender": self._local_addr, "recipient": self._peer_addr},
        )
        try:
            # TODO: this is just copy-pasted from TCP. Figure out what
            # makes sense for UCX
            lengths = [nbytes(frame) for frame in frames]
            length_bytes = [struct.pack("Q", len(frames))] + [
                struct.pack("Q", x) for x in lengths
            ]
            if PY3 and sum(lengths) < 2 ** 17:  # 128kiB
                b = b"".join(length_bytes + frames)  # small enough, send in one go
                # TODO: check units on the size here...
                # stream.send_msg(b, sum(lengths))
                stream.send_msg(b, sys.getsizeof(b))
            else:
                raise
                # stream.write(b''.join(length_bytes))  # avoid large memcpy, send in many

                # for frame in frames:
                #     # Can't wait for the write() Future as it may be lost
                #     # ("If write is called again before that Future has resolved,
                #     #   the previous future will be orphaned and will never resolve")
                #     if not self._iostream_allows_memoryview:
                #         frame = ensure_bytes(frame)
                #     future = stream.write(frame)
                #     bytes_since_last_yield += nbytes(frame)
                #     if bytes_since_last_yield > 32e6:
                #         yield future
                #         bytes_since_last_yield = 0
        except Exception as e:
            # TODO: tighter exception
            stream = None
            # convert_stream_closed_error(self, e)
            raise
        except TypeError as e:
            if stream._write_buffer is None:
                logger.info("tried to write message %s on closed stream", msg)
            else:
                raise

        return sum(map(nbytes, frames))

    async def read(self, deserializers=None):
        logger.debug("UXC.read")
        ep = self._get_endpoint()
        print('got ep')
        resp = await ep.recv_future()
        print('got fut')
        frames = ucp.get_obj_from_msg(resp)
        print('got frames')
        try:
            msg = await from_frames(frames, deserialize=self.deserialize,
                                    deserializers=deserializers)
        except Exception:
            logger.exception('whoops')
            raise
        return msg

    def abort(self):
        pass

    def close(self):
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


# TODO: Remove / cleanup connect & serve.
# This is just trying to figure out how to get them started up.
msg = b'hi'


async def connect():
    print("Connecting")
    ep = ucp.get_endpoint(b"10.33.225.160", 13337)
    ep.send_msg(msg, sys.getsizeof(msg))

    resp = await ep.recv_future()
    r_msg = ucp.get_obj_from_msg(resp)
    print("Response: ", r_msg.decode())
    print("Stopping client")
    ucp.destroy_ep(ep)
    print("Client done")


async def serve(ep):
    print('Handling')
    ack = await ep.recv_future()
    rmsg = ucp.get_obj_from_msg(ack)
    ep.send_msg(rmsg, sys.getsizeof(rmsg))

    print('Stopping server')
    ucp.destroy_ep(ep)
    ucp.stop_server()
    print("Server done")


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(self, address, comm_handler=None, deserialize=False,
                 ucp_handler=serve):
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

    async def _start(self):
        client = connect()
        server = ucp.start_server(self.ucp_handler,
                                  server_port=self.port,
                                  is_coroutine=True)
        print('schuduling startup')
        await asyncio.gather(client, server)

    def start(self):
        logger.debug("UCXListener.start")
        ucp.init()  # TODO: thread local for this?
        loop = asyncio.get_event_loop()
        # I don't understand this part...
        # We want the server to continue to run in the background,
        # ready to accept new connections, right? So we need to
        # *start* the server, but "run_until_complete" doesn't really
        # make sense, because this function needs to return as
        # soon as the server fininhes *starting*, not *serving*.
        # Does that require another thread for the server? Or am
        # I missing something?
        loop.run_until_complete(self._start())
        # if self.comm_handler:
            # fut = self.comm_handler(self)
            # loop.call_soon(fut)

    def stop(self):
        if self.ep:
            ucp.destroy_ep(self.ep)
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
