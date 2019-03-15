"""
ref:`UCX`_ based communications for distributed.

See :ref:`communications` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import asyncio
import logging
import struct

from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener
from .registry import Backend, backends
from .utils import ensure_concrete_host, to_frames, from_frames
from ..utils import ensure_ip, get_ip, get_ipv6, nbytes

import ucp_py as ucp

logger = logging.getLogger(__name__)
_INITIALIZED = False


def _ucp_init():
    global _INITIALIZED

    if not _INITIALIZED:
        logger.debug("Initializing UCX")
        ucp.init()
        _INITIALIZED = True


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


def _parse_host_port(address: str, default_port=0) -> tuple:
    """
    Parse an endpoint address given in the form "host:port".

    >>> _parse_host_port("10.33.225.160:13337")
    ("10.33.225.160", 13337)
    """
    if address.startswith("ucx://"):
        _, address = _parse_address(address)

    return parse_host_port(address, default_port=default_port)


# ----------------------------------------------------------------------------
# Comm Interface
# ----------------------------------------------------------------------------


class UCX(Comm):
    """Comm object using UCP.

    Parameters
    ----------
    ep : ucp.ucp_py_ep
        The UCP endpoint.
    address : str
        The address, prefixed with `ucx://` to use.
    listener_instance : Optional[ucp.ListenerFuture]
        Only provided when created via UCXListener.
    deserialize : bool, default True
        Whether to deserialize data in :meth:`distributed.protocol.loads`

    Notes
    -----
    The read-write cycle uses the following pattern:

    Each msg is serialized into a number of "data" frames. We prepend these
    real frames with two additional frames

        1. is_gpu: Boolean indicator for whether the frame should be
           received into GPU memory. Packed in '?' format. Unpack with
           ``<n_frames>?`` format.
        2. frame_size : Unsigned int describing the size of frame (in bytes)
           to receive. Packed in 'Q' format, so a length-0 frame is equivalent
           to an unsized frame. Unpacked with ``<n_frames>Q``.

    The expected read cycle is

    1. Read the frame describing number of frames
    2. Read the frame describing whether each data frame is gpu-bound
    3. Read the frame describing whether each data frame is sized
    4. Read all the data frames.
    """
    prefix = 'ucx://'

    def __init__(self,
                 ep: ucp.ucp_py_ep,
                 address: str,
                 listener_instance,
                 deserialize=True):
        logger.debug("Creating <UCX %s %s>", address, listener_instance)
        self.ep = ep
        assert address.startswith("ucx")
        self.address = address
        self.listener_instance = listener_instance
        self._host, self._port = _parse_host_port(address)
        self._local_addr = None
        self.deserialize = deserialize
        # TODO: finalizer?

    @property
    def port(self):
        if self._port == 0:
            return self.listener_instance.port
        return self._port

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return '{}{}:{}'.format(self.prefix, self._host, self.port)

    async def write(self, msg: dict, serializers=None, on_error: str = "message"):
        frames = await to_frames(
            msg, serializers=serializers, on_error=on_error
        )  # TODO: context=
        gpu_frames = b''.join([
            struct.pack("?", hasattr(frame, '__cuda_array_interface__'))
            for frame in frames
        ])
        size_frames = b''.join([struct.pack("Q", nbytes(frame)) for frame in frames])

        frames = [gpu_frames] + [size_frames] + frames
        nframes = struct.pack("Q", len(frames))

        await self.ep.send_obj(nframes)

        for frame in frames:
            await self.ep.send_obj(frame)
        return sum(map(nbytes, frames))

    async def read(self, deserializers=None):
        resp = await self.ep.recv_future()
        obj = ucp.get_obj_from_msg(resp)
        n_frames, = struct.unpack("Q", obj)
        n_data_frames = n_frames - 2

        gpu_frame_msg = await self.ep.recv_future()
        gpu_frame_msg = gpu_frame_msg.get_obj()
        is_gpus = struct.unpack("{}?".format(n_data_frames), gpu_frame_msg)

        sized_frame_msg = await self.ep.recv_future()
        sized_frame_msg = sized_frame_msg.get_obj()
        sizes = struct.unpack("{}Q".format(n_data_frames), sized_frame_msg)

        frames = []

        for i, (is_gpu, size) in enumerate(zip(is_gpus, sizes)):
            if size > 0:
                resp = await self.ep.recv_obj(size, cuda=is_gpu)
            else:
                resp = await self.ep.recv_future()
            frame = ucp.get_obj_from_msg(resp)
            frames.append(frame)

        msg = await from_frames(
            frames, deserialize=self.deserialize, deserializers=deserializers
        )
        return msg

    def abort(self):
        if self.ep:
            ucp.destroy_ep(self.ep)
            self.ep = None
        # if self.listener_instance:
        #     ucp.stop_listener(self.listener_instance)

    async def close(self):
        # TODO: Handle in-flight messages?
        self.abort()

    def closed(self):
        return self.ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    encrypted = False

    async def connect(self, address: str, deserialize=True, **connection_args) -> UCX:
        logger.debug("UCXConnector.connect")
        _ucp_init()
        ip, port = _parse_host_port(address)
        ep = ucp.get_endpoint(ip.encode(), port)
        return UCX(ep, self.prefix + address,
                   listener_instance=None,
                   deserialize=deserialize)


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    encrypted = UCXConnector.encrypted

    def __init__(
        self,
        address: str,
        comm_handler: None,
        deserialize=False,
        **connection_args,
    ):
        logger.debug("UCXListener.__init__")
        if not address.startswith("ucx"):
            address = "ucx://" + address
        self.address = address
        self.ip, self.port = _parse_host_port(address)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.ep = None  # type: ucp.ucp_py_ep
        self.listener_instance = None  # type: ucp.ListenerFuture
        self._task = None

        # XXX: The init may be required to take args like
        # {'require_encryption': None, 'ssl_context': None}
        self.connection_args = connection_args
        self._task = None

    def start(self):
        async def serve_forever(client_ep, listener_instance):
            ucx = UCX(client_ep, self.address, listener_instance,
                      deserialize=self.deserialize)
            self.listener_instance = listener_instance
            if self.comm_handler:
                await self.comm_handler(ucx)

        _ucp_init()
        # We use port=0 to be consistent with distributed. But UCX
        # uses port=-1 as the "find me a port" port.
        port = self.port or -1
        server = ucp.start_listener(
            serve_forever, listener_port=port, is_coroutine=True
        )
        self.port = server.port

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

        t = loop.create_task(server.coroutine)
        self._task = t

    def stop(self):
        # What all should this do?
        if self._task:
            self._task.cancel()

        if self.ep:
            ucp.destroy_ep(self.ep)

        # TODO: Clean this up properly. Currnently getting
        #    asyncio.base_futures.InvalidStateError
        # if self.listener_instance:
        #     ucp.stop_listener(self.listener_instance)

    def get_host_port(self):
        # TODO: TCP raises if this hasn't started yet.
        return self.ip, self.port

    @property
    def listen_address(self):
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)  # TODO: ensure_concrete_host
        return self.prefix + unparse_host_port(host, port)

    @property
    def bound_address(self):
        # TODO: Does this become part of the base API? Kinda hazy, since
        # we exclude in for inproc.
        return self.get_host_port()


class UCXBackend(Backend):
    # I / O

    def get_connector(self):
        return UCXConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return UCXListener(loc, handle_comm, deserialize, **connection_args)

    # Address handling
    # This duplicates BaseTCPBackend

    def get_address_host(self, loc):
        return _parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return _parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends["ucx"] = UCXBackend()
