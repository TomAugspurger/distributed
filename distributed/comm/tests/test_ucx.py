import asyncio

import pytest

from distributed.comm import ucx, listen, connect
from distributed.comm.registry import backends, get_backend
from distributed.comm import ucx


def test_parse_address():
    result = ucx._parse_address("ucx://10.33.225.160")
    assert result == ('ucx', '10.33.225.160')


def test_parse_host_port():
    assert ucx._parse_host_port("10.33.225.160") == ("10.33.225.160", 13337)
    assert ucx._parse_host_port("10.33.225.160:13337") == ("10.33.225.160", 13337)
    assert ucx._parse_host_port("10.33.225.160:13338") == ("10.33.225.160", 13338)


def test_registered():
    assert 'ucx' in backends
    backend = get_backend('ucx')
    assert isinstance(backend, ucx.UCXBackend)


async def get_comm_pair(listen_addr, listen_args=None, connection_args=None):
    q = asyncio.queues.Queue()

    def handle_comm(comm):
        q.put_nowait(comm)

    print("test.listen")
    listener = listen(listen_addr, handle_comm)
    print("test.listener.start")
    listener.start()

    print("test.connect")
    comm = connect(listener.contact_address, connection_args=connection_args)
    print("test.q.get")
    # serv_comm = await q.get()
    serv_comm = None
    return comm, listener


def test_ucx_specific():
    """
    Test concrete UCX API.
    """
    # TODO:
    # 1. ensure exceptions in handle_comm fail the test
    # 2. Use dict in read / write, put seralization there.
    # 3. Test peer_address
    # 4. Test cleanup
    async def f():

        async def handle_comm(comm):
            # XXX: failures here don't fail the build yet
            import pdb; pdb.set_trace()
            msg = yield comm.read()
            msg['op'] = 'pong'
            await comm.write(msg)
            await comm.close()

        listener = ucx.UCXListener(ucx.ADDRESS, handle_comm)
        listener.start()
        host, port = listener.get_host_port()
        assert host == '10.33.225.160'
        assert port > 0

        connector = ucx.UCXConnector()
        l = []

        async def client_communicate(key, delay=0):
            addr = '%s:%d' % (host, port)
            comm = await connector.connect(addr)
            # TODO: peer_address
            # assert comm.peer_address == 'ucx://' + addr
            assert comm.extra_info == {}
            msg = {'op': 'ping', 'data': key}
            await comm.write(msg)
            if delay:
                await asyncio.sleep(delay)
            msg = await comm.read()
            assert msg == {'op': 'pong', 'data': key}
            l.append(key)
            await comm.close()

        await client_communicate(key=1234)

        # Many clients at once
        N = 100
        futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
        await asyncio.gather(*futures)
        assert set(l) == {1234} | set(range(N))

    asyncio.run(f())
