import pytest
from distributed.protocol import serialize, deserialize

cupy = pytest.importorskip("cupy")


@pytest.mark.parametrize('data', [
    cupy.random.random(10),
    cupy.random.random((10, 10)),
])
@pytest.mark.xfail("TODO")
def test_cupy(data):
    header, frames = serialize(data)
    result = deserialize(header, frames)
    cupy.testing.assert_array_equal(result, data)
