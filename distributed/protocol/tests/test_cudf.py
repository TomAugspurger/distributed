import pytest
from distributed.protocol import serialize, deserialize

cudf = pytest.importorskip("cudf")
import distributed.protocol.cudf  # noqa
from cudf.tests.utils import assert_eq


@pytest.mark.parametrize("data", [
    [],
    [1, 2, 3],
    [1, 2, None]
])
def test_serialize_series(data):
    s = cudf.Series(data, name="name")
    header, frames = serialize(s)

    result = deserialize(header, frames)
    assert_eq(result, s)


def test_serialize_frame():
    df = cudf.DataFrame({"A": [1, None, 2], "B": [1, 2, 3]})
    header, frames = serialize(df)

    result = deserialize(header, frames)
    assert_eq(result, df)
