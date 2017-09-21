"""
Serialize pandas DataFrames using Apache Arrow
"""
import pickle

import pandas as pd
import pyarrow as pa

from .serialize import register_serialization


def serialize_pandas_dataframe(df, name=None):
    # integer column names cast to strings
    # ARROW-1585
    header = {'column_dtype': str(df.columns.dtype)}

    if name is not None:
        header['name'] = name
    if df.columns.name is not None:
        header['columns_name'] = df.columns.name
    if len(df) == 0:
        # Exception when serializing length 0 dataframe
        # See ARROW-1584
        header['empty_dataframe'] = True
        buf = pickle.dumps(df.columns)
    else:
        buf = pa.serialize_pandas(df)
    return header, [memoryview(buf)]


def deserialize_pandas_dataframe(header, frames):
    buf = frames[0]

    if header.get('empty_dataframe'):
        # See ARROW-1584
        columns = pickle.loads(buf)
        df = pd.DataFrame(columns=columns)
    else:
        df = pa.deserialize_pandas(buf)
    df.columns.name = header.get("columns_name")
    df.columns = df.columns.astype(header['column_dtype'])
    return df


def serialize_pandas_series(s):
    return serialize_pandas_dataframe(s.to_frame(), name=s.name)


def deserialize_pandas_series(header, frames):
    s = deserialize_pandas_dataframe(header, frames).squeeze()
    s.name = header.get('name')
    return s


register_serialization(pd.DataFrame,
                       serialize_pandas_dataframe,
                       deserialize_pandas_dataframe)
register_serialization(pd.Series,
                       serialize_pandas_series,
                       deserialize_pandas_series)
