import pickle

import cudf
from .serialize import dask_serialize, dask_deserialize
from .numba import serialize_numba_ndarray, deserialize_numba_ndarray

# maybe remove old things
classes = [
    cudf.dataframe.buffer.Buffer,
    cudf.dataframe.datetime.DatetimeColumn,
    cudf.dataframe.numerical.NumericalColumn,
    cudf.dataframe.categorical.CategoricalColumn,
    cudf.dataframe.index.RangeIndex,
    cudf.dataframe.index.GenericIndex,
    cudf.dataframe.index.DatetimeIndex,
    cudf.dataframe.index.CategoricalIndex,
    cudf.dataframe.series.Series,
    cudf.dataframe.dataframe.DataFrame,
]
for cls in classes:
    dask_serialize._lookup.pop(cls, None)
    dask_deserialize._lookup.pop(cls, None)


# TODO:
# 1. Just use positions
#    a. Fixes duplicate columns
#    b. Fixes non-msgpack-serializable names
# 2. cudf.Series
# 3. Serialize the index


@dask_serialize.register(cudf.dataframe.buffer.Buffer)
def serialize_cudf_buffer(x):
    return serialize_numba_ndarray(x.mem)


# Index seems to be a little rough right now. Ignoring.

# @dask_serialize.register(cudf.dataframe.index.GenericIndex)
# def serialize_cudf_index(x):
#     # TODO: verify if nulls allowed in the index.
#     header, frames = serialize_cudf_buffer(x.index.as_column().data)
#     header['name'] = x.name
#     return header, frames


@dask_serialize.register(cudf.Series)
def serialize_cudf_series(x):
    name = 0 if x.name is None else x.name
    frame = cudf.DataFrame({name: x})
    header, arrays = serialize_cudf_dataframe(frame)
    header['is_series'] = True
    return header, arrays


@dask_serialize.register(cudf.DataFrame)
def serialize_cudf_dataframe(x):
    subheaders = []
    arrays = []
    null_masks = []
    null_headers = []
    null_counts = {}

    for label, col in x.iteritems():
        header, [frame] = serialize_numba_ndarray(col.data.mem)
        header['name'] = label
        subheaders.append(header)
        arrays.append(frame)
        if col.null_count:
            header, [frame] = serialize_numba_ndarray(col.nullmask.mem)
            header['name'] = label
            null_headers.append(header)
            null_masks.append(frame)
            null_counts[label] = col.null_count

    arrays.extend(null_masks)

    # index_header, index_frames = serialize_cudf_index(x.index)
    lengths = [subheader['lengths'][0] for subheader in subheaders]
    lengths.extend([nullheader['lengths'][0]
                    for nullheader in null_headers])

    header = {
        'lengths': lengths,
        'is_cuda': len(arrays),
        'subheaders': subheaders,
        'columns': pickle.dumps(x.columns),
        'null_counts': null_counts,
        'null_subheaders': null_headers,
        # 'index_header': index_header,
        # 'index_frames': index_frames,
    }

    return header, arrays


# ----------------------------------------------------------------------------
# Deserialize


@dask_deserialize.register(cudf.dataframe.buffer.Buffer)
def deserialize_cudf_buffer(header, frames):
    mem = deserialize_numba_ndarray(header, frames)
    return cudf.dataframe.buffer.Buffer(mem)


# @dask_deserialize.register(cudf.dataframe.index.GenericIndex)
# def deserialize_cudf_index(header, frames):
#     buf = deserialize_cudf_buffer(header, frames)
#     raise


@dask_deserialize.register(cudf.Series)
@dask_deserialize.register(cudf.DataFrame)
def deserialize_cudf_dataframe(header, frames):
    columns = pickle.loads(header['columns'])
    n_columns = len(columns)
    n_masks = len(header['null_subheaders'])

    masks = {}
    pairs = []

    for i in range(n_masks):
        subheader = header['null_subheaders'][i]
        frame = frames[n_columns + i]
        mask = deserialize_numba_ndarray(subheader, [frame])
        masks[subheader['name']] = mask

    for subheader, frame in zip(header['subheaders'], frames[:n_columns]):
        name = subheader['name']
        array = deserialize_numba_ndarray(subheader, [frame])

        if name in masks:
            series = cudf.Series.from_masked_array(array, masks[name])
        else:
            series = cudf.Series(array)
        pairs.append((name, series))

    if header.get('is_series', False):
        assert len(pairs) == 1
        name, series = pairs[0]
        series.name = name
        return series
    else:
        return cudf.DataFrame(pairs)
