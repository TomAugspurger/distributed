import cudf
from .serialize import dask_serialize, dask_deserialize
from .numba import serialize_numba_ndarray, deserialize_numba_ndarray


@dask_serialize.register(cudf.DataFrame)
def serialize_cudf_dataframe(x):
    # TODO: does cudf support duplicate columns?
    sub_headers = []
    arrays = []
    null_masks = []
    null_headers = []
    null_counts = {}

    for label, col in x.iteritems():
        header, [frame] = serialize_numba_ndarray(col.data.mem)
        header['name'] = label
        sub_headers.append(header)
        arrays.append(frame)
        if col.null_count:
            header, [frame] = serialize_numba_ndarray(col.nullmask.mem)
            header['name'] = label
            null_headers.append(header)
            null_masks.append(frame)
            null_counts[label] = col.null_count

    arrays.extend(null_masks)

    header = {
        'lengths': [len(x)] * len(arrays),
        'is_cuda': len(arrays),
        'subheaders': sub_headers,
        'columns': x.columns.tolist(),  # TODO: ugh...
        'null_counts': null_counts,
        'null_subheaders': null_headers
    }

    return header, arrays


@dask_deserialize.register(cudf.DataFrame)
def serialize_cudf_dataframe(header, frames):
    # TODO: duplicate columns

    columns = header['columns']
    n_columns = len(header['columns'])
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

    return cudf.DataFrame(pairs)
