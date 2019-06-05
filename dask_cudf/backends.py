import numpy as np
import pandas as pd

from dask.utils import typename
from dask.dataframe.methods import concat_dispatch
from dask.dataframe.utils import is_integer_na_dtype, _scalar_from_dtype
from dask.dataframe.core import get_parallel_type, meta_nonempty, make_meta

import cudf
from cudf.dataframe.index import (
    DatetimeIndex,
    GenericIndex,
    CategoricalIndex,
    StringIndex,
    RangeIndex,
)
from cudf import MultiIndex


from pandas.api.types import (
    is_categorical_dtype,
    is_period_dtype,
    is_datetime64tz_dtype,
)

from .core import DataFrame, Series, Index

get_parallel_type.register(cudf.DataFrame, lambda _: DataFrame)
get_parallel_type.register(cudf.Series, lambda _: Series)
get_parallel_type.register(cudf.Index, lambda _: Index)


@meta_nonempty.register((cudf.DataFrame, cudf.Series, cudf.Index))
def meta_nonempty_cudf(x, index=None):

    idx = _nonempty_index(x.index)
    data = {i: _nonempty_series(x.iloc[:, i], idx=idx) for i, c in enumerate(x.columns)}
    res = cudf.DataFrame(data, index=idx)
    res.columns = x.columns
    return res


@make_meta.register((cudf.Series, cudf.DataFrame))
def make_meta_cudf(x, index=None):
    return x.head(0)


@make_meta.register(cudf.Index)
def make_meta_cudf_index(x, index=None):
    return x[:0]


@concat_dispatch.register((cudf.DataFrame, cudf.Series, cudf.Index))
def concat_cudf(dfs, axis=0, join="outer", uniform=False, filter_warning=True):
    assert axis == 0
    assert join == "outer"
    return cudf.concat(dfs)


@meta_nonempty.register(cudf.Series)
def _nonempty_series(s, idx=None):
    if idx is None:
        idx = _nonempty_index(s.index)
    dtype = s.dtype
    if is_datetime64tz_dtype(dtype):
        entry = pd.Timestamp("1970-01-01", tz=dtype.tz)
        data = [entry, entry]
    elif is_categorical_dtype(dtype):
        if len(s.cat.categories):
            data = [s.cat.categories[0]] * 2
            cats = s.cat.categories
        else:
            data = _nonempty_index(s.cat.categories)
            cats = None
        data = pd.Categorical(data, categories=cats, ordered=s.cat.ordered)
    elif is_integer_na_dtype(dtype):
        data = pd.array([1, None], dtype=dtype)
    elif is_period_dtype(dtype):
        # pandas 0.24.0+ should infer this to be Series[Period[freq]]
        freq = dtype.freq
        data = [pd.Period("2000", freq), pd.Period("2001", freq)]
    else:
        entry = _scalar_from_dtype(dtype)
        data = np.array([entry, entry], dtype=dtype)

    return cudf.Series(data, name=s.name, index=idx)


@meta_nonempty.register(cudf.Index)
def _nonempty_index(idx):
    typ = type(idx)
    if typ is RangeIndex:
        return typ(2, name=idx.name)
    elif typ is GenericIndex:
        return typ([1, 2], name=idx.name)
    elif typ is StringIndex:
        return typ(["a", "b"], name=idx.name)
    elif typ is CategoricalIndex:
        if len(idx.categories) == 0:
            data = pd.Categorical(_nonempty_index(idx.categories), ordered=idx.ordered)
        else:
            data = pd.Categorical.from_codes(
                [-1, 0], categories=idx.categories, ordered=idx.ordered
            )
        return type(data, name=idx.name)
    elif typ is DatetimeIndex:
        start = "1970-01-01"
        # Need a non-monotonic decreasing index to avoid issues with
        # partial string indexing see https://github.com/dask/dask/issues/2389
        # and https://github.com/pandas-dev/pandas/issues/16515
        # This doesn't mean `_meta_nonempty` should ever rely on
        # `self.monotonic_increasing` or `self.monotonic_decreasing`
        try:
            dates = pd.date_range(
                start=start, periods=2, freq=idx.freq, tz=idx.tz, name=idx.name
            )
        except ValueError:  # older pandas versions
            data = [start, "1970-01-02"] if idx.freq is None else None
            dates = pd.DatetimeIndex(
                data, start=start, periods=2, freq=idx.freq, tz=idx.tz, name=idx.name
            )
        return type(dates, name=idx.name)
    elif typ is MultiIndex:
        levels = [_nonempty_index(l) for l in idx.levels]
        codes = [[0, 0] for i in idx.levels]
        try:
            return typ(levels=levels, codes=codes, names=idx.names)
        except TypeError:  # older pandas versions
            return typ(levels=levels, labels=codes, names=idx.names)

    raise TypeError(
        "Don't know how to handle index of " "type {0}".format(typename(type(idx)))
    )
