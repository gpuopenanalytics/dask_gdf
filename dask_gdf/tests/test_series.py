
import numpy as np
import pandas as pd
import pytest

import pygdf as gd
import dask_gdf as dgd
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq


def _make_random_frame(nelem, npartitions=2):
    df = pd.DataFrame({'x': np.random.randint(0, 5, size=nelem),
                       'y': np.random.normal(size=nelem) + 1})
    gf = gd.DataFrame.from_pandas(df)
    return df, gf


_reducers = [
    'sum',
    'count',
    'mean',
    'var',
    'std',
    'min',
    'max',
]


@pytest.mark.parametrize('func', [
    lambda x: x,
    lambda x: x.nlargest(2),
    lambda x: x.nsmallest(2),
    lambda x: x + 1,
    lambda x: x * 2,
    lambda x: x ** 2,
])
def test_series_reduce(func):
    df, gf = _make_random_frame(20)
    ddf = dd.from_pandas(df, npartitions=3)
    gdf = dgd.from_pygdf(gf, npartitions=3)

    a = func(ddf.x)
    b = func(gdf.x)
    assert_eq(a, b)
