import dask
import dask_cudf
import dask.dataframe as dd
from dask.utils import tmpfile
import pandas as pd

import pytest


def test_read_json(tmp_path):
    df = dask.datasets.timeseries(
        dtypes={"x": int, "y": int}, freq="120s").reset_index(drop=True)
    df.to_json(tmp_path / "data-*.json")
    df2 = dask_cudf.read_json(tmp_path / "data-*.json")
    dd.assert_eq(df, df2)

@pytest.mark.filterwarnings("ignore:Using CPU")
@pytest.mark.parametrize('orient', ['split', 'records', 'index', 'columns',
                                    'values'])
def test_read_json_basic(orient):
    df = pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                       'y': [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)
    with tmpfile('json') as f:
        df.to_json(f, orient=orient, lines=False)
        actual = dask_cudf.read_json(f, orient=orient, lines=False)
        actual_pd = pd.read_json(f, orient=orient, lines=False)

        out = actual.compute()
        dd.assert_eq(out, actual_pd)
        if orient == 'values':
            out.columns = list(df.columns)
        dd.assert_eq(out, df)
