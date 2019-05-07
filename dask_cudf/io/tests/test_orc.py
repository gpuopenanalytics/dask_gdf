import os

import dask_cudf
import dask.dataframe as dd
import cudf
import numpy as np

import pytest

# import pyarrow.orc as orc

cur_dir = os.path.dirname(__file__)
sample_orc = os.path.join(cur_dir, "sample.orc")


def test_read_orc_defaults():
    df1 = cudf.read_orc(sample_orc)
    df2 = dask_cudf.read_orc(sample_orc)
    df2.head().to_pandas()
    dd.assert_eq(df1, df2, check_index=False)


# engine pyarrow fails
# https://github.com/rapidsai/cudf/issues/1595
@pytest.mark.parametrize("engine", ["cudf"])
@pytest.mark.parametrize("columns", [["time", "date"], ["time"]])
def test_read_orc_cols(engine, columns):
    df1 = cudf.read_orc(sample_orc, engine=engine, columns=columns)

    df2 = dask_cudf.read_orc(sample_orc, engine=engine, columns=columns)

    dd.assert_eq(df1, df2, check_index=False)
    np.testing.assert_array_equal(df1.columns, df2.columns)
    assert len(df2.columns) == len(columns)


@pytest.mark.parametrize('skip_rows', [0, 5])
@pytest.mark.parametrize('num_rows', [10, 20])
def test_read_orc_skip_num(skip_rows, num_rows):
    df_total = cudf.read_orc(sample_orc)
    df1 = cudf.read_orc(sample_orc, skip_rows=skip_rows, num_rows=num_rows)

    df2 = dask_cudf.read_orc(sample_orc, skip_rows=skip_rows, num_rows=num_rows)

    dd.assert_eq(df1, df2, check_index=False)

    # check num_rows
    assert len(df2) == num_rows

    # check skip_rows
    dd.assert_eq(df_total.iloc[skip_rows:num_rows + skip_rows], df2, check_index=False)
