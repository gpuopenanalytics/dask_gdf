import dask
import dask_cudf
import dask.dataframe as dd
import pandas as pd
import numpy as np


def test_read_csv(tmp_path):
    df = dask.datasets.timeseries(dtypes={"x": int, "y": int}, freq="120s").reset_index(
        drop=True
    )
    df.to_csv(tmp_path / "data-*.csv", index=False)

    df2 = dask_cudf.read_csv(tmp_path / "*.csv")
    dd.assert_eq(df, df2)


def test_read_csv_w_bytes(tmp_path):
    df = dask.datasets.timeseries(dtypes={"x": int, "y": int}, freq="120s").reset_index(
        drop=True
    )
    df = pd.DataFrame(dict(x=np.arange(20), y=np.arange(20)))
    df = dd.from_pandas(df, npartitions=1)
    df.to_csv(tmp_path / "data-*.csv", index=False)

    df2 = dask_cudf.read_csv(tmp_path / "*.csv", chunksize='50 B')
#    assert df2.npartitions > df.npartitions

    result = df2.compute().to_pandas()
    expected = df.compute()
    breakpoint()
    dd.assert_eq(result, expected, check_index=False)

def test_cudf_read_csv_chunks():
    import cudf
    import os
    df = pd.DataFrame(dict(x=np.arange(10), y=np.arange(10)))
    df.to_csv('myfile.csv')

    a = cudf.read_csv('myfile.csv', byte_range=(0, 50))
    b = cudf.read_csv('myfile.csv', byte_range=(50, 50), header=None, names=a.columns)

    with open('myfile.csv') as f:
        data = f.read()

    print(df)
    print(a.to_pandas())
    print(b.to_pandas())

    print("50th byte is within the line of sevens")
    print(data[50:55])
    print("context (bytes 40-60):", data[40: 60])

    c = cudf.read_csv('myfile.csv', byte_range=(1000, 50))  # segfaults
