import pytest

import cudf
import dask
from dask.distributed import Client
import dask.dataframe as dd
from distributed.utils_test import loop  # noqa: F401

dask_cuda = pytest.importorskip("dask_cuda")


def test_basic(loop):  # noqa: F811
    with dask_cuda.LocalCUDACluster(loop=loop) as cluster:
        with Client(cluster):
            pdf = dask.datasets.timeseries()
            gdf = pdf.map_partitions(cudf.DataFrame.from_pandas)
            dd.assert_eq(pdf.head(), gdf.head())
