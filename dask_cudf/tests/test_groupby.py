import dask.dataframe as dd
import dask_cudf
import pandas as pd
import cudf
import numpy as np

import pytest


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.groupby("x").sum(),
        lambda df: df.groupby("x").mean(),
        lambda df: df.groupby("x").count(),
        lambda df: df.groupby("x").min(),
        lambda df: df.groupby("x").max(),
        lambda df: df.groupby("x").std(),
        lambda df: df.groupby("x").y.sum(),
        lambda df: df.groupby("x").y.std(),
    ],
)
def test_groupby(func):
    pdf = pd.DataFrame(
        {"x": np.random.randint(0, 5, size=10000), "y": np.random.normal(size=10000)}
    )

    gdf = cudf.DataFrame.from_pandas(pdf)

    ddf = dask_cudf.from_cudf(gdf, npartitions=5)

    a = func(gdf).to_pandas()
    b = func(ddf).compute().to_pandas()

    a.index.name = None
    b.index.name = None

    dd.assert_eq(a, b)
