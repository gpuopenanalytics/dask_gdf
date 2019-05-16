import os
import cudf
import dask
import dask.dataframe as dd
from glob import glob

def read_json(
    url_path,
    orient="records",
    lines=None,
    storage_options=None,
    blocksize=None,
    sample=2 ** 20,
    encoding="utf-8",
    errors="strict",
    compression="infer",
    meta=None,
    **kwargs
):
    if blocksize or not isinstance(url_path, str):
        return dd.read_json(
            url_path,
            orient=orient,
            lines=lines,
            storage_options=storage_options,
            blocksize=blocksize,
            sample=sample,
            encoding=encoding,
            errors=errors,
            compression=compression,
            meta=meta,
            **kwargs
        )
    else:
        if lines is None:
            lines = orient == "records"
        if orient != 'records' and lines:
            raise ValueError('Line-delimited JSON is only available with'
                             'orient="records".')
        storage_options = storage_options or {}
        files = sorted(glob(str(url_path)))
        parts = [dask.delayed(_read_json_file)(f, orient, lines, kwargs)
                 for f in files]
        return dd.from_delayed(parts, meta=meta)


def _read_json_file(f, orient, lines, kwargs):
    return cudf.read_json(f, orient=orient, lines=lines, **kwargs)
