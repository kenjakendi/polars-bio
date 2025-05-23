import polars as pl

from .context import ctx

from polars_bio.polars_bio import (
    py_kmer_count,
)

def kmer_count(k, df):

    reader = (
        df.to_arrow()
        if isinstance(df, pl.DataFrame)
        else df.collect().to_arrow().to_reader()
    )

    return py_kmer_count(ctx, k, reader)
