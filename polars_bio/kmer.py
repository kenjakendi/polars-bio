from .context import ctx

from polars_bio.polars_bio import (
    py_kmer_count,
)

def kmer_count(k,):
    return py_kmer_count(ctx, k)