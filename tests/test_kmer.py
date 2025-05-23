from polars_bio.io import read_fastq
from polars_bio.kmer import kmer_count

f = read_fastq("tests/data/io/fastq/example.fastq")
print(kmer_count(5, f))
