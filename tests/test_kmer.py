from polars_bio.io import read_fastq
from polars_bio.kmer import kmer_count

f = read_fastq("tests/data/kmer/example.fastq")

print(f.select("sequence").collect())
print(kmer_count(5))
