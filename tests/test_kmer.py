import pandas as pd

from polars_bio.io import read_fastq
from polars_bio.kmer import kmer_count

from _expected import (
    DATA_DIR,
    PD_KMER_2_TEST,
    PD_KMER_2_EXAMPLE
)

class TestKmerCount:
    def test_kmer_count_2_test_lazy_frame(self):
        file = read_fastq(f"{DATA_DIR}/io/fastq/test.fastq")
        result = kmer_count(2, file).sort_values(by="kmer").reset_index(drop=True)
        expected = PD_KMER_2_TEST
        pd.testing.assert_frame_equal(result, expected)
    
    def test_kmer_count_2_test_data_frame(self):
        file = read_fastq(f"{DATA_DIR}/io/fastq/test.fastq").collect()
        result = kmer_count(2, file).sort_values(by="kmer").reset_index(drop=True)
        expected = PD_KMER_2_TEST
        pd.testing.assert_frame_equal(result, expected)
    
    def test_kmer_count_2_example_lazy_frame(self):
        file = read_fastq(f"{DATA_DIR}/io/fastq/example.fastq")
        result = kmer_count(2, file).sort_values(by="kmer").reset_index(drop=True)
        expected = PD_KMER_2_EXAMPLE
        pd.testing.assert_frame_equal(result, expected)

    def test_kmer_count_2_example_data_frame(self):
        file = read_fastq(f"{DATA_DIR}/io/fastq/example.fastq").collect()
        result = kmer_count(2, file).sort_values(by="kmer").reset_index(drop=True)
        expected = PD_KMER_2_EXAMPLE
        pd.testing.assert_frame_equal(result, expected)
