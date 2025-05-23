use arrow_array::{Array, ArrayRef, StringArray};
use datafusion::error::DataFusionError;
use std::collections::HashMap;
use datafusion::common::{ScalarValue};
use datafusion::scalar::ScalarValue::Utf8;
use datafusion::{error::Result, physical_plan::Accumulator};
use serde_json;

#[derive(Debug)]
pub struct KmerAccumulator {
    k: usize,
    counts: HashMap<String, usize>,
}

impl KmerAccumulator {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            counts: HashMap::new(),
        }
    }
}

impl Accumulator for KmerAccumulator {
        fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        if let Some(array) = values.get(0) {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;
            for i in 0..string_array.len() {
                if let Some(seq) = string_array.value(i).into() {
                    for j in 0..=(seq.len().saturating_sub(self.k)) {
                        let kmer = &seq[j..j + self.k];
                        if kmer.contains('N') {
                            continue;
                        }
                        *self.counts.entry(kmer.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
    if let Some(array) = states.get(0) {
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;
        for i in 0..string_array.len() {
            if string_array.is_valid(i) {
                let state_str = string_array.value(i);
                let other_map: HashMap<String, usize> = serde_json::from_str(state_str)
                    .map_err(|e| DataFusionError::Execution(format!("Failed to parse state: {}", e)))?;
                for (k, v) in other_map {
                    *self.counts.entry(k).or_insert(0) += v;
                }
            }
        }
    }
    Ok(())
}
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let json = serde_json::to_string(&self.counts).map_err(|e| {
            DataFusionError::Execution(format!("Failed to serialize state: {}", e))
        })?;
        Ok(vec![Utf8(Some(json))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let json = serde_json::to_string(&self.counts).map_err(|e| {
            DataFusionError::Execution(format!("Failed to serialize output: {}", e))
        })?;
        Ok(Utf8(Some(json)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
