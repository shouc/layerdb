use anyhow::Context;
use arrow_array::{Float32Array, UInt64Array};

use crate::linalg::squared_l2;
use crate::types::Neighbor;

#[derive(Clone, Debug)]
pub struct VectorColumnarPage {
    ids: UInt64Array,
    values: Float32Array,
    dim: usize,
}

impl VectorColumnarPage {
    pub fn from_owned_rows(rows: Vec<(u64, Vec<f32>)>, dim: usize) -> anyhow::Result<Self> {
        if dim == 0 {
            anyhow::bail!("columnar page dim must be > 0");
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut flat = Vec::with_capacity(rows.len().saturating_mul(dim));
        for (id, values) in rows {
            if values.len() != dim {
                anyhow::bail!(
                    "columnar row dim mismatch for id={id}: got {} expected {}",
                    values.len(),
                    dim
                );
            }
            ids.push(id);
            flat.extend_from_slice(&values);
        }
        let ids = UInt64Array::from(ids);
        let values = Float32Array::from(flat);
        Ok(Self { ids, values, dim })
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn scan_l2(&self, query: &[f32], k: usize) -> anyhow::Result<Vec<Neighbor>> {
        if query.len() != self.dim {
            anyhow::bail!(
                "query dim mismatch in columnar page: got {} expected {}",
                query.len(),
                self.dim
            );
        }
        if k == 0 || self.is_empty() {
            return Ok(Vec::new());
        }

        let values = self.values.values();
        let mut out = Vec::with_capacity(k.min(self.len()));
        for row in 0..self.len() {
            let start = row
                .checked_mul(self.dim)
                .context("columnar row offset overflow")?;
            let end = start
                .checked_add(self.dim)
                .context("columnar row end overflow")?;
            let d = squared_l2(query, &values[start..end]);
            let n = Neighbor {
                id: self.ids.value(row),
                distance: d,
            };
            if out.len() < k {
                out.push(n);
                continue;
            }
            let mut worst_idx = 0usize;
            for i in 1..out.len() {
                if out[i].distance.total_cmp(&out[worst_idx].distance).is_gt() {
                    worst_idx = i;
                }
            }
            if n.distance.total_cmp(&out[worst_idx].distance).is_lt() {
                out[worst_idx] = n;
            }
        }
        out.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::VectorColumnarPage;

    #[test]
    fn scan_l2_returns_nearest_rows() -> anyhow::Result<()> {
        let rows = vec![
            (1u64, vec![0.0, 0.0, 0.0, 0.0]),
            (2u64, vec![1.0, 0.0, 0.0, 0.0]),
            (3u64, vec![0.0, 1.0, 0.0, 0.0]),
        ];
        let page = VectorColumnarPage::from_owned_rows(rows, 4)?;
        let got = page.scan_l2(&[0.9, 0.0, 0.0, 0.0], 2)?;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, 2);
        Ok(())
    }
}
