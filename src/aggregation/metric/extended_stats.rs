use columnar::ColumnType;
use serde::{Deserialize, Serialize};

use crate::{
    aggregation::{
        agg_req_with_accessor::AggregationWithAccessor,
        f64_from_fastfield_u64,
        intermediate_agg_result::{IntermediateAggregationResult, IntermediateMetricResult},
        segment_agg_result::SegmentAggregationCollector,
    },
    DocId, TantivyError,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtendedStatsAggregation {
    /// The field name to compute the extended stats on.
    pub field: String,
}

impl ExtendedStatsAggregation {
    pub fn from_field_name(field_name: String) -> Self {
        ExtendedStatsAggregation { field: field_name }
    }
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Extended stats a collection of statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtendedStats {
    /// The number of documents.
    pub count: u64,
    /// The sum of the fast field values.
    pub sum: f64,
    /// The min value of the fast field values.
    pub min: Option<f64>,
    /// The max value of the fast field values.
    pub max: Option<f64>,
    /// The average of the fast field values. `None` if count equals zero.
    pub avg: Option<f64>,
    /// todo The squares of sum of the fast field values.
    pub sum_of_squares: Option<f64>,
}

impl ExtendedStats {
    pub(crate) fn get_value(&self, agg_property: &str) -> crate::Result<Option<f64>> {
        match agg_property {
            "count" => Ok(Some(self.count as f64)),
            "sum" => Ok(Some(self.sum)),
            "min" => Ok(self.min),
            "max" => Ok(self.max),
            "avg" => Ok(self.avg),
            "sum_of_squares" => Ok(self.sum_of_squares),
            _ => Err(TantivyError::InvalidArgument(format!(
                "Unknown property {agg_property} on extended stats metric aggregation"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateExtendedStats {
    /// The number of extracted values.
    pub count: u64,
    /// The sum of the extracted values.
    sum: f64,
    /// The min value.
    min: f64,
    /// The max value.
    max: f64,
}

impl Default for IntermediateExtendedStats {
    fn default() -> Self {
        IntermediateExtendedStats {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
}

impl IntermediateExtendedStats {
    /// Merges the other stats intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateExtendedStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Computes the final extended stats value.
    pub fn finalize(&self) -> ExtendedStats {
        let min = if self.count == 0 {
            None
        } else {
            Some(self.min)
        };
        let max = if self.count == 0 {
            None
        } else {
            Some(self.max)
        };
        let avg = if self.count == 0 {
            None
        } else {
            Some(self.sum / (self.count as f64))
        };
        let sum_of_squares = if self.count == 0 {
            None
        } else {
            Some(self.sum * self.sum)
        };
        ExtendedStats {
            count: self.count,
            sum: self.sum,
            min,
            max,
            avg,
            sum_of_squares,
        }
    }

    #[inline]
    fn collect(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentExtendedStatsType {
    ExtendedStats,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentExtendedStatsCollector {
    field_type: ColumnType,
    pub(crate) collecting_for: SegmentExtendedStatsType,
    pub(crate) extended_stats: IntermediateExtendedStats,
    pub(crate) accessor_idx: usize,
    val_cache: Vec<u64>,
}

impl SegmentExtendedStatsCollector {
    pub fn from_req(
        field_type: ColumnType,
        collecting_for: SegmentExtendedStatsType,
        accessor_idx: usize,
    ) -> Self {
        Self {
            field_type,
            collecting_for,
            extended_stats: IntermediateExtendedStats::default(),
            accessor_idx,
            val_cache: Vec::new(),
        }
    }
    #[inline]
    pub(crate) fn collect_block_with_field(
        &mut self,
        docs: &[DocId],
        agg_accessor: &mut AggregationWithAccessor,
    ) {
        agg_accessor
            .column_block_accessor
            .fetch_block(docs, &agg_accessor.accessor);

        for val in agg_accessor.column_block_accessor.iter_vals() {
            let val1 = f64_from_fastfield_u64(val, &self.field_type);
            self.extended_stats.collect(val1);
        }
    }
}

impl SegmentAggregationCollector for SegmentExtendedStatsCollector {
    #[inline]
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
        results: &mut crate::aggregation::intermediate_agg_result::IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let intermediate_metric_result = match self.collecting_for {
            SegmentExtendedStatsType::ExtendedStats => {
                IntermediateMetricResult::ExtendedStats(self.extended_stats)
            }
        };

        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_metric_result),
        )?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &agg_with_accessor.aggs.values[self.accessor_idx].accessor;

        for val in field.values_for_doc(doc) {
            let val1 = f64_from_fastfield_u64(val, &self.field_type);
            self.extended_stats.collect(val1);
        }

        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let field = &mut agg_with_accessor.aggs.values[self.accessor_idx];
        self.collect_block_with_field(docs, field);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;
    use serde_json::Value;

    #[test]
    fn test_aggregation_extended_stats_empty_index() -> crate::Result<()> {
        let values = vec![];
        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "extended_stats": {
                "extended_stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(
            res["extended_stats"],
            json!({
                "count": 0,
            })
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_extended_stats_index() -> crate::Result<()> {
        let values = vec![10.0, 20.0];
        let index = get_test_index_from_values(false, &values)?;

        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "extended_stats": {
                "extended_stats": {
                    "field": "score",
                },
            }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(agg_req_1, Default::default());
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(
            res["extended_stats"],
            json!({
                "avg": 15.0,
                "count": 2,
                "sum": 30.0,
                "max": 20.0,
                "min": 10.0,
                "sum_of_squares": 900.0,
            })
        );

        Ok(())
    }
}
