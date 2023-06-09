// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_repr::GlobalId;
use prometheus::core::AtomicU64;

use crate::source::metrics::SourceBaseMetrics;

pub(super) struct MsSourceMetrics {
    pub inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub updates: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub deletes: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub ignored: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub total: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub transactions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub tables: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub lsn: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl MsSourceMetrics {
    pub(super) fn new(base_metrics: &SourceBaseMetrics, source_id: GlobalId) -> Self {
        let labels = &[source_id.to_string()];
        let ms_metrics = &base_metrics.mssql_source_specific;
        Self {
            inserts: ms_metrics
                .insert_messages
                .get_delete_on_drop_counter(labels.to_vec()),
            updates: ms_metrics
                .update_messages
                .get_delete_on_drop_counter(labels.to_vec()),
            deletes: ms_metrics
                .delete_messages
                .get_delete_on_drop_counter(labels.to_vec()),
            ignored: ms_metrics
                .ignored_messages
                .get_delete_on_drop_counter(labels.to_vec()),
            total: ms_metrics
                .total_messages
                .get_delete_on_drop_counter(labels.to_vec()),
            transactions: ms_metrics
                .transactions
                .get_delete_on_drop_counter(labels.to_vec()),
            tables: ms_metrics
                .tables_in_publication
                .get_delete_on_drop_gauge(labels.to_vec()),
            lsn: ms_metrics.wal_lsn.get_delete_on_drop_gauge(labels.to_vec()),
        }
    }
}
