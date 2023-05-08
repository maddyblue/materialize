// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

// Test determine_timestamp.

use std::collections::{BTreeMap, BTreeSet};

use mz_sql_parser::ast::TransactionIsolationLevel;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use mz_adapter::{
    catalog::CatalogState, session::Session, CollectionIdBundle, TimelineContext, TimestampProvider,
};
use mz_compute_client::controller::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::QueryWhen;
use mz_storage_client::types::sources::Timeline;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
struct Set {
    ids: BTreeMap<String, Vec<Timestamp>>,
}

impl Set {
    fn to_compute_frontiers(self) -> BTreeMap<(ComputeInstanceId, GlobalId), Frontier> {
        let mut m = BTreeMap::new();
        for (id, v) in self.ids {
            let (instance, id) = id.split_once(',').unwrap();
            let instance: ComputeInstanceId = instance.parse().unwrap();
            let id: GlobalId = id.parse().unwrap();
            m.insert((instance, id), v.into());
        }
        m
    }
    fn to_storage_frontiers(self) -> BTreeMap<GlobalId, Frontier> {
        let mut m = BTreeMap::new();
        for (id, v) in self.ids {
            let id: GlobalId = id.parse().unwrap();
            m.insert(id, v.into());
        }
        m
    }
}

struct Frontiers {
    compute: BTreeMap<(ComputeInstanceId, GlobalId), Frontier>,
    storage: BTreeMap<GlobalId, Frontier>,
    oracle: Timestamp,
}

struct Frontier {
    read: Antichain<Timestamp>,
    write: Antichain<Timestamp>,
}

impl From<Vec<Timestamp>> for Frontier {
    fn from(val: Vec<Timestamp>) -> Self {
        if val.len() != 2 {
            panic!("expected exactly 2 entries");
        }
        Frontier {
            read: Antichain::from_elem(val[0]),
            write: Antichain::from_elem(val[1]),
        }
    }
}

impl TimestampProvider for Frontiers {
    fn compute_read_frontier<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.compute.get(&(instance, id)).unwrap().read.borrow()
    }

    fn compute_read_capability<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.compute.get(&(instance, id)).unwrap().read
    }

    fn compute_write_frontier<'a>(
        &'a self,
        instance: ComputeInstanceId,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.compute.get(&(instance, id)).unwrap().write.borrow()
    }

    fn storage_read_capabilities<'a>(
        &'a self,
        id: GlobalId,
    ) -> timely::progress::frontier::AntichainRef<'a, Timestamp> {
        self.storage.get(&id).unwrap().read.borrow()
    }

    fn storage_implied_capability<'a>(
        &'a self,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.storage.get(&id).unwrap().read
    }

    fn storage_write_frontier<'a>(
        &'a self,
        id: GlobalId,
    ) -> &'a timely::progress::Antichain<Timestamp> {
        &self.storage.get(&id).unwrap().write
    }

    fn oracle_read_ts(&self, timeline: &Timeline) -> Option<Timestamp> {
        matches!(timeline, Timeline::EpochMilliseconds).then(|| self.oracle)
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Determine {
    id_bundle: IdBundle,
    when: QueryWhen,
    instance: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct IdBundle {
    #[serde(default)]
    storage_ids: BTreeSet<String>,
    #[serde(default)]
    compute_ids: BTreeMap<String, BTreeSet<String>>,
}

impl From<IdBundle> for CollectionIdBundle {
    fn from(val: IdBundle) -> CollectionIdBundle {
        CollectionIdBundle {
            storage_ids: BTreeSet::from_iter(val.storage_ids.iter().map(|id| id.parse().unwrap())),
            compute_ids: BTreeMap::from_iter(val.compute_ids.iter().map(|(id, set)| {
                let set = BTreeSet::from_iter(set.iter().map(|s| s.parse().unwrap()));
                (id.parse().unwrap(), set)
            })),
        }
    }
}

#[test]
fn test_timestamp_selection() {
    datadriven::walk("tests/testdata/timestamp_selection", |tf| {
        let mut f = Frontiers {
            compute: BTreeMap::new(),
            storage: BTreeMap::new(),
            oracle: Timestamp::MIN,
        };
        let catalog = CatalogState::empty();
        let mut session = Session::dummy()
            .start_transaction(
                mz_ore::now::to_datetime(0),
                None,
                Some(TransactionIsolationLevel::StrictSerializable),
            )
            .0;
        tf.run(move |tc| -> String {
            match tc.directive.as_str() {
                "set-compute" => {
                    let set: Set = serde_json::from_str(&tc.input).unwrap();
                    f.compute = set.to_compute_frontiers();
                    "".into()
                }
                "set-storage" => {
                    let set: Set = serde_json::from_str(&tc.input).unwrap();
                    f.storage = set.to_storage_frontiers();
                    "".into()
                }
                "set-oracle" => {
                    let set: Timestamp = serde_json::from_str(&tc.input).unwrap();
                    f.oracle = set;
                    "".into()
                }
                "set-isolation" => {
                    let level = tc.input.trim().to_uppercase();
                    let set = if level == TransactionIsolationLevel::StrictSerializable.to_string()
                    {
                        TransactionIsolationLevel::StrictSerializable
                    } else if level == TransactionIsolationLevel::Serializable.to_string() {
                        TransactionIsolationLevel::Serializable
                    } else {
                        panic!("unknown level {}", tc.input);
                    };
                    session = Session::dummy()
                        .start_transaction(mz_ore::now::to_datetime(0), None, Some(set))
                        .0;
                    "".into()
                }
                "determine" => {
                    let det: Determine = serde_json::from_str(&tc.input).unwrap();
                    let ts = f
                        .determine_timestamp_for(
                            &catalog,
                            &session,
                            &det.id_bundle.into(),
                            &det.when,
                            det.instance.parse().unwrap(),
                            TimelineContext::TimestampDependent,
                            None,
                        )
                        .unwrap();
                    if tc.args.contains_key("full") {
                        format!("{}\n", serde_json::to_string_pretty(&ts).unwrap())
                    } else {
                        format!("{}\n", ts.timestamp_context.timestamp_or_default())
                    }
                }
                _ => panic!("unknown directive {}", tc.directive),
            }
        })
    })
}
