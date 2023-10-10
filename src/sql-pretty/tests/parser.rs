// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
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

use datadriven::walk;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::datadriven_testcase;
use mz_sql_parser::parser::parse_statements;
use mz_sql_pretty::to_pretty;

// Use the parser's datadriven tests to get a comprehensive set of SQL statements. Assert they all
// generate identical ASTs when pretty printed. Output the same output as the parser so datadriven
// is happy. (Having the datadriven parser be exported would be nice here too.)
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_parser() {
    walk("../sql-parser/tests/testdata", |f| {
        f.run(|tc| -> String {
            if tc.directive == "parse-statement" {
                verify_pretty_print(&tc.input);
            }
            datadriven_testcase(tc)
        })
    });
}

fn verify_pretty_print(stmt: &str) {
    let original = match parse_statements(stmt) {
        Ok(stmt) => match stmt.into_iter().next() {
            Some(stmt) => stmt,
            None => return,
        },
        Err(_) => return,
    };
    for n in &[1, 40, 1000000] {
        let n = *n;
        let pretty1 = to_pretty(&original.ast, n);
        let prettied = parse_statements(&pretty1)
            .unwrap_or_else(|_| panic!("could not parse: {pretty1}, original: {stmt}"))
            .into_iter()
            .next()
            .unwrap();
        let pretty2 = to_pretty(&prettied.ast, n);
        assert_eq!(pretty1, pretty2);
        assert_eq!(
            original.ast.to_ast_string_stable(),
            prettied.ast.to_ast_string_stable(),
            "\noriginal: {stmt}",
        );
        // Everything should always squash to a single line.
        if n > (stmt.len() * 2) {
            assert_eq!(pretty1.lines().count(), 1, "{}: {}", n, pretty1);
        }
    }
}
