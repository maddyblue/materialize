// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions that convert SQL AST nodes to pretty Docs.

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::*;
use pretty::{Doc, RcDoc};

use crate::util::{bracket, bracket_doc, comma_separated, intersperse_line_nest, nest, nest_title};
use crate::{Prettier, TAB};

impl Prettier {
    // Use when we don't know what to do.
    pub(crate) fn doc_display<T: AstDisplay>(&self, v: &T, _debug: &'static str) -> RcDoc {
        #[cfg(test)]
        eprintln!(
            "UNKNOWN PRETTY TYPE in {}: {}, {}",
            _debug,
            std::any::type_name::<T>(),
            v.to_ast_string()
        );
        self.doc_display_pass(v)
    }

    // Use when the AstDisplay trait is what we want.
    pub(crate) fn doc_display_pass<T: AstDisplay>(&self, v: &T) -> RcDoc {
        RcDoc::text(v.to_ast_string())
    }

    pub(crate) fn doc_copy<'a, T: AstInfo>(&'a self, v: &'a CopyStatement<T>) -> RcDoc<'a, ()> {
        let relation = match &v.relation {
            CopyRelation::Table { name, columns } => {
                let mut relation = self.doc_display_pass(name);
                if !columns.is_empty() {
                    relation = bracket_doc(
                        nest(relation, RcDoc::text("(")),
                        self.comma_separate(Self::doc_display_pass, columns),
                        RcDoc::text(")"),
                        RcDoc::line_(),
                    );
                }
                RcDoc::concat([RcDoc::text("COPY "), relation])
            }
            CopyRelation::Select(query) => bracket("COPY (", self.doc_select_statement(query), ")"),
            CopyRelation::Subscribe(query) => bracket("COPY (", self.doc_subscribe(query), ")"),
        };
        let mut docs = vec![
            relation,
            RcDoc::concat([
                self.doc_display_pass(&v.direction),
                RcDoc::text(" "),
                self.doc_display_pass(&v.target),
            ]),
        ];
        if !v.options.is_empty() {
            docs.push(bracket(
                "WITH (",
                self.comma_separate(Self::doc_display_pass, &v.options),
                ")",
            ));
        }
        RcDoc::intersperse(docs, Doc::line()).group()
    }

    pub(crate) fn doc_subscribe<'a, T: AstInfo>(
        &'a self,
        v: &'a SubscribeStatement<T>,
    ) -> RcDoc<'a, ()> {
        let doc = match &v.relation {
            SubscribeRelation::Name(name) => nest_title("SUBSCRIBE", self.doc_display_pass(name)),
            SubscribeRelation::Query(query) => bracket("SUBSCRIBE (", self.doc_query(query), ")"),
        };
        let mut docs = vec![doc];
        if !v.options.is_empty() {
            docs.push(bracket(
                "WITH (",
                self.comma_separate(Self::doc_display_pass, &v.options),
                ")",
            ));
        }
        if let Some(as_of) = &v.as_of {
            docs.push(self.doc_as_of(as_of));
        }
        if let Some(up_to) = &v.up_to {
            docs.push(nest_title("UP TO", self.doc_expr(up_to)));
        }
        match &v.output {
            SubscribeOutput::Diffs => {}
            SubscribeOutput::WithinTimestampOrderBy { order_by } => {
                docs.push(nest_title(
                    "WITHIN TIMESTAMP ORDER BY ",
                    self.comma_separate(Self::doc_order_by_expr, order_by),
                ));
            }
            SubscribeOutput::EnvelopeUpsert { key_columns } => {
                docs.push(bracket(
                    "ENVELOPE UPSERT (KEY (",
                    self.comma_separate(Self::doc_display_pass, key_columns),
                    "))",
                ));
            }
            SubscribeOutput::EnvelopeDebezium { key_columns } => {
                docs.push(bracket(
                    "ENVELOPE DEBEZIUM (KEY (",
                    self.comma_separate(Self::doc_display_pass, key_columns),
                    "))",
                ));
            }
        }
        RcDoc::intersperse(docs, Doc::line()).group()
    }

    pub(crate) fn doc_as_of<'a, T: AstInfo>(&'a self, v: &'a AsOf<T>) -> RcDoc<'a, ()> {
        let (title, expr) = match v {
            AsOf::At(expr) => ("AS OF", expr),
            AsOf::AtLeast(expr) => ("AS OFAT LEAST", expr),
        };
        nest_title(title, self.doc_expr(expr))
    }

    pub(crate) fn doc_create_view<'a, T: AstInfo>(
        &'a self,
        v: &'a CreateViewStatement<T>,
    ) -> RcDoc<'a, ()> {
        let mut docs = vec![];
        docs.push(RcDoc::text(format!(
            "CREATE{}{} VIEW{}",
            if v.if_exists == IfExistsBehavior::Replace {
                " OR REPLACE"
            } else {
                ""
            },
            if v.temporary { " TEMPORARY" } else { "" },
            if v.if_exists == IfExistsBehavior::Skip {
                " IF NOT EXISTS"
            } else {
                ""
            },
        )));
        docs.push(self.doc_view_definition(&v.definition));
        intersperse_line_nest(docs)
    }

    pub(crate) fn doc_create_materialized_view<'a, T: AstInfo>(
        &'a self,
        v: &'a CreateMaterializedViewStatement<T>,
    ) -> RcDoc<'a, ()> {
        let mut docs = vec![];
        docs.push(RcDoc::text(format!(
            "CREATE{} MATERIALIZED VIEW{} {}",
            if v.if_exists == IfExistsBehavior::Replace {
                " OR REPLACE"
            } else {
                ""
            },
            if v.if_exists == IfExistsBehavior::Skip {
                " IF NOT EXISTS"
            } else {
                ""
            },
            v.name,
        )));
        if !v.columns.is_empty() {
            docs.push(bracket(
                "(",
                self.comma_separate(Self::doc_display_pass, &v.columns),
                ")",
            ));
        }
        if let Some(cluster) = &v.in_cluster {
            docs.push(RcDoc::text(format!(
                "IN CLUSTER {}",
                cluster.to_ast_string()
            )));
        }
        if !v.with_options.is_empty() {
            docs.push(bracket(
                "WITH (",
                self.comma_separate(Self::doc_display_pass, &v.with_options),
                ")",
            ));
        }
        docs.push(nest_title("AS", self.doc_query(&v.query)));
        intersperse_line_nest(docs)
    }

    pub(crate) fn doc_view_definition<'a, T: AstInfo>(
        &'a self,
        v: &'a ViewDefinition<T>,
    ) -> RcDoc<'a, ()> {
        let mut docs = vec![RcDoc::text(v.name.to_string())];
        if !v.columns.is_empty() {
            docs.push(bracket(
                "(",
                self.comma_separate(Self::doc_display_pass, &v.columns),
                ")",
            ));
        }
        docs.push(nest_title("AS", self.doc_query(&v.query)));
        RcDoc::intersperse(docs, Doc::line()).group()
    }

    pub(crate) fn doc_insert<'a, T: AstInfo>(&'a self, v: &'a InsertStatement<T>) -> RcDoc<'a, ()> {
        let mut first = vec![RcDoc::text(format!(
            "INSERT INTO {}",
            v.table_name.to_ast_string()
        ))];
        if !v.columns.is_empty() {
            first.push(bracket(
                "(",
                self.comma_separate(Self::doc_display_pass, &v.columns),
                ")",
            ));
        }
        let sources = match &v.source {
            InsertSource::Query(query) => self.doc_query(query),
            _ => self.doc_display(&v.source, "insert source"),
        };
        let mut doc = intersperse_line_nest([intersperse_line_nest(first), sources]);
        if !v.returning.is_empty() {
            doc = nest(
                doc,
                nest_title(
                    "RETURNING",
                    self.comma_separate(Self::doc_select_item, &v.returning),
                ),
            )
        }
        doc
    }

    pub(crate) fn doc_select_statement<'a, T: AstInfo>(
        &'a self,
        v: &'a SelectStatement<T>,
    ) -> RcDoc<'a, ()> {
        let mut doc = self.doc_query(&v.query);
        if let Some(as_of) = &v.as_of {
            doc = intersperse_line_nest([doc, self.doc_display_pass(as_of)]);
        }
        doc.group()
    }

    pub(crate) fn doc_order_by<'a, T: AstInfo>(&'a self, v: &'a [OrderByExpr<T>]) -> RcDoc<'a, ()> {
        self.title_comma_separate("ORDER BY", Self::doc_order_by_expr, v)
    }

    pub(crate) fn doc_order_by_expr<'a, T: AstInfo>(
        &'a self,
        v: &'a OrderByExpr<T>,
    ) -> RcDoc<'a, ()> {
        let doc = self.doc_expr(&v.expr);
        let doc = match v.asc {
            Some(true) => nest(doc, RcDoc::text("ASC")),
            Some(false) => nest(doc, RcDoc::text("DESC")),
            None => doc,
        };
        match v.nulls_last {
            Some(true) => nest(doc, RcDoc::text("NULLS LAST")),
            Some(false) => nest(doc, RcDoc::text("NULLS FIRST")),
            None => doc,
        }
    }

    pub(crate) fn doc_query<'a, T: AstInfo>(&'a self, v: &'a Query<T>) -> RcDoc<'a, ()> {
        let mut docs = vec![];
        if !v.ctes.is_empty() {
            match &v.ctes {
                CteBlock::Simple(ctes) => {
                    docs.push(self.title_comma_separate("WITH", Self::doc_cte, ctes))
                }
                CteBlock::MutuallyRecursive(mutrec) => {
                    let mut doc = RcDoc::text("WITH MUTUALLY RECURSIVE");
                    if !mutrec.options.is_empty() {
                        doc = nest(
                            doc,
                            bracket(
                                "(",
                                self.comma_separate(Self::doc_display_pass, &mutrec.options),
                                ")",
                            ),
                        );
                    }
                    docs.push(nest(
                        doc,
                        self.comma_separate(Self::doc_mutually_recursive, &mutrec.ctes),
                    ));
                }
            }
        }
        docs.push(self.doc_set_expr(&v.body));
        if !v.order_by.is_empty() {
            docs.push(self.doc_order_by(&v.order_by));
        }

        let offset = if let Some(offset) = &v.offset {
            vec![RcDoc::concat([nest_title("OFFSET", self.doc_expr(offset))])]
        } else {
            vec![]
        };

        if let Some(limit) = &v.limit {
            if limit.with_ties {
                docs.extend(offset);
                docs.push(RcDoc::concat([
                    RcDoc::text("FETCH FIRST "),
                    self.doc_expr(&limit.quantity),
                    RcDoc::text(" ROWS WITH TIES"),
                ]));
            } else {
                docs.push(nest_title("LIMIT", self.doc_expr(&limit.quantity)));
                docs.extend(offset);
            }
        } else {
            docs.extend(offset);
        }

        RcDoc::intersperse(docs, Doc::line()).group()
    }

    pub(crate) fn doc_cte<'a, T: AstInfo>(&'a self, v: &'a Cte<T>) -> RcDoc<'a, ()> {
        RcDoc::concat([
            RcDoc::text(format!("{} AS", v.alias)),
            RcDoc::line(),
            bracket("(", self.doc_query(&v.query), ")"),
        ])
    }

    pub(crate) fn doc_mutually_recursive<'a, T: AstInfo>(
        &'a self,
        v: &'a CteMutRec<T>,
    ) -> RcDoc<'a, ()> {
        let mut docs = Vec::new();
        if !v.columns.is_empty() {
            docs.push(bracket(
                "(",
                self.comma_separate(Self::doc_display_pass, &v.columns),
                ")",
            ));
        }
        docs.push(bracket("AS (", self.doc_query(&v.query), ")"));
        nest(
            self.doc_display_pass(&v.name),
            RcDoc::intersperse(docs, Doc::line()).group(),
        )
    }

    pub(crate) fn doc_set_expr<'a, T: AstInfo>(&'a self, v: &'a SetExpr<T>) -> RcDoc<'a, ()> {
        match v {
            SetExpr::Select(v) => self.doc_select(v),
            SetExpr::Query(v) => bracket("(", self.doc_query(v), ")"),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                RcDoc::concat([
                    self.doc_set_expr(left),
                    RcDoc::line(),
                    RcDoc::concat([
                        RcDoc::text(format!("{}{}", op, all_str)),
                        RcDoc::line(),
                        self.doc_set_expr(right),
                    ])
                    .nest(TAB)
                    .group(),
                ])
            }
            SetExpr::Values(v) => self.doc_values(v),
            SetExpr::Show(v) => self.doc_display(v, "SHOW"),
            SetExpr::Table(v) => nest(RcDoc::text("TABLE"), self.doc_display_pass(v)),
        }
        .group()
    }

    pub(crate) fn doc_values<'a, T: AstInfo>(&'a self, v: &'a Values<T>) -> RcDoc<'a, ()> {
        let rows =
            v.0.iter()
                .map(|row| bracket("(", self.comma_separate(Self::doc_expr, row), ")"));
        RcDoc::concat([RcDoc::text("VALUES"), RcDoc::line(), comma_separated(rows)])
            .nest(TAB)
            .group()
    }

    pub(crate) fn doc_table_with_joins<'a, T: AstInfo>(
        &'a self,
        v: &'a TableWithJoins<T>,
    ) -> RcDoc<'a, ()> {
        let mut docs = vec![self.doc_table_factor(&v.relation)];
        for j in &v.joins {
            docs.push(self.doc_join(j));
        }
        intersperse_line_nest(docs)
    }

    pub(crate) fn doc_join<'a, T: AstInfo>(&'a self, v: &'a Join<T>) -> RcDoc<'a, ()> {
        let (constraint, name) = match &v.join_operator {
            JoinOperator::Inner(constraint) => (constraint, "JOIN"),
            JoinOperator::FullOuter(constraint) => (constraint, "FULL JOIN"),
            JoinOperator::LeftOuter(constraint) => (constraint, "LEFT JOIN"),
            JoinOperator::RightOuter(constraint) => (constraint, "RIGHT JOIN"),
            _ => return self.doc_display(v, "join operator"),
        };
        let constraint = match constraint {
            JoinConstraint::On(expr) => nest_title("ON", self.doc_expr(expr)),
            JoinConstraint::Using { columns, alias } => {
                let mut doc = bracket(
                    "USING(",
                    self.comma_separate(Self::doc_display_pass, columns),
                    ")",
                );
                if let Some(alias) = alias {
                    doc = nest(doc, nest_title("AS", self.doc_display_pass(alias)));
                }
                doc
            }
            _ => return self.doc_display(v, "join constrant"),
        };
        intersperse_line_nest([
            RcDoc::text(name),
            self.doc_table_factor(&v.relation),
            constraint,
        ])
    }

    pub(crate) fn doc_table_factor<'a, T: AstInfo>(
        &'a self,
        v: &'a TableFactor<T>,
    ) -> RcDoc<'a, ()> {
        match v {
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                let prefix = if *lateral { "LATERAL (" } else { "(" };
                let mut docs = vec![bracket(prefix, self.doc_query(subquery), ")")];
                if let Some(alias) = alias {
                    docs.push(RcDoc::text(format!("AS {}", alias)));
                }
                intersperse_line_nest(docs)
            }
            TableFactor::NestedJoin { join, alias } => {
                let mut doc = bracket("(", self.doc_table_with_joins(join), ")");
                if let Some(alias) = alias {
                    doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
                }
                doc
            }
            TableFactor::Table { name, alias } => {
                let mut doc = self.doc_display_pass(name);
                if let Some(alias) = alias {
                    doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
                }
                doc
            }
            _ => self.doc_display(v, "table factor variant"),
        }
    }

    pub(crate) fn doc_distinct<'a, T: AstInfo>(&'a self, v: &'a Distinct<T>) -> RcDoc<'a, ()> {
        match v {
            Distinct::EntireRow => RcDoc::text("DISTINCT"),
            Distinct::On(cols) => bracket(
                "DISTINCT ON (",
                self.comma_separate(Self::doc_expr, cols),
                ")",
            ),
        }
    }

    pub(crate) fn doc_select<'a, T: AstInfo>(&'a self, v: &'a Select<T>) -> RcDoc<'a, ()> {
        let mut docs = vec![];
        let mut select = RcDoc::text("SELECT");
        if let Some(distinct) = &v.distinct {
            select = nest(select, self.doc_distinct(distinct));
        }
        docs.push(self.nest_comma_separate(select, Self::doc_select_item, &v.projection));
        if !v.from.is_empty() {
            docs.push(self.title_comma_separate("FROM", Self::doc_table_with_joins, &v.from));
        }
        if let Some(selection) = &v.selection {
            docs.push(nest_title("WHERE", self.doc_expr(selection)));
        }
        if !v.group_by.is_empty() {
            docs.push(self.title_comma_separate("GROUP BY", Self::doc_expr, &v.group_by));
        }
        if let Some(having) = &v.having {
            docs.push(nest_title("HAVING", self.doc_expr(having)));
        }
        if !v.options.is_empty() {
            docs.push(bracket(
                "OPTIONS (",
                self.comma_separate(Self::doc_display_pass, &v.options),
                ")",
            ));
        }
        RcDoc::intersperse(docs, Doc::line()).group()
    }

    pub(crate) fn doc_select_item<'a, T: AstInfo>(&'a self, v: &'a SelectItem<T>) -> RcDoc<'a, ()> {
        match v {
            SelectItem::Expr { expr, alias } => {
                let mut doc = self.doc_expr(expr);
                if let Some(alias) = alias {
                    doc = nest(
                        doc,
                        RcDoc::concat([RcDoc::text("AS "), self.doc_display_pass(alias)]),
                    );
                }
                doc
            }
            SelectItem::Wildcard => self.doc_display_pass(v),
        }
    }

    pub(crate) fn doc_expr<'a, T: AstInfo>(&'a self, v: &'a Expr<T>) -> RcDoc<'a, ()> {
        match v {
            Expr::Op { op, expr1, expr2 } => {
                if let Some(expr2) = expr2 {
                    RcDoc::concat([
                        self.doc_expr(expr1),
                        RcDoc::line(),
                        RcDoc::text(format!("{} ", op)),
                        self.doc_expr(expr2).nest(TAB),
                    ])
                } else {
                    RcDoc::concat([RcDoc::text(format!("{} ", op)), self.doc_expr(expr1)])
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut docs = Vec::new();
                if let Some(operand) = operand {
                    docs.push(self.doc_expr(operand));
                }
                for (c, r) in conditions.iter().zip(results) {
                    let when = nest_title("WHEN", self.doc_expr(c));
                    let then = nest_title("THEN", self.doc_expr(r));
                    docs.push(nest(when, then));
                }
                if let Some(else_result) = else_result {
                    docs.push(nest_title("ELSE", self.doc_expr(else_result)));
                }
                let doc = intersperse_line_nest(docs);
                bracket("CASE", doc, "END")
            }
            Expr::Cast { expr, data_type } => bracket(
                "CAST(",
                RcDoc::concat([
                    self.doc_expr(expr),
                    RcDoc::line(),
                    RcDoc::text(format!("AS {}", data_type.to_ast_string())),
                ])
                .nest(TAB),
                ")",
            ),
            Expr::Nested(ast) => bracket("(", self.doc_expr(ast), ")"),
            Expr::Function(fun) => self.doc_function(fun),
            Expr::Subquery(ast) => bracket("(", self.doc_query(ast), ")"),
            Expr::Identifier(_)
            | Expr::Value(_)
            | Expr::QualifiedWildcard(_)
            | Expr::WildcardAccess(_)
            | Expr::FieldAccess { .. } => self.doc_display_pass(v),
            Expr::And { left, right } => bracket_doc(
                self.doc_expr(left),
                RcDoc::text("AND"),
                self.doc_expr(right),
                RcDoc::line(),
            ),
            Expr::Or { left, right } => bracket_doc(
                self.doc_expr(left),
                RcDoc::text("OR"),
                self.doc_expr(right),
                RcDoc::line(),
            ),
            Expr::Exists(s) => bracket("EXISTS (", self.doc_query(s), ")"),
            Expr::IsExpr {
                expr,
                negated,
                construct,
            } => bracket_doc(
                self.doc_expr(expr),
                RcDoc::text(if *negated { "IS NOT" } else { "IS" }),
                self.doc_display_pass(construct),
                RcDoc::line(),
            ),
            Expr::Not { expr } => {
                RcDoc::concat([RcDoc::text("NOT"), RcDoc::line(), self.doc_expr(expr)])
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => RcDoc::intersperse(
                [
                    self.doc_expr(expr),
                    RcDoc::text(if *negated { "NOT BETWEEN" } else { "BETWEEN" }),
                    RcDoc::intersperse(
                        [self.doc_expr(low), RcDoc::text("AND"), self.doc_expr(high)],
                        RcDoc::line(),
                    )
                    .group(),
                ],
                RcDoc::line(),
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => RcDoc::intersperse(
                [
                    self.doc_expr(expr),
                    RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                    self.doc_query(subquery),
                    RcDoc::text(")"),
                ],
                RcDoc::line(),
            ),
            Expr::InList {
                expr,
                list,
                negated,
            } => RcDoc::intersperse(
                [
                    self.doc_expr(expr),
                    RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                    self.comma_separate(Self::doc_expr, list),
                    RcDoc::text(")"),
                ],
                RcDoc::line(),
            ),
            Expr::Row { exprs } => bracket("ROW(", self.comma_separate(Self::doc_expr, exprs), ")"),
            Expr::NullIf { l_expr, r_expr } => bracket(
                "NULLIF (",
                self.comma_separate(Self::doc_expr, [&**l_expr, &**r_expr]),
                ")",
            ),
            Expr::HomogenizingFunction { function, exprs } => bracket(
                format!("{function}("),
                self.comma_separate(Self::doc_expr, exprs),
                ")",
            ),
            Expr::ArraySubquery(s) => bracket("ARRAY(", self.doc_query(s), ")"),
            Expr::ListSubquery(s) => bracket("LIST(", self.doc_query(s), ")"),
            Expr::Array(exprs) => {
                bracket("ARRAY[", self.comma_separate(Self::doc_expr, exprs), "]")
            }
            Expr::List(exprs) => bracket("LIST[", self.comma_separate(Self::doc_expr, exprs), "]"),
            _ => self.doc_display(v, "expr variant"),
        }
        .group()
    }

    pub(crate) fn doc_function<'a, T: AstInfo>(&'a self, v: &'a Function<T>) -> RcDoc<'a, ()> {
        match &v.args {
            FunctionArgs::Star => self.doc_display_pass(v),
            FunctionArgs::Args { args, order_by } => {
                if args.is_empty() {
                    // Nullary, don't allow newline between parens, so just delegate.
                    self.doc_display_pass(v)
                } else {
                    if v.filter.is_some() || v.over.is_some() || !order_by.is_empty() {
                        return self.doc_display(v, "function filter or over or order by");
                    }
                    let name = format!(
                        "{}({}",
                        v.name.to_ast_string(),
                        if v.distinct { "DISTINCT " } else { "" }
                    );
                    bracket(name, self.comma_separate(Self::doc_expr, args), ")")
                }
            }
        }
    }
}
