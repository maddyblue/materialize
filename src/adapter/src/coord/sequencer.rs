// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Prevents anyone from accidentally exporting a method from the `inner` module.
#![allow(clippy::pub_use)]

//! Logic for executing a planned SQL query.

use inner::return_if_err;
use mz_controller::clusters::ClusterId;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::ExplainFormat;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CreateRolePlan, CreateSourcePlans, Params, Plan,
    RotateKeysPlan,
};
use mz_sql_parser::ast::{Raw, Statement};
use tokio::sync::oneshot;

use crate::command::{Command, ExecuteResponse, Response};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::{Coordinator, Message};
use crate::error::AdapterError;
use crate::notice::AdapterNotice;
use crate::session::{EndTransactionAction, Session, TransactionStatus};
use crate::util::ClientTransmitter;
use crate::util::{send_immediate_rows, ClientTransmitter};
use crate::{rbac, ExecuteContext};
use crate::{rbac, ExecuteContext, ExecuteResponseKind};

// DO NOT make this visible in any way, i.e. do not add any version of
// `pub` to this mod. The inner `sequence_X` methods are hidden in this
// private module to prevent anyone from calling them directly. All
// sequencing should be done through the `sequence_plan` method.
// This allows us to add catch-all logic that should be applied to all
// plans in `sequence_plan` and guarantee that no caller can circumvent
// that logic.
//
// The two exceptions are:
//
// - Creating a role during connection startup. In this scenario, the session has not been properly
// initialized and we need to skip directly to creating role. We have a specific method,
// `sequence_create_role_for_startup` for this purpose.
// - Methods that continue the execution of some plan that was being run asynchronously, such as
// `sequence_peek_stage` and `sequence_create_connection_stage_finish`.
mod alter_set_cluster;
mod cluster;
mod inner;
mod linked_cluster;

impl Coordinator {
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_plan(
        &mut self,
        mut ctx: ExecuteContext,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) {
        // TODO(adapter): (is this already done elsewhere?)
        /*
        event!(Level::TRACE, plan = format!("{:?}", plan));
        let mut responses = ExecuteResponse::generated_from(PlanKind::from(&plan));
        responses.push(ExecuteResponseKind::Canceled);
        ctx.tx_mut().set_allowed(responses);

        let session_catalog = self.catalog.for_session(ctx.session());

        if let Err(e) = introspection::user_privilege_hack(
            &session_catalog,
            ctx.session(),
            &plan,
            &resolved_ids,
        ) {
            return ctx.retire(Err(e));
        }
        if let Err(e) = introspection::check_cluster_restrictions(&session_catalog, &plan) {
            return ctx.retire(Err(e));
        }

        // If our query only depends on system tables, a LaunchDarkly flag is enabled, and a
        // session var is set, then we automatically run the query on the mz_introspection cluster.
        let target_cluster =
            introspection::auto_run_on_introspection(&self.catalog, ctx.session(), &plan);
        let target_cluster_id = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())
            .ok()
            .map(|cluster| cluster.id());

        if let Err(e) = rbac::check_plan(
            self,
            &session_catalog,
            ctx.session(),
            &plan,
            target_cluster_id,
            &resolved_ids,
        ) {
            return ctx.retire(Err(e));
        }
        */

        match plan {
            Plan::CreateSource(plan) => {
                let source_id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);
                let result = self
                    .sequence_create_source(
                        &mut ctx,
                        vec![CreateSourcePlans {
                            source_id,
                            plan,
                            resolved_ids,
                        }],
                    )
                    .await;
                ctx.retire(result);
            }
            Plan::CreateSources(plans) => {
                assert!(
                    resolved_ids.0.is_empty(),
                    "each plan has separate resolved_ids"
                );
                let result = self.sequence_create_source(&mut ctx, plans).await;
                ctx.retire(result);
            }
            Plan::CreateConnection(plan) => {
                self.sequence_create_connection(ctx, plan, resolved_ids)
                    .await;
            }
            Plan::CreateDatabase(plan) => {
                let result = self.sequence_create_database(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::CreateSchema(plan) => {
                let result = self.sequence_create_schema(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::CreateRole(plan) => {
                let result = self.sequence_create_role(ctx.session(), plan).await;
                if result.is_ok() {
                    self.maybe_send_rbac_notice(&mut ctx);
                }
                ctx.retire(result);
            }
            Plan::CreateCluster(plan) => {
                let result = self.sequence_create_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::CreateClusterReplica(plan) => {
                let result = self
                    .sequence_create_cluster_replica(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateTable(plan) => {
                let result = self
                    .sequence_create_table(&mut ctx, plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateSecret(plan) => {
                let result = self.sequence_create_secret(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(ctx, plan, resolved_ids).await;
            }
            Plan::CreateView(plan) => {
                let result = self
                    .sequence_create_view(&mut ctx, plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateMaterializedView(plan) => {
                let result = self
                    .sequence_create_materialized_view(&mut ctx, plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateIndex(plan) => {
                let result = self
                    .sequence_create_index(&mut ctx, plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::CreateType(plan) => {
                let result = self
                    .sequence_create_type(ctx.session(), plan, resolved_ids)
                    .await;
                ctx.retire(result);
            }
            Plan::DropObjects(plan) => {
                let result = self.sequence_drop_objects(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::DropOwned(plan) => {
                let result = self.sequence_drop_owned(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::EmptyQuery => {
                ctx.retire(Ok(ExecuteResponse::EmptyQuery));
            }
            Plan::ShowAllVariables => unreachable!(),
            Plan::ShowVariable(_) => unreachable!(),
            Plan::InspectShard(plan) => {
                // TODO: Ideally, this await would happen off the main thread.
                let result = self.sequence_inspect_shard(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::SetVariable(_) => unreachable!(),
            Plan::ResetVariable(_) => unreachable!(),
            Plan::SetTransaction(_) => unreachable!(),
            Plan::StartTransaction(_) => unreachable!(),
            Plan::CommitTransaction(_) => unreachable!(),
            Plan::AbortTransaction(_) => unreachable!(),
            Plan::Select(plan) => {
                self.sequence_peek(ctx, plan, target_cluster).await;
            }
            Plan::Subscribe(plan) => {
                let result = self
                    .sequence_subscribe(&mut ctx, plan, target_cluster)
                    .await;
                ctx.retire(result);
            }
            Plan::SideEffectingFunc(plan) => {
                ctx.retire(self.sequence_side_effecting_func(plan));
            }
            Plan::ShowCreate(plan) => {
                ctx.retire(Ok(Self::send_immediate_rows(vec![plan.row])));
            }
            Plan::CopyFrom(_) => unreachable!(),
            Plan::CopyRows(_) => unreachable!(),
            Plan::Explain(plan) => {
                self.sequence_explain(ctx, plan, target_cluster).await;
            }
            Plan::Insert(_) => unreachable!(),
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(ctx, plan).await;
            }
            Plan::AlterNoop(plan) => {
                ctx.retire(Ok(ExecuteResponse::AlteredObject(plan.object_type)));
            }
            Plan::AlterCluster(plan) => {
                let result = self.sequence_alter_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterClusterRename(plan) => {
                let result = self
                    .sequence_alter_cluster_rename(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterClusterReplicaRename(plan) => {
                let result = self
                    .sequence_alter_cluster_replica_rename(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterSetCluster(plan) => {
                let result = self.sequence_alter_set_cluster(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterItemRename(plan) => {
                let result = self.sequence_alter_item_rename(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterIndexSetOptions(plan) => {
                let result = self.sequence_alter_index_set_options(plan);
                ctx.retire(result);
            }
            Plan::AlterIndexResetOptions(plan) => {
                let result = self.sequence_alter_index_reset_options(plan);
                ctx.retire(result);
            }
            Plan::AlterRole(plan) => {
                let result = self.sequence_alter_role(ctx.session(), plan).await;
                if result.is_ok() {
                    self.maybe_send_rbac_notice(&mut ctx);
                }
                ctx.retire(result);
            }
            Plan::AlterSecret(plan) => {
                let result = self.sequence_alter_secret(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSink(plan) => {
                let result = self.sequence_alter_sink(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            } => {
                let result = self
                    .sequence_alter_source(&mut ctx, alter_source, subsources)
                    .await;
                ctx.retire(result);
            }
            Plan::AlterSource(_) => {
                unreachable!("ALTER SOURCE must be purified")
            }
            Plan::AlterSystemSet(plan) => {
                let result = self.sequence_alter_system_set(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSystemReset(plan) => {
                let result = self.sequence_alter_system_reset(ctx.session(), plan).await;
                ctx.retire(result);
            }
            Plan::AlterSystemResetAll(plan) => {
                let result = self
                    .sequence_alter_system_reset_all(ctx.session(), plan)
                    .await;
                ctx.retire(result);
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(ctx.session()).await;
                ctx.retire(Ok(ExecuteResponse::DiscardedTemp));
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = ctx.session().transaction() {
                    // TODO(adapter): merge this with clear_transaction.
                    self.drop_temp_items(ctx.session()).await;
                    let conn_meta = self
                        .active_conns
                        .get_mut(ctx.session().conn_id())
                        .expect("must exist for active session");
                    let drop_sinks = std::mem::take(&mut conn_meta.drop_sinks);
                    self.drop_compute_sinks(drop_sinks);
                    &mut ctx.reset();
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(AdapterError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                ctx.retire(ret);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                self.declare(ctx, plan.name, plan.stmt, plan.sql, param_types);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                let ctx_extra = std::mem::take(ctx.extra_mut());
                ctx.retire(Ok(ExecuteResponse::Fetch {
                    name,
                    count,
                    timeout,
                    ctx_extra,
                }));
            }
            Plan::Close(plan) => {
                if ctx.session_mut().remove_portal(&plan.name) {
                    ctx.retire(Ok(ExecuteResponse::ClosedCursor));
                } else {
                    ctx.retire(Err(AdapterError::UnknownCursor(plan.name)));
                }
            }
            Plan::Prepare(plan) => {
                if ctx
                    .session()
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    ctx.retire(Err(AdapterError::PreparedStatementExists(plan.name)));
                } else {
                    ctx.session_mut().set_prepared_statement(
                        plan.name,
                        Some(plan.stmt),
                        plan.sql,
                        plan.desc,
                        self.catalog().transient_revision(),
                        self.now(),
                    );
                    ctx.retire(Ok(ExecuteResponse::Prepare));
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(ctx.session_mut(), plan) {
                    Ok(portal_name) => {
                        let (tx, _, session, extra) = ctx.into_parts();
                        self.internal_cmd_tx
                            .send(Message::Command(Command::Execute {
                                portal_name,
                                session,
                                tx: tx.take(),
                                outer_ctx_extra: Some(extra),
                                span: tracing::Span::none(),
                            }))
                            .expect("sending to self.internal_cmd_tx cannot fail");
                    }
                    Err(err) => ctx.retire(Err(err)),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if ctx.session_mut().remove_prepared_statement(&name) {
                        ctx.retire(Ok(ExecuteResponse::Deallocate { all: false }));
                    } else {
                        ctx.retire(Err(AdapterError::UnknownPreparedStatement(name)));
                    }
                }
                None => {
                    ctx.session_mut().remove_all_prepared_statements();
                    ctx.retire(Ok(ExecuteResponse::Deallocate { all: true }));
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                ctx.session()
                    .add_notice(AdapterNotice::UserRequested { severity });
                ctx.retire(Ok(ExecuteResponse::Raised));
            }
            Plan::Declare(_) => unreachable!(),
            Plan::Fetch(_) => unreachable!(),
            Plan::Close(_) => unreachable!(),
            Plan::Prepare(_) => unreachable!(),
            Plan::Execute(_) => unreachable!(),
            Plan::Deallocate(_) => unreachable!(),
            Plan::Raise(_) => unreachable!(),
            Plan::RotateKeys(RotateKeysPlan { id }) => {
                let result = self.sequence_rotate_keys(ctx.session(), id).await;
                ctx.retire(result);
            }
            Plan::GrantPrivileges(plan) => {
                let result = self.sequence_grant_privileges(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::RevokePrivileges(plan) => {
                let result = self.sequence_revoke_privileges(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::AlterDefaultPrivileges(plan) => {
                let result = self.sequence_alter_default_privileges(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::GrantRole(plan) => {
                let result = self.sequence_grant_role(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::RevokeRole(plan) => {
                let result = self.sequence_revoke_role(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::AlterOwner(plan) => {
                let result = self.sequence_alter_owner(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::ReassignOwned(plan) => {
                let result = self.sequence_reassign_owned(&mut ctx, plan).await;
                ctx.retire(result);
            }
            Plan::ValidateConnection(plan) => {
                let connection_context = self.connection_context.clone();
                mz_ore::task::spawn(|| "coord::validate_connection", async move {
                    let res = match plan.connection.validate(plan.id, &connection_context).await {
                        Ok(()) => Ok(ExecuteResponse::ValidatedConnection),
                        Err(err) => Err(err.into()),
                    };
                    ctx.retire(res);
                });
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_execute_single_statement_transaction(
        &mut self,
        ctx: ExecuteContext,
        stmt: Statement<Raw>,
        params: Params,
    ) {
        // Put the session into single statement implicit so anything can execute.
        let (tx, internal_cmd_tx, mut session, extra) = ctx.into_parts();
        assert!(matches!(session.transaction(), TransactionStatus::Default));
        session.start_transaction_single_stmt(self.now_datetime());
        let conn_id = session.conn_id().unhandled();

        // Execute the saved statement in a temp transmitter so we can run COMMIT.
        let (sub_tx, sub_rx) = oneshot::channel();
        let sub_ct = ClientTransmitter::new(sub_tx, self.internal_cmd_tx.clone());
        let sub_ctx = ExecuteContext::from_parts(sub_ct, internal_cmd_tx, session, extra);
        self.handle_execute_inner(stmt, params, sub_ctx).await;

        // The response can need off-thread processing. Wait for it elsewhere so the coordinator can
        // continue processing.
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        mz_ore::task::spawn(
            || format!("execute_single_statement:{conn_id}"),
            async move {
                let Ok(Response { result, session }) = sub_rx.await else {
                    // Coordinator went away.
                    return;
                };
                let (sub_tx, sub_rx) = oneshot::channel();
                let _ = internal_cmd_tx.send(Message::Command(Command::Commit {
                    action: EndTransactionAction::Commit,
                    session,
                    tx: sub_tx,
                    otel_ctx: OpenTelemetryContext::obtain(),
                }));
                let Ok(commit_response) = sub_rx.await else {
                    // Coordinator went away.
                    return;
                };
                assert!(matches!(
                    commit_response.session.transaction(),
                    TransactionStatus::Default
                ));
                // The fake, generated response was already sent to the user and we don't need to
                // ever send an `Ok(result)` to the user, because they are expecting a response from
                // a `COMMIT`. So, always send the `COMMIT`'s result if the original statement
                // succeeded. If it failed, we can send an error and don't need to wrap it or send a
                // later COMMIT or ROLLBACK.
                let result = match (result, commit_response.result) {
                    (Ok(_), commit) => commit,
                    (Err(result), _) => Err(result),
                };
                // We ignore the resp.result because it's not clear what to do if it failed since we
                // can only send a single ExecuteResponse to tx.
                tx.send(result);
            },
        );
    }

    /// Creates a role during connection startup.
    ///
    /// This should not be called from anywhere except connection startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_role_for_startup(
        &mut self,
        session: &SessionMetadata,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_create_role(session, plan).await
    }

    pub(crate) fn sequence_explain_timestamp_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.sequence_explain_timestamp_finish_inner(
            ctx.session_mut(),
            format,
            cluster_id,
            optimized_plan,
            id_bundle,
            real_time_recency_ts,
        )
    }

    pub(crate) fn allocate_transient_id(&mut self) -> Result<GlobalId, AdapterError> {
        let id = self.transient_id_counter;
        if id == u64::MAX {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    fn maybe_send_rbac_notice(&self, ctx: &mut ExecuteContext) {
        if !rbac::is_rbac_enabled_for_session(self.catalog.system_config(), ctx.session()) {
            if !self.catalog.system_config().enable_ld_rbac_checks() {
                ctx.add_notice(AdapterNotice::RbacSystemDisabled);
            } else {
                ctx.add_notice(AdapterNotice::RbacUserDisabled);
            }
        }
    }
}
