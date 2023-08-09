// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for  processing client [`Command`]s. Each [`Command`] is initiated by a
//! client via some external Materialize API (ex: HTTP and psql).

use std::collections::BTreeSet;
use std::sync::Arc;

use mz_compute_client::protocol::response::PeekResponse;
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{RoleAttributes, SessionCatalog};
use mz_sql::names::{PartialItemName, ResolvedIds};
use mz_sql::plan::{
    AbortTransactionPlan, CommitTransactionPlan, CopyRowsPlan, CreateRolePlan, Params, Plan,
    TransactionType,
};
use mz_sql::session::vars::{
    EndTransactionAction, OwnedVarInput, Var, VarInput, STATEMENT_LOGGING_SAMPLE_RATE,
};
use tokio::sync::{oneshot, watch};
use tracing::Instrument;

use crate::catalog::{CatalogItem, DataSourceDesc, Source};
use crate::client::{ConnectionId, ConnectionIdType};
use crate::command::{
    AppendWebhookResponse, AppendWebhookValidator, Canceled, CatalogSnapshot, Command,
    ExecuteResponse, GetVariablesResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::peek::PendingPeek;
use crate::coord::{ConnMeta, Coordinator, PendingTxn};
use crate::error::AdapterError;
use crate::session::SessionMetadata;
use crate::util::{ClientTransmitter, ResultExt};
use crate::{catalog, metrics, rbac, ExecuteContext};

impl Coordinator {
    fn send_error(&mut self, cmd: Command, e: AdapterError) {
        fn send<T>(tx: oneshot::Sender<Response<T>>, e: AdapterError) {
            let _ = tx.send(Response::<T> { result: Err(e) });
        }
        match cmd {
            Command::CatalogSnapshot { tx, .. } => send(tx, e),
            Command::AllocateUserId { tx, .. } => send(tx, e),
            Command::Startup { tx, .. } => send(tx, e),
            Command::Sequence { tx, .. } => send(tx, e),
            Command::Commit { tx, .. } => send(tx, e),
            Command::CancelRequest { .. } | Command::PrivilegedCancelRequest { .. } => {}
            Command::CopyRows { tx, .. } => send(tx, e),
            Command::GetSystemVars { tx, .. } => send(tx, e),
            Command::SetSystemVars { tx, .. } => send(tx, e),
            Command::AppendWebhook { tx, .. } => {
                // We don't care if our listener went away.
                let _ = tx.send(Err(e));
            }
            Command::Terminate { tx, .. } => {
                if let Some(tx) = tx {
                    send(tx, e)
                }
            }
            Command::RetireExecute { .. } => panic!("Command::RetireExecute is infallible"),
        }
    }

    pub(crate) async fn handle_command(&mut self, mut cmd: Command) {
        /*
        TODO(adapter)
        if let Some(session) = cmd.session_mut() {
            session.apply_external_metadata_updates();
        }
        */
        if let Err(e) = rbac::check_command(self.catalog(), &cmd) {
            self.send_error(cmd, e.into());
            return;
        }
        match cmd {
            Command::Startup {
                session,
                cancel_tx,
                tx,
                set_setting_keys,
            } => {
                // Note: We purposefully do not use a ClientTransmitter here because startup
                // handles errors and cleanup of sessions itself.
                self.handle_startup(session, cancel_tx, tx, set_setting_keys)
                    .await;
            }

            Command::RetireExecute { data, reason } => self.retire_execution(reason, data),

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key);
            }

            Command::PrivilegedCancelRequest { session } => {
                self.handle_privileged_cancel(session.conn_id().clone());
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                session,
                tx,
                ctx_extra,
            } => {
                let ctx = ExecuteContext::from_parts(
                    ClientTransmitter::new(tx, self.internal_cmd_tx.clone()),
                    self.internal_cmd_tx.clone(),
                    session,
                    ctx_extra,
                    Vec::new(),
                );
                self.sequence_plan(
                    ctx,
                    Plan::CopyRows(CopyRowsPlan { id, columns, rows }),
                    ResolvedIds(BTreeSet::new()),
                )
                .await;
            }

            Command::AppendWebhook {
                database,
                schema,
                name,
                session,
                tx,
            } => {
                self.handle_append_webhook(database, schema, name, session, tx);
            }

            Command::GetSystemVars { session, tx } => {
                let vars =
                    GetVariablesResponse::new(self.catalog.system_config().iter().filter(|var| {
                        var.visible(session.user(), Some(self.catalog.system_config()))
                            .is_ok()
                    }));
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                tx.send(Ok(vars));
            }

            Command::SetSystemVars { vars, session, tx } => {
                let mut ops = Vec::with_capacity(vars.len());
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                for (name, value) in vars {
                    if let Err(e) = self.catalog().system_config().get(&name).and_then(|var| {
                        var.visible(session.user(), Some(self.catalog.system_config()))
                    }) {
                        return tx.send(Err(e.into()));
                    }

                    ops.push(catalog::Op::UpdateSystemConfiguration {
                        name,
                        value: OwnedVarInput::Flat(value),
                    });
                }

                let result = self.catalog_transact(Some(&session), ops).await;
                tx.send(result);
            }

            Command::Terminate { mut session, tx } => {
                // TODO(adapter)
                /*
                self.handle_terminate(conn_id).await;
                // Note: We purposefully do not use a ClientTransmitter here because we're already
                // terminating the provided session.
                if let Some(tx) = tx {
                    let _ = tx.send(Response { result: Ok(()) });
                }
                */
            }

            Command::Sequence {
                plan,
                resolved_ids,
                session,
                tx,
                span,
                outer_ctx_extra,
            } => {
                self.sequence_plan(ctx, plan, resolved_ids)
                    .instrument(span)
                    .await;
            }

            Command::Commit {
                action,
                session,
                tx,
                otel_ctx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                // We reach here not through a statement execution, but from the
                // "commit" pgwire command. Thus, we just generate a default statement
                // execution context (once statement logging is implemented, this will cause nothing to be logged
                // when the execution finishes.)
                let ctx = ExecuteContext::from_parts(
                    tx,
                    self.internal_cmd_tx.clone(),
                    session,
                    Default::default(),
                    Vec::new(),
                );
                let plan = match action {
                    EndTransactionAction::Commit => {
                        Plan::CommitTransaction(CommitTransactionPlan {
                            transaction_type: TransactionType::Implicit,
                        })
                    }
                    EndTransactionAction::Rollback => {
                        Plan::AbortTransaction(AbortTransactionPlan {
                            transaction_type: TransactionType::Implicit,
                        })
                    }
                };
                // TODO: We need a Span that is not none for the otel_ctx to
                // attach the parent relationship to. If we do the TODO to swap
                // otel_ctx in `Command::Commit` for a Span, we can downgrade
                // this to a debug_span.
                let span = tracing::info_span!("message_command (commit)");
                span.in_scope(|| otel_ctx.attach_as_parent());
                self.sequence_plan(ctx, plan, ResolvedIds(BTreeSet::new()))
                    .instrument(span)
                    .await;
            }

            Command::CatalogSnapshot { if_more_recent, tx } => {
                let current_transient_revision = self.catalog().transient_revision();
                let need_catalog =
                    if_more_recent.is_none() || if_more_recent < Some(current_transient_revision);
                let catalog = need_catalog.then(|| self.owned_catalog());
                let _ = tx.send(Response {
                    result: Ok(CatalogSnapshot {
                        catalog,
                        current_transient_revision,
                    }),
                });
            }

            Command::AllocateUserId { tx } => {
                let result = self.catalog().allocate_user_id().await;
                let _ = tx.send(Response {
                    result: mz_ore::result::ResultExt::err_into(result),
                });
            }
        }
    }

    async fn handle_startup(
        &mut self,
        mut session: SessionMetadata,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
        set_setting_keys: Vec<String>,
    ) {
        if self
            .catalog()
            .try_get_role_by_name(&session.user().name)
            .is_none()
        {
            // If the user has made it to this point, that means they have been fully authenticated.
            // This includes preventing any user, except a pre-defined set of system users, from
            // connecting to an internal port. Therefore it's ok to always create a new role for
            // the user.
            let attributes = RoleAttributes::new();
            let plan = CreateRolePlan {
                name: session.user().name.to_string(),
                attributes,
            };
            if let Err(err) = self.sequence_create_role_for_startup(&session, plan).await {
                let _ = tx.send(Response { result: Err(err) });
                return;
            }
        }

        let role_id = self
            .catalog()
            .try_get_role_by_name(&session.user().name)
            .expect("created above")
            .id;
        session.initialize_role_metadata(role_id);

        if let Err(e) = self
            .catalog_mut()
            .create_temporary_schema(session.conn_id(), role_id)
        {
            let _ = tx.send(Response {
                result: Err(e.into()),
            });
            return;
        }

        let mut messages = vec![];
        let catalog = self.catalog();
        let catalog = catalog.for_session(&session);
        if catalog.active_database().is_none() {
            messages.push(StartupMessage::UnknownSessionDatabase(
                session.vars().database().into(),
            ));
        }
        if !set_setting_keys
            .iter()
            .any(|k| k == STATEMENT_LOGGING_SAMPLE_RATE.name())
        {
            let default = catalog
                .state()
                .system_config()
                .statement_logging_default_sample_rate();
            session
                .vars_mut()
                .set(
                    None,
                    STATEMENT_LOGGING_SAMPLE_RATE.name(),
                    VarInput::Flat(&default.to_string()),
                    false,
                )
                .expect("constrained to be valid");
            session
                .vars_mut()
                .end_transaction(EndTransactionAction::Commit);
        }

        let session_type = metrics::session_type_label_value(session.user());
        self.metrics
            .active_sessions
            .with_label_values(&[session_type])
            .inc();
        self.active_conns.insert(
            session.conn_id().clone(),
            ConnMeta {
                cancel_tx,
                secret_key: session.secret_key(),
                notice_tx: session.retain_notice_transmitter(),
                drop_sinks: Vec::new(),
                // TODO: Switch to authenticated role once implemented.
                authenticated_role: session.session_role_id().clone(),
            },
        );
        let update = self
            .catalog()
            .state()
            .pack_session_update(&session.metadata(), 1);
        self.begin_session_for_statement_logging(&session);
        self.send_builtin_table_updates(vec![update]).await;

        ClientTransmitter::new(tx, self.internal_cmd_tx.clone())
            .send(Ok(StartupResponse { messages }))
    }

    #[tracing::instrument(level = "trace", skip(self, ctx))]
    pub(crate) async fn handle_execute_inner(
        &mut self,
        stmt: Statement<Raw>,
        params: Params,
        mut ctx: ExecuteContext,
    ) {
        todo!()
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id` if the correct secret key is specified.
    ///
    /// Note: Here we take a [`ConnectionIdType`] as opposed to an owned
    /// `ConnectionId` because this method gets called by external clients when
    /// they request to cancel a request.
    fn handle_cancel(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
        if let Some((id_handle, conn_meta)) = self.active_conns.get_key_value(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Now that we've verified the secret key, this is a privileged
            // cancellation request. We can upgrade the raw connection ID to a
            // proper `IdHandle`.
            self.handle_privileged_cancel(id_handle.clone())
        }
    }

    /// Unconditionally instructs the dataflow layer to cancel any ongoing,
    /// interactive work for the named `conn_id`.
    pub(crate) fn handle_privileged_cancel(&mut self, conn_id: ConnectionId) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // Cancel pending writes. There is at most one pending write per session.
            let mut maybe_ctx = None;
            if let Some(idx) = self.pending_writes.iter().position(|pending_write_txn| {
                matches!(pending_write_txn, PendingWriteTxn::User {
                    pending_txn: PendingTxn { ctx, .. },
                    ..
                } if *ctx.session().conn_id() == conn_id)
            }) {
                if let PendingWriteTxn::User {
                    pending_txn: PendingTxn { ctx, .. },
                    ..
                } = self.pending_writes.remove(idx)
                {
                    maybe_ctx = Some(ctx);
                }
            }

            // Cancel deferred writes. There is at most one deferred write per session.
            if let Some(idx) = self.write_lock_wait_group.iter().position(
                |ready| matches!(ready, Deferred::Plan(ready) if *ready.ctx.session().conn_id() == conn_id),
            ) {
                let ready = self
                    .write_lock_wait_group
                    .remove(idx)
                    .expect("known to exist from call to `position` above");
                if let Deferred::Plan(ready) = ready {
                    maybe_ctx = Some(ready.ctx);
                }
            }

            // Cancel commands waiting on a real time recency timestamp. There is at most one  per session.
            if let Some(real_time_recency_context) =
                self.pending_real_time_recency_timestamp.remove(&conn_id)
            {
                let ctx = real_time_recency_context.take_context();
                maybe_ctx = Some(ctx);
            }

            if let Some(ctx) = maybe_ctx {
                ctx.retire(Ok(ExecuteResponse::Canceled));
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Canceled::Canceled);

            for PendingPeek {
                sender: rows_tx,
                conn_id: _,
                cluster_id: _,
                depends_on: _,
                // We take responsibility for retiring the
                // peek in `self.cancel_pending_peeks`,
                // so we don't need to do anything with `ctx_extra` here.
                ctx_extra: _,
                is_fast_path: _,
            } in self.cancel_pending_peeks(&conn_id)
            {
                // Cancel messages can be sent after the connection has hung
                // up, but before the connection's state has been cleaned up.
                // So we ignore errors when sending the response.
                let _ = rows_tx.send(PeekResponse::Canceled);
            }
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut SessionMetadata) {
        if self.active_conns.get(session.conn_id()).is_none() {
            // If the session doesn't exist in `active_conns`, then this method will panic later on.
            // Instead we explicitly panic here while dumping the entire Coord to the logs to help
            // debug. This panic is very infrequent so we want as much information as possible.
            // See https://github.com/MaterializeInc/materialize/issues/18996.
            panic!("unknown session: {session:?}\n\n{self:?}")
        }

        self.clear_transaction(session);

        self.drop_temp_items(session).await;
        self.catalog_mut()
            .drop_temporary_schema(session.conn_id())
            .unwrap_or_terminate("unable to drop temporary schema");
        let session_type = metrics::session_type_label_value(session.user());
        self.metrics
            .active_sessions
            .with_label_values(&[session_type])
            .dec();
        self.active_conns.remove(session.conn_id());
        self.cancel_pending_peeks(session.conn_id());
        self.end_session_for_statement_logging(session.uuid());
        let update = self.catalog().state().pack_session_update(session, -1);
        self.send_builtin_table_updates(vec![update]).await;
    }

    fn handle_append_webhook(
        &mut self,
        database: String,
        schema: String,
        name: String,
        session: SessionMetadata,
        tx: oneshot::Sender<Result<AppendWebhookResponse, AdapterError>>,
    ) {
        // Make sure the feature is enabled before doing anything else.
        if !self.catalog().system_config().enable_webhook_sources() {
            // We don't care if the listener went away.
            let _ = tx.send(Err(AdapterError::Unsupported("enable_webhook_sources")));
            return;
        }

        /// Attempts to resolve a Webhook source from a provided `database.schema.name` path.
        ///
        /// Returns a struct that can be used to append data to the underlying storate collection, and the
        /// types we should cast the request to.
        fn resolve(
            coord: &Coordinator,
            database: String,
            schema: String,
            name: String,
            conn_id: &ConnectionId,
        ) -> Result<AppendWebhookResponse, PartialItemName> {
            // Resolve our collection.
            let name = PartialItemName {
                database: Some(database),
                schema: Some(schema),
                item: name,
            };
            let Ok(entry) = coord.catalog().resolve_entry(None, &vec![], &name, conn_id) else {
                return Err(name);
            };

            let (body_ty, header_ty, validator) = match entry.item() {
                CatalogItem::Source(Source {
                    data_source: DataSourceDesc::Webhook { validation, .. },
                    desc,
                    ..
                }) => {
                    // All Webhook sources should have at most 2 columns.
                    mz_ore::soft_assert!(desc.arity() <= 2);

                    let body = desc
                        .get_by_name(&"body".into())
                        .map(|(_idx, ty)| ty.clone())
                        .ok_or(name.clone())?;
                    let header = desc
                        .get_by_name(&"headers".into())
                        .map(|(_idx, ty)| ty.clone());

                    // Create a validator that can be called to validate a webhook request.
                    let validator = validation.as_ref().map(|v| {
                        let validation = v.clone();
                        AppendWebhookValidator::new(
                            validation,
                            coord.caching_secrets_reader.clone(),
                        )
                    });
                    (body, header, validator)
                }
                _ => return Err(name),
            };

            // Get a channel so we can queue updates to be written.
            let row_tx = coord
                .controller
                .storage
                .monotonic_appender(entry.id())
                .map_err(|_| name)?;
            Ok(AppendWebhookResponse {
                tx: row_tx,
                body_ty,
                header_ty,
                validator,
            })
        }

        let response = resolve(self, database, schema, name, session.conn_id()).map_err(|name| {
            AdapterError::UnknownWebhookSource {
                database: name.database.expect("provided"),
                schema: name.schema.expect("provided"),
                name: name.item,
            }
        });
        let _ = tx.send(response);
    }
}
