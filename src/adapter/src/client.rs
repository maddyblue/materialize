// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::{self};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use futures::{Stream, StreamExt};
use mz_build_info::BuildInfo;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::{IdAllocator, IdHandle};
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::result::ResultExt;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_ore::thread::JoinOnDropHandle;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::statement_logging::StatementEndedExecutionReason;
use mz_repr::{Datum, Diff, GlobalId, Row, ScalarType};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{CatalogCluster, CatalogError, CatalogSchema, EnvironmentId};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{
    self, AbortTransactionPlan, CommitTransactionPlan, CopyRowsPlan, CreateSourcePlans,
    DeclarePlan, FetchPlan, MutationKind, Plan, PlanKind, RaisePlan,
};
use mz_sql::session::hint::ApplicationNameHint;
use mz_sql::session::user::{User, SUPPORT_USER};
use mz_sql::session::vars::{
    IsolationLevel, VarInput, CLUSTER_VAR_NAME, DATABASE_VAR_NAME, SCHEMA_ALIAS,
    TRANSACTION_ISOLATION_VAR_NAME,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{InsertSource, Query, SetExpr, TransactionMode};
use mz_sql_parser::parser::{ParserStatementError, StatementParseResult};
use mz_storage_client::types::connections::ConnectionContext;
use mz_transform::Optimizer;
use opentelemetry::trace::TraceContextExt;
use prometheus::Histogram;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::{
    AppendWebhookResponse, Canceled, CatalogDump, Command, ExecuteResponse, GetVariablesResponse,
    Response, StartupResponse,
};
use crate::coord::{introspection, Coordinator, ExecuteContextExtra};
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, SessionMetadata, TransactionId,
    TransactionOps, TransactionStatus, WriteOp,
};
use crate::telemetry::{self, SegmentClientExt, StatementFailureType};
use crate::util::{viewable_variables, SessionResponse};
use crate::{rbac, AdapterNotice, ExecuteResponseKind, PeekResponseUnary};

/// Inner type of a [`ConnectionId`], `u32` for postgres compatibility.
///
/// Note: Generally you should not use this type directly, and instead use [`ConnectionId`].
pub type ConnectionIdType = u32;

/// An abstraction allowing us to name different connections.
pub type ConnectionId = IdHandle<ConnectionIdType>;

/// A handle to a running coordinator.
///
/// The coordinator runs on its own thread. Dropping the handle will wait for
/// the coordinator's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the coordinator have dropped.
pub struct Handle {
    pub(crate) session_id: Uuid,
    pub(crate) start_instant: Instant,
    pub(crate) _thread: JoinOnDropHandle<()>,
}

impl Handle {
    /// Returns the session ID associated with this coordinator.
    ///
    /// The session ID is generated on coordinator boot. It lasts for the
    /// lifetime of the coordinator. Restarting the coordinator will result
    /// in a new session ID.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the instant at which the coordinator booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}

/// A coordinator client.
///
/// A coordinator client is a simple handle to a communication channel with the
/// coordinator. It can be cheaply cloned.
///
/// Clients keep the coordinator alive. The coordinator will not exit until all
/// outstanding clients have dropped.
#[derive(Debug, Clone)]
pub struct Client {
    build_info: &'static BuildInfo,
    inner_cmd_tx: mpsc::UnboundedSender<Command>,
    id_alloc: IdAllocator<ConnectionIdType>,
    now: NowFn,
    metrics: Metrics,
    environment_id: EnvironmentId,
    segment_client: Option<mz_segment::Client>,
    connection_context: Arc<ConnectionContext>,
}

impl Client {
    pub(crate) fn new(
        build_info: &'static BuildInfo,
        cmd_tx: mpsc::UnboundedSender<Command>,
        metrics: Metrics,
        now: NowFn,
        environment_id: EnvironmentId,
        segment_client: Option<mz_segment::Client>,
        connection_context: Arc<ConnectionContext>,
    ) -> Client {
        Client {
            build_info,
            inner_cmd_tx: cmd_tx,
            id_alloc: IdAllocator::new(1, 1 << 16),
            now,
            metrics,
            environment_id,
            segment_client,
            connection_context,
        }
    }

    /// Allocates a client for an incoming connection.
    pub fn new_conn_id(&self) -> Result<ConnectionId, AdapterError> {
        self.id_alloc.alloc().ok_or(AdapterError::IdExhaustionError)
    }

    /// Creates a new session associated with this client for the given user.
    ///
    /// It is the caller's responsibility to have authenticated the user.
    pub fn new_session(&self, conn_id: ConnectionId, user: User) -> Session {
        // We use the system clock to determine when a session connected to Materialize. This is not
        // intended to be 100% accurate and correct, so we don't burden the timestamp oracle with
        // generating a more correct timestamp.
        Session::new(self.build_info, conn_id, user, (self.now)())
    }

    /// Upgrades this client to a session client.
    ///
    /// A session is a connection that has successfully negotiated parameters,
    /// like the user. Most coordinator operations are available only after
    /// upgrading a connection to a session.
    ///
    /// Returns a new client that is bound to the session and a response
    /// containing various details about the startup.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn startup(
        &self,
        session: Session,
        // keys of settings that were set on statup, and thus should not be
        // overridden by defaults.
        set_setting_keys: Vec<String>,
    ) -> Result<(SessionClient, StartupResponse), AdapterError> {
        // Cancellation works by creating a watch channel (which remembers only
        // the last value sent to it) and sharing it between the coordinator and
        // connection. The coordinator will send a canceled message on it if a
        // cancellation request comes. The connection will reset that on every message
        // it receives and then check for it where we want to add the ability to cancel
        // an in-progress statement.
        let (cancel_tx, cancel_rx) = watch::channel(Canceled::NotCanceled);
        let cancel_tx = Arc::new(cancel_tx);
        let client = SessionClient {
            inner: Some(self.clone()),
            session,
            cancel_tx: Arc::clone(&cancel_tx),
            cancel_rx,
            timeouts: Timeout::new(),
            environment_id: self.environment_id.clone(),
            segment_client: self.segment_client.clone(),
        };
        let response = client
            .send(|tx| Command::Startup {
                session: client.session.metadata(),
                cancel_tx,
                tx,
                set_setting_keys,
            })
            .await;
        match response {
            Ok(response) => Ok((client, response)),
            Err(e) => {
                // When startup fails, no need to call terminate. Remove the
                // session from the client to sidestep the panic in the `Drop`
                // implementation.
                // TODO: Figure out what to do here.
                //client.session.take();
                Err(e)
            }
        }
    }

    /// Cancels the query currently running on the specified connection.
    pub fn cancel_request(&mut self, conn_id: ConnectionIdType, secret_key: u32) {
        self.send(Command::CancelRequest {
            conn_id,
            secret_key,
        });
    }

    /// Executes a single SQL statement that returns rows as the
    /// `mz_introspection` user.
    pub async fn introspection_execute_one(&self, sql: &str) -> Result<Vec<Row>, anyhow::Error> {
        // Connect to the coordinator.
        let conn_id = self.new_conn_id()?;
        let session = self.new_session(conn_id, SUPPORT_USER.clone());
        let (mut session_client, _) = self.startup(session, vec![]).await?;

        // Parse the SQL statement.
        let stmts = mz_sql::parse::parse(sql)?;
        if stmts.len() != 1 {
            bail!("must supply exactly one query");
        }
        let StatementParseResult { ast: stmt, sql } = stmts.into_element();

        const EMPTY_PORTAL: &str = "";
        session_client.start_transaction(Some(1))?;
        session_client
            .declare(EMPTY_PORTAL.into(), stmt, sql.to_string(), vec![])
            .await?;
        match session_client
            .execute(EMPTY_PORTAL.into(), futures::future::pending(), None)
            .await?
        {
            (ExecuteResponse::SendingRows { future, span: _ }, _) => match future.await {
                PeekResponseUnary::Rows(rows) => Ok(rows),
                PeekResponseUnary::Canceled => bail!("query canceled"),
                PeekResponseUnary::Error(e) => bail!(e),
            },
            r => bail!("unsupported response type: {r:?}"),
        }
    }

    /// Returns the metrics associated with the adapter layer.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub async fn append_webhook(
        &self,
        database: String,
        schema: String,
        name: String,
        session: SessionMetadata,
    ) -> Result<AppendWebhookResponse, AdapterError> {
        let (tx, rx) = oneshot::channel();

        // Send our request.
        self.send(Command::AppendWebhook {
            database,
            schema,
            name,
            session,
            tx,
        });

        // Using our one shot channel to get the result, returning an error if the sender dropped.
        let response = rx.await.map_err(|_| {
            AdapterError::Internal("failed to receive webhook response".to_string())
        })?;

        response
    }

    fn send(&self, cmd: Command) {
        self.inner_cmd_tx
            .send(cmd)
            .expect("coordinator unexpectedly gone");
    }
}

/// A coordinator client that is bound to a connection.
///
/// See also [`Client`].
pub struct SessionClient {
    // Invariant: inner may only be `None` after the session has been terminated.
    // Once the session is terminated, no communication to the Coordinator
    // should be attempted.
    inner: Option<Client>,
    // Invariant: session may only be `None` during a method call. Every public
    // method must ensure that `Session` is `Some` before it returns.
    session: Session,
    cancel_tx: Arc<watch::Sender<Canceled>>,
    cancel_rx: watch::Receiver<Canceled>,
    timeouts: Timeout,
    segment_client: Option<mz_segment::Client>,
    environment_id: EnvironmentId,
}

impl SessionClient {
    /// Parses a SQL expression, reporting failures as a telemetry event if
    /// possible.
    pub fn parse<'a>(
        &self,
        sql: &'a str,
    ) -> Result<Result<Vec<StatementParseResult<'a>>, ParserStatementError>, String> {
        match mz_sql::parse::parse_with_limit(sql) {
            Ok(Err(e)) => {
                self.track_statement_parse_failure(&e);
                Ok(Err(e))
            }
            r => r,
        }
    }

    fn track_statement_parse_failure(&self, parse_error: &ParserStatementError) {
        let session = &self.session;
        let Some(user_id) = session.user().external_metadata.as_ref().map(|m| m.user_id) else {
            return;
        };
        let Some(segment_client) = &self.segment_client else {
            return;
        };
        let Some(statement_kind) = parse_error.statement else {
            return;
        };
        let Some((action, object_type)) = telemetry::analyze_audited_statement(statement_kind) else {
            return;
        };
        let event_type = StatementFailureType::ParseFailure;
        let event_name = format!(
            "{} {} {}",
            object_type.as_title_case(),
            action.as_title_case(),
            event_type.as_title_case(),
        );
        segment_client.environment_track(
            &self.environment_id,
            session.application_name(),
            user_id,
            event_name,
            json!({
                "statement_kind": statement_kind,
                "error": &parse_error.error,
            }),
        );
    }

    pub fn canceled(&self) -> impl Future<Output = ()> + Send {
        let mut cancel_rx = self.cancel_rx.clone();
        async move {
            loop {
                let _ = cancel_rx.changed().await;
                if let Canceled::Canceled = *cancel_rx.borrow() {
                    return;
                }
            }
        }
    }

    pub fn reset_canceled(&mut self) {
        // Clear any cancellation message.
        // TODO(mjibson): This makes the use of .changed annoying since it will
        // generally always have a NotCanceled message first that needs to be ignored,
        // and thus run in a loop. Figure out a way to have the future only resolve on
        // a Canceled message.
        let _ = self.cancel_tx.send(Canceled::NotCanceled);
    }

    // Verify and return the named prepared statement. We need to verify each use
    // to make sure the prepared statement is still safe to use.
    pub async fn get_prepared_statement(
        &mut self,
        name: &str,
    ) -> Result<&PreparedStatement, AdapterError> {
        let catalog = self.catalog_snapshot().await?;
        Coordinator::verify_prepared_statement(&catalog, &mut self.session, name)?;
        Ok(self
            .session()
            .get_prepared_statement_unverified(name)
            .expect("must exist"))
    }

    /// Saves the parsed statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`crate::session::Session`]
    /// under the specified name.
    pub async fn prepare(
        &mut self,
        name: String,
        stmt: Option<Statement<Raw>>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let catalog = self.catalog_snapshot().await?;

        // Note: This failpoint is used to simulate a request outliving the external connection
        // that made it.
        let mut async_pause = false;
        (|| {
            fail::fail_point!("async_prepare", |val| {
                async_pause = val.map_or(false, |val| val.parse().unwrap_or(false))
            });
        })();
        if async_pause {
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        let desc = Coordinator::describe(&catalog, &self.session, stmt.clone(), param_types)?;
        self.session.set_prepared_statement(
            name,
            stmt,
            sql,
            desc,
            catalog.transient_revision(),
            self.now(),
        );
        Ok(())
    }

    /// Binds a statement to a portal.
    pub async fn declare(
        &mut self,
        name: String,
        stmt: Statement<Raw>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let catalog = self.catalog_snapshot().await?;
        self.declare_inner(&catalog, name, stmt, sql, param_types)
    }

    /// Executes a previously-bound portal.
    #[tracing::instrument(level = "debug", skip(self, cancel_future))]
    pub async fn execute(
        &mut self,
        portal_name: String,
        cancel_future: impl Future<Output = std::io::Error> + Send,
        // If this command was part of another execute command
        // (for example, executing a `FETCH` statement causes an execute to be
        //  issued for the cursor it references),
        // then `outer_context` should be `Some`.
        // This instructs the coordinator that the
        // outer execute should be considered finished once the inner one is.
        outer_context: Option<ExecuteContextExtra>,
    ) -> Result<(ExecuteResponse, Instant), AdapterError> {
        let execute_started = Instant::now();
        let catalog = self.catalog_snapshot().await?;

        if self.session.vars().emit_trace_id_notice() {
            let span_context = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .clone();
            if span_context.is_valid() {
                self.session.add_notice(AdapterNotice::QueryTrace {
                    trace_id: span_context.trace_id(),
                });
            }
        }

        if let Err(err) = self.verify_portal(&catalog, &portal_name) {
            // If statement logging hasn't started yet, we don't need
            // to add any "end" event, so just make up a no-op
            // `ExecuteContextExtra` here, via `Default::default`.
            //
            // It's a bit unfortunate because the edge case of failed
            // portal verifications won't show up in statement
            // logging, but there seems to be nothing else we can do,
            // because we need access to the portal to begin logging.
            //
            // Another option would be to log a begin and end event, but just fill in NULLs
            // for everything we get from the portal (prepared statement id, params).

            // TODO(adapter): care about outer_context?
            //let extra = outer_context.unwrap_or_else(Default::default);
            return Err(err);
        }

        // The reference to `portal` can't outlive `session`, which we
        // use to construct the context, so scope the reference to this block where we
        // get everything we need from the portal for later.
        let (stmt, extra, params) = {
            let portal = self
                .session
                .get_portal_unverified(&portal_name)
                .expect("known to exist");
            let params = portal.parameters.clone();
            let stmt = portal.stmt.clone();
            let logging = Arc::clone(&portal.logging);

            let extra = if let Some(extra) = outer_context {
                // We are executing in the context of another SQL statement, so we don't
                // want to begin statement logging anew. The context of the actual statement
                // being executed is the one that should be retired once this finishes.
                extra
            } else {
                // This is a new statement, log it and return the context
                let maybe_uuid =
                    self.begin_statement_execution(&mut self.session, params.clone(), &logging);

                ExecuteContextExtra::new(maybe_uuid)
            };
            (stmt, extra, params)
        };

        let Some(stmt) = stmt else {
            return Ok((ExecuteResponse::EmptyQuery, execute_started));
        };

        /*
        TODO(adapter)

        let session_type = metrics::session_type_label_value(self.session.user());
        let stmt_type = metrics::statement_type_label_value(&stmt);
        self.metrics
            .query_total
            .with_label_values(&[session_type, stmt_type])
            .inc();
        match &stmt {
            Statement::Subscribe(SubscribeStatement { output, .. })
            | Statement::Copy(CopyStatement {
                relation: CopyRelation::Subscribe(SubscribeStatement { output, .. }),
                ..
            }) => {
                self.metrics
                    .subscribe_outputs
                    .with_label_values(&[
                        session_type,
                        metrics::subscribe_output_label_value(output),
                    ])
                    .inc();
            }
            _ => {}
        }
        */

        // Verify that this statement type can be executed in the current
        // transaction state.
        match self.session.transaction_mut() {
            // By this point we should be in a running transaction.
            TransactionStatus::Default => unreachable!(),

            // Failed transactions have already been checked in pgwire for a safe statement
            // (COMMIT, ROLLBACK, etc.) and can proceed.
            TransactionStatus::Failed(_) => {}

            // Started is a deceptive name, and means different things depending on which
            // protocol was used. It's either exactly one statement (known because this
            // is the simple protocol and the parser parsed the entire string, and it had
            // one statement). Or from the extended protocol, it means *some* query is
            // being executed, but there might be others after it before the Sync (commit)
            // message. Postgres handles this by teaching Started to eagerly commit certain
            // statements that can't be run in a transaction block.
            TransactionStatus::Started(_) => {
                if let Statement::Declare(_) = stmt {
                    // Declare is an exception. Although it's not against any spec to execute
                    // it, it will always result in nothing happening, since all portals will be
                    // immediately closed. Users don't know this detail, so this error helps them
                    // understand what's going wrong. Postgres does this too.
                    return Err(AdapterError::OperationRequiresTransaction(
                        "DECLARE CURSOR".into(),
                    ));
                }

                // TODO(mjibson): The current code causes DDL statements (well, any statement
                // that doesn't call `add_transaction_ops`) to execute outside of the extended
                // protocol transaction. For example, executing in extended a SELECT, then
                // CREATE, then SELECT, followed by a Sync would register the transaction
                // as read only in the first SELECT, then the CREATE ignores the transaction
                // ops, and the last SELECT will use the timestamp from the first. This isn't
                // correct, but this is an edge case that we can fix later.
            }

            // Implicit or explicit transactions.
            //
            // Implicit transactions happen when a multi-statement query is executed
            // (a "simple query"). However if a "BEGIN" appears somewhere in there,
            // then the existing implicit transaction will be upgraded to an explicit
            // transaction. Thus, we should not separate what implicit and explicit
            // transactions can do unless there's some additional checking to make sure
            // something disallowed in explicit transactions did not previously take place
            // in the implicit portion.
            txn @ TransactionStatus::InTransactionImplicit(_)
            | txn @ TransactionStatus::InTransaction(_) => {
                match stmt {
                    // Statements that are safe in a transaction. We still need to verify that we
                    // don't interleave reads and writes since we can't perform those serializably.
                    Statement::Close(_)
                    | Statement::Commit(_)
                    | Statement::Copy(_)
                    | Statement::Deallocate(_)
                    | Statement::Declare(_)
                    | Statement::Discard(_)
                    | Statement::Execute(_)
                    | Statement::Explain(_)
                    | Statement::Fetch(_)
                    | Statement::Prepare(_)
                    | Statement::Rollback(_)
                    | Statement::Select(_)
                    | Statement::SetTransaction(_)
                    | Statement::Show(_)
                    | Statement::SetVariable(_)
                    | Statement::ResetVariable(_)
                    | Statement::StartTransaction(_)
                    | Statement::Subscribe(_)
                    | Statement::Raise(_) => {
                        // Always safe.
                    }

                    Statement::Insert(ref insert_statement)
                        if matches!(
                            insert_statement.source,
                            InsertSource::Query(Query {
                                body: SetExpr::Values(..),
                                ..
                            }) | InsertSource::DefaultValues
                        ) =>
                    {
                        // Inserting from default? values statements
                        // is always safe.
                    }

                    // Statements below must by run singly (in Started).
                    Statement::AlterCluster(_)
                    | Statement::AlterConnection(_)
                    | Statement::AlterDefaultPrivileges(_)
                    | Statement::AlterIndex(_)
                    | Statement::AlterSetCluster(_)
                    | Statement::AlterObjectRename(_)
                    | Statement::AlterOwner(_)
                    | Statement::AlterRole(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterSink(_)
                    | Statement::AlterSource(_)
                    | Statement::AlterSystemReset(_)
                    | Statement::AlterSystemResetAll(_)
                    | Statement::AlterSystemSet(_)
                    | Statement::CreateCluster(_)
                    | Statement::CreateClusterReplica(_)
                    | Statement::CreateConnection(_)
                    | Statement::CreateDatabase(_)
                    | Statement::CreateIndex(_)
                    | Statement::CreateMaterializedView(_)
                    | Statement::CreateRole(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreateSecret(_)
                    | Statement::CreateSink(_)
                    | Statement::CreateSource(_)
                    | Statement::CreateSubsource(_)
                    | Statement::CreateTable(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateWebhookSource(_)
                    | Statement::Delete(_)
                    | Statement::DropObjects(_)
                    | Statement::DropOwned(_)
                    | Statement::GrantPrivileges(_)
                    | Statement::GrantRole(_)
                    | Statement::Insert(_)
                    | Statement::ReassignOwned(_)
                    | Statement::RevokePrivileges(_)
                    | Statement::RevokeRole(_)
                    | Statement::Update(_)
                    | Statement::ValidateConnection(_) => {
                        // If we're not in an implicit transaction and we could generate exactly one
                        // valid ExecuteResponse, we can delay execution until commit.
                        if !txn.is_implicit() {
                            // Statements whose tag is trivial (known only from an unexecuted statement) can
                            // be run in a special single-statement explicit mode. In this mode (`BEGIN;
                            // <stmt>; COMMIT`), we generate the expected tag from a successful <stmt>, but
                            // delay execution until `COMMIT`.
                            if let Ok(resp) = ExecuteResponse::try_from(&stmt) {
                                if let Err(err) =
                                    txn.add_ops(TransactionOps::SingleStatement { stmt, params })
                                {
                                    return Err(err);
                                }
                                // TODO(adapter)
                                todo!()
                                //return Ok(resp);
                            }
                        }

                        return Err(AdapterError::OperationProhibitsTransaction(
                            stmt.to_string(),
                        ));
                    }
                }
            }
        }

        // TODO(adapter): remove needless conversion to metadata
        let session_catalog = catalog.for_session(&self.session.metadata());
        let original_stmt = stmt.clone();
        let (stmt, resolved_ids) = match mz_sql::names::resolve(&session_catalog, stmt) {
            Ok(resolved) => resolved,
            Err(e) => return Err(e.into()),
        };
        // N.B. The catalog can change during purification so we must validate that the dependencies still exist after
        // purification.  This should be done back on the main thread.
        // We do the validation:
        //   - In the handler for `Message::PurifiedStatementReady`, before we handle the purified statement.
        // If we add special handling for more types of `Statement`s, we'll need to ensure similar verification
        // occurs.
        let (plan, resolved_ids) = match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            stmt @ (Statement::CreateSource(_) | Statement::AlterSource(_)) => {
                let otel_ctx = OpenTelemetryContext::obtain();

                // Checks if the session is authorized to purify a statement. Usually
                // authorization is checked after planning, however purification happens before
                // planning, which may require the use of some connections and secrets.
                if let Err(e) =
                    rbac::check_item_usage(&session_catalog, &self.session, &resolved_ids)
                {
                    return Err(e);
                }

                // TODO: Listen for cancellation.
                let (subsource_stmts, stmt) = mz_sql::pure::purify_statement(
                    &session_catalog,
                    self.now(),
                    stmt,
                    &self.inner().connection_context,
                )
                .await?;

                let mut plans: Vec<CreateSourcePlans> = vec![];
                let mut id_allocation = BTreeMap::new();

                // First we'll allocate global ids for each subsource and plan them
                for (transient_id, subsource_stmt) in subsource_stmts {
                    let resolved_ids = mz_sql::names::visit_dependencies(&subsource_stmt);
                    let source_id = self.allocate_user_id().await?;
                    let plan = match mz_sql::plan::plan(
                        Some(self.session.pcx()),
                        &session_catalog,
                        Statement::CreateSubsource(subsource_stmt),
                        &params,
                        &resolved_ids,
                    )? {
                        Plan::CreateSource(plan) => plan,
                        _ => {
                            unreachable!(
                                "planning CREATE SUBSOURCE must result in a Plan::CreateSource"
                            )
                        }
                    };
                    id_allocation.insert(transient_id, source_id);
                    plans.push(CreateSourcePlans {
                        source_id,
                        plan,
                        resolved_ids,
                    });
                }

                // Then, we'll rewrite the source statement to point to the newly minted global ids and
                // plan it too
                let stmt = mz_sql::names::resolve_transient_ids(&id_allocation, stmt)?;
                let resolved_ids = mz_sql::names::visit_dependencies(&stmt);

                match mz_sql::plan::plan(
                    Some(self.session.pcx()),
                    &session_catalog,
                    stmt,
                    &params,
                    &resolved_ids,
                )? {
                    Plan::CreateSource(plan) => {
                        let source_id = self.allocate_user_id().await?;
                        plans.push(CreateSourcePlans {
                            source_id,
                            plan,
                            resolved_ids,
                        });
                        (Plan::CreateSources(plans), ResolvedIds(BTreeSet::new()))
                    }
                    Plan::AlterSource(alter_source) => (
                        Plan::PurifiedAlterSource {
                            alter_source,
                            subsources: plans,
                        },
                        ResolvedIds(BTreeSet::new()),
                    ),
                    plan @ Plan::AlterNoop(..) => (plan, ResolvedIds(BTreeSet::new())),
                    p => {
                        unreachable!("{:?} is not purified", p)
                    }
                }
            }

            // `CREATE SUBSOURCE` statements are disallowed for users and are only generated
            // automatically as part of purification
            Statement::CreateSubsource(_) => {
                return Err(AdapterError::Unsupported("CREATE SUBSOURCE statements"))
            }

            // All other statements are handled immediately.
            _ => (
                mz_sql::plan::plan(
                    Some(self.session.pcx()),
                    &session_catalog,
                    stmt,
                    &params,
                    &resolved_ids,
                )?,
                resolved_ids,
            ),
        };

        Ok((
            self.sequence_plan(&catalog, plan, resolved_ids).await?,
            execute_started,
        ))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_plan(
        &mut self,
        catalog: &Catalog,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        let mut responses = ExecuteResponse::generated_from(PlanKind::from(&plan));
        responses.push(ExecuteResponseKind::Canceled);
        // TODO(adapter)
        //ctx.tx_mut().set_allowed(responses);

        {
            // TODO: We also create this in execute. Would be nice to not do it twice. Can we just
            // move this small block to execute also?
            // TODO(adapter): remove .metadata
            let session_catalog = catalog.for_session(&self.session.metadata());
            introspection::user_privilege_hack(
                &session_catalog,
                &self.session,
                &plan,
                &resolved_ids,
            )?;
            introspection::check_cluster_restrictions(&session_catalog, &plan)?;
        }

        // If our query only depends on system tables, a LaunchDarkly flag is enabled, and a
        // session var is set, then we automatically run the query on the mz_introspection cluster.
        let target_cluster =
            introspection::auto_run_on_introspection(catalog, &self.session, &plan);
        // TODO(adapter): remove .metadata
        let target_cluster_id = catalog
            .resolve_target_cluster(target_cluster, &self.session.metadata())
            .ok()
            .map(|cluster| cluster.id());

        // TODO(adapter)
        /*
        rbac::check_plan(
            self,
            session_catalog,
            session,
            &plan,
            target_cluster_id,
            &resolved_ids,
        )?;
        */

        match plan {
            // Plans handled by the coordinator.
            Plan::AlterCluster(_)
            | Plan::AlterClusterRename(_)
            | Plan::AlterClusterReplicaRename(_)
            | Plan::AlterDefaultPrivileges(_)
            | Plan::AlterIndexResetOptions(_)
            | Plan::AlterIndexSetOptions(_)
            | Plan::AlterItemRename(_)
            | Plan::AlterNoop(_)
            | Plan::AlterOwner(_)
            | Plan::AlterRole(_)
            | Plan::AlterSecret(_)
            | Plan::AlterSetCluster(_)
            | Plan::AlterSink(_)
            | Plan::AlterSource(_)
            | Plan::AlterSystemReset(_)
            | Plan::AlterSystemResetAll(_)
            | Plan::AlterSystemSet(_)
            | Plan::CreateCluster(_)
            | Plan::CreateClusterReplica(_)
            | Plan::CreateConnection(_)
            | Plan::CreateDatabase(_)
            | Plan::CreateIndex(_)
            | Plan::CreateMaterializedView(_)
            | Plan::CreateRole(_)
            | Plan::CreateSchema(_)
            | Plan::CreateSecret(_)
            | Plan::CreateSink(_)
            | Plan::CreateSource(_)
            | Plan::CreateSources(_)
            | Plan::CreateTable(_)
            | Plan::CreateType(_)
            | Plan::CreateView(_)
            | Plan::DiscardAll
            | Plan::DiscardTemp
            | Plan::DropObjects(_)
            | Plan::DropOwned(_)
            | Plan::Explain(_)
            | Plan::GrantPrivileges(_)
            | Plan::GrantRole(_)
            | Plan::InspectShard(_)
            | Plan::PurifiedAlterSource { .. }
            | Plan::ReadThenWrite(_)
            | Plan::ReassignOwned(_)
            | Plan::RevokePrivileges(_)
            | Plan::RevokeRole(_)
            | Plan::RotateKeys(_)
            | Plan::Select(_)
            | Plan::ShowCreate(_)
            | Plan::SideEffectingFunc(_)
            | Plan::Subscribe(_)
            | Plan::ValidateConnection(_) => self.from_session_response(
                self.send(|tx| Command::Sequence {
                    plan,
                    session: self.session.metadata(),
                    tx,
                    outer_ctx_extra: None,
                    resolved_ids,
                    span: todo!(),
                })
                .await,
            ),

            // Plans handled directly by the adapter client.
            Plan::CommitTransaction(CommitTransactionPlan {
                ref transaction_type,
            })
            | Plan::AbortTransaction(AbortTransactionPlan {
                ref transaction_type,
            }) => {
                let action = match &plan {
                    Plan::CommitTransaction(_) => EndTransactionAction::Commit,
                    Plan::AbortTransaction(_) => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                if self.session.transaction().is_implicit() && !transaction_type.is_implicit() {
                    // In Postgres, if a user sends a COMMIT or ROLLBACK in an
                    // implicit transaction, a warning is sent warning them.
                    // (The transaction is still closed and a new implicit
                    // transaction started, though.)
                    self.session
                        .add_notice(AdapterNotice::ExplicitTransactionControlInImplicitTransaction);
                }
                self.sequence_end_transaction(catalog, action);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => Ok(ExecuteResponse::Fetch {
                name,
                count,
                timeout,
                ctx_extra,
            }),
            Plan::CopyFrom(plan) => Ok(ExecuteResponse::CopyFrom {
                id: plan.id,
                columns: plan.columns,
                params: plan.params,
                // TODO(adapter)
                ctx_extra: todo!(),
            }),
            Plan::EmptyQuery => Ok(ExecuteResponse::EmptyQuery),
            Plan::SetVariable(plan) => self.sequence_set_variable(catalog, plan),
            Plan::SetTransaction(plan) => self.sequence_set_transaction(catalog, plan),
            Plan::ResetVariable(plan) => self.sequence_reset_variable(catalog, plan),
            Plan::ShowAllVariables => self.sequence_show_all_variables(catalog),
            Plan::ShowVariable(plan) => self.sequence_show_variable(catalog, plan),
            Plan::Execute(plan) => {
                let portal_name = self.sequence_execute(catalog, plan)?;
                /*
                TODO(adapter)
                self.internal_cmd_tx
                    .send(Message::Execute {
                        portal_name,
                        ctx,
                        span: tracing::Span::none(),
                    })
                    .expect("sending to self.internal_cmd_tx cannot fail");
                */
                todo!()
            }
            Plan::CopyRows(CopyRowsPlan { id, columns, rows }) => {
                self.sequence_copy_rows(catalog, id, columns, rows)
            }
            Plan::Insert(plan) => self.sequence_insert(catalog, plan),
            Plan::Prepare(plan) => {
                if self
                    .session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    Err(AdapterError::PreparedStatementExists(plan.name))
                } else {
                    self.session.set_prepared_statement(
                        plan.name,
                        Some(plan.stmt),
                        plan.sql,
                        plan.desc,
                        catalog.transient_revision(),
                        self.now(),
                    );
                    Ok(ExecuteResponse::Prepare)
                }
            }
            Plan::Close(plan) => {
                if self.session.remove_portal(&plan.name) {
                    Ok(ExecuteResponse::ClosedCursor)
                } else {
                    Err(AdapterError::UnknownCursor(plan.name))
                }
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if self.session.remove_prepared_statement(&name) {
                        Ok(ExecuteResponse::Deallocate { all: false })
                    } else {
                        Err(AdapterError::UnknownPreparedStatement(name))
                    }
                }
                None => {
                    self.session.remove_all_prepared_statements();
                    Ok(ExecuteResponse::Deallocate { all: true })
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                self.session
                    .add_notice(AdapterNotice::UserRequested { severity });
                Ok(ExecuteResponse::Raised)
            }
            Plan::Declare(DeclarePlan { name, stmt, sql }) => {
                let param_types = vec![];
                self.declare_inner(catalog, name, stmt, sql, param_types)?;
                Ok(ExecuteResponse::DeclaredCursor)
            }
            Plan::StartTransaction(plan) => {
                if matches!(
                    self.session().transaction(),
                    TransactionStatus::InTransaction(_)
                ) {
                    self.session()
                        .add_notice(AdapterNotice::ExistingTransactionInProgress);
                }
                let result = self.session.start_transaction(
                    self.now_datetime(),
                    plan.access,
                    plan.isolation_level,
                );
                result.map(|_| ExecuteResponse::StartedTransaction)
            }
        }
    }

    fn sequence_end_transaction(&mut self, catalog: &Catalog, mut action: EndTransactionAction) {
        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, self.session.transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = match action {
            EndTransactionAction::Commit => Ok(PendingTxnResponse::Committed {
                params: BTreeMap::new(),
            }),
            EndTransactionAction::Rollback => Ok(PendingTxnResponse::Rolledback {
                params: BTreeMap::new(),
            }),
        };

        let result = self.sequence_end_transaction_inner(action);

        let (response, action) = match result {
            Ok((Some(TransactionOps::Writes(writes)), _)) if writes.is_empty() => {
                (response, action)
            }
            Ok((Some(TransactionOps::Writes(writes)), write_lock_guard)) => {
                self.submit_write(PendingWriteTxn::User {
                    writes,
                    write_lock_guard,
                    pending_txn: PendingTxn {
                        ctx,
                        response,
                        action,
                    },
                });
                return;
            }
            Ok((Some(TransactionOps::Peeks(determination)), _))
                if self.session.vars().transaction_isolation()
                    == &IsolationLevel::StrictSerializable =>
            {
                self.strict_serializable_reads_tx
                    .send(PendingReadTxn {
                        txn: PendingRead::Read {
                            txn: PendingTxn {
                                ctx,
                                response,
                                action,
                            },
                            timestamp_context: determination.timestamp_context,
                        },
                        created: Instant::now(),
                        num_requeues: 0,
                    })
                    .expect("sending to strict_serializable_reads_tx cannot fail");
                return;
            }
            Ok((Some(TransactionOps::SingleStatement { stmt, params }), _)) => {
                self.internal_cmd_tx
                    .send(Message::ExecuteSingleStatementTransaction { ctx, stmt, params })
                    .expect("must send");
                return;
            }
            Ok((_, _)) => (response, action),
            Err(err) => (Err(err), EndTransactionAction::Rollback),
        };
        let changed = self.session.vars_mut().end_transaction(action);
        // Append any parameters that changed to the response.
        let response = response.map(|mut r| {
            r.extend_params(changed);
            ExecuteResponse::from(r)
        });

        ctx.retire(response);
    }

    fn sequence_end_transaction_inner(
        &mut self,
        catalog: &Catalog,
        action: EndTransactionAction,
    ) -> Result<
        (
            Option<TransactionOps<Timestamp>>,
            Option<OwnedMutexGuard<()>>,
        ),
        AdapterError,
    > {
        let txn = self.clear_transaction(session);

        if let EndTransactionAction::Commit = action {
            if let (Some(mut ops), write_lock_guard) = txn.into_ops_and_lock_guard() {
                if let TransactionOps::Writes(writes) = &mut ops {
                    for WriteOp { id, .. } in &mut writes.iter() {
                        // Re-verify this id exists.
                        let _ = catalog.try_get_entry(id).ok_or_else(|| {
                            AdapterError::SqlCatalog(CatalogError::UnknownItem(id.to_string()))
                        })?;
                    }

                    // `rows` can be empty if, say, a DELETE's WHERE clause had 0 results.
                    writes.retain(|WriteOp { rows, .. }| !rows.is_empty());
                }
                return Ok((Some(ops), write_lock_guard));
            }
        }

        Ok((None, None))
    }

    fn declare_inner(
        &self,
        catalog: &Catalog,
        name: String,
        stmt: Statement<Raw>,
        sql: String,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = Coordinator::describe(catalog, &self.session, Some(stmt), param_types)?;
        let params = vec![];
        let result_formats = vec![mz_pgrepr::Format::Text; desc.arity()];
        let logging = self.session.mint_logging(sql);
        self.session.set_portal(
            name,
            desc,
            Some(stmt),
            logging,
            params,
            result_formats,
            catalog.transient_revision(),
        )?;
        Ok(())
    }

    fn sequence_insert(
        &mut self,
        catalog: &Catalog,
        plan: plan::InsertPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let optimized_mir = if let Some(..) = &plan.values.as_const() {
            // We don't perform any optimizations on an expression that is already
            // a constant for writes, as we want to maximize bulk-insert throughput.
            OptimizedMirRelationExpr(plan.values)
        } else {
            let view_optimizer =
                Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context());
            view_optimizer.optimize(plan.values)?
        };

        match optimized_mir.into_inner() {
            selection if selection.as_const().is_some() && plan.returning.is_empty() => {
                self.sequence_insert_constant(catalog, plan.id, selection)
            }
            // All non-constant values must be planned as read-then-writes.
            selection => {
                let desc_arity = match catalog.try_get_entry(&plan.id) {
                    Some(table) => table
                        .desc(
                            &catalog
                                .resolve_full_name(table.name(), Some(self.session().conn_id())),
                        )
                        .expect("desc called on table")
                        .arity(),
                    None => {
                        return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                            plan.id.to_string(),
                        )));
                    }
                };

                if selection.contains_temporal() {
                    return Err(AdapterError::Unsupported(
                        "calls to mz_now in write statements",
                    ));
                }

                let finishing = RowSetFinishing {
                    order_by: vec![],
                    limit: None,
                    offset: 0,
                    project: (0..desc_arity).collect(),
                };

                let read_then_write_plan = plan::ReadThenWritePlan {
                    id: plan.id,
                    selection,
                    finishing,
                    assignments: BTreeMap::new(),
                    kind: MutationKind::Insert,
                    returning: plan.returning,
                };

                /* TODO(adapter)
                self.sequence_read_then_write(ctx, read_then_write_plan)
                    .await;
                */
                todo!()
            }
        }
    }

    fn sequence_copy_rows(
        &mut self,
        catalog: &Catalog,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let conn_catalog = catalog.for_session(&self.session);
        mz_sql::plan::plan_copy_from(self.session.pcx(), &conn_catalog, id, columns, rows)
            .err_into()
            .and_then(|values| values.lower().err_into())
            .and_then(|values| {
                Optimizer::logical_optimizer(&mz_transform::typecheck::empty_context())
                    .optimize(values)
                    .err_into()
            })
            .and_then(|values| {
                // Copied rows must always be constants.
                self.sequence_insert_constant(catalog, id, values.into_inner())
            })
    }

    fn sequence_insert_constant(
        &mut self,
        catalog: &Catalog,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match catalog.try_get_entry(&id) {
            Some(table) => table
                .desc(&catalog.resolve_full_name(table.name(), Some(self.session.conn_id())))?,
            None => {
                return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants.as_const() {
            Some((rows, ..)) => {
                let rows = rows.clone()?;
                for (row, _) in &rows {
                    for (i, datum) in row.iter().enumerate() {
                        desc.constraints_met(i, &datum)?;
                    }
                }
                let diffs_plan = plan::SendDiffsPlan {
                    id,
                    updates: rows,
                    kind: MutationKind::Insert,
                    returning: Vec::new(),
                    max_result_size: catalog.system_config().max_result_size(),
                };
                self.sequence_send_diffs(diffs_plan)
            }
            None => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                constants
            ),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn sequence_send_diffs(
        &mut self,
        mut plan: plan::SendDiffsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let affected_rows = {
            let mut affected_rows = Diff::from(0);
            let mut all_positive_diffs = true;
            // If all diffs are positive, the number of affected rows is just the
            // sum of all unconsolidated diffs.
            for (_, diff) in plan.updates.iter() {
                if *diff < 0 {
                    all_positive_diffs = false;
                    break;
                }

                affected_rows += diff;
            }

            if !all_positive_diffs {
                // Consolidate rows. This is useful e.g. for an UPDATE where the row
                // doesn't change, and we need to reflect that in the number of
                // affected rows.
                differential_dataflow::consolidation::consolidate(&mut plan.updates);

                affected_rows = 0;
                // With retractions, the number of affected rows is not the number
                // of rows we see, but the sum of the absolute value of their diffs,
                // e.g. if one row is retracted and another is added, the total
                // number of rows affected is 2.
                for (_, diff) in plan.updates.iter() {
                    affected_rows += diff.abs();
                }
            }

            usize::try_from(affected_rows).expect("positive isize must fit")
        };
        event!(
            Level::TRACE,
            affected_rows,
            id = format!("{:?}", plan.id),
            kind = format!("{:?}", plan.kind),
            updates = plan.updates.len(),
            returning = plan.returning.len(),
        );

        self.session
            .add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
                id: plan.id,
                rows: plan.updates,
            }]))?;
        if !plan.returning.is_empty() {
            let finishing = RowSetFinishing {
                order_by: Vec::new(),
                limit: None,
                offset: 0,
                project: (0..plan.returning[0].0.iter().count()).collect(),
            };
            return match finishing.finish(plan.returning, plan.max_result_size) {
                Ok(rows) => Ok(Coordinator::send_immediate_rows(rows)),
                Err(e) => Err(AdapterError::ResultSize(e)),
            };
        }
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }

    // Returns the name of the portal to execute.
    fn sequence_execute(
        &mut self,
        catalog: &Catalog,
        plan: plan::ExecutePlan,
    ) -> Result<String, AdapterError> {
        // Verify the stmt is still valid.
        Coordinator::verify_prepared_statement(catalog, &mut self.session, &plan.name)?;
        let ps = self
            .session
            .get_prepared_statement_unverified(&plan.name)
            .expect("known to exist");
        let stmt = ps.stmt().cloned();
        let desc = ps.desc().clone();
        let revision = ps.catalog_revision;
        let logging = Arc::clone(ps.logging());
        self.session
            .create_new_portal(stmt, logging, desc, plan.params, Vec::new(), revision)
    }

    fn sequence_show_variable(
        &self,
        catalog: &Catalog,
        plan: plan::ShowVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        if &plan.name == SCHEMA_ALIAS {
            let schemas = catalog.resolve_search_path(&self.session);
            let schema = schemas.first();
            return match schema {
                Some((database_spec, schema_spec)) => {
                    let schema_name = &catalog
                        .get_schema(database_spec, schema_spec, self.session.conn_id())
                        .name()
                        .schema;
                    let row = Row::pack_slice(&[Datum::String(schema_name)]);
                    Ok(Coordinator::send_immediate_rows(vec![row]))
                }
                None => {
                    self.session
                        .add_notice(AdapterNotice::NoResolvableSearchPathSchema {
                            search_path: self
                                .session
                                .vars()
                                .search_path()
                                .into_iter()
                                .map(|schema| schema.to_string())
                                .collect(),
                        });
                    Ok(Coordinator::send_immediate_rows(vec![Row::pack_slice(&[
                        Datum::Null,
                    ])]))
                }
            };
        }

        let variable = self
            .session
            .vars()
            .get(Some(catalog.system_config()), &plan.name)
            .or_else(|_| catalog.system_config().get(&plan.name))?;

        // In lieu of plumbing the user to all system config functions, just check that the var is
        // visible.
        variable.visible(self.session.user(), Some(catalog.system_config()))?;

        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        if variable.name() == DATABASE_VAR_NAME
            && matches!(
                catalog.resolve_database(&variable.value()),
                Err(CatalogError::UnknownDatabase(_))
            )
        {
            let name = variable.value();
            self.session
                .add_notice(AdapterNotice::DatabaseDoesNotExist { name });
        } else if variable.name() == CLUSTER_VAR_NAME
            && matches!(
                catalog.resolve_cluster(&variable.value()),
                Err(CatalogError::UnknownCluster(_))
            )
        {
            let name = variable.value();
            self.session
                .add_notice(AdapterNotice::ClusterDoesNotExist { name });
        }
        Ok(Coordinator::send_immediate_rows(vec![row]))
    }

    fn sequence_show_all_variables(
        &self,
        catalog: &Catalog,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut rows = viewable_variables(catalog.state(), &self.session)
            .map(|v| (v.name(), v.value(), v.description()))
            .collect::<Vec<_>>();
        rows.sort_by_cached_key(|(name, _, _)| name.to_lowercase());
        Ok(Coordinator::send_immediate_rows(
            rows.into_iter()
                .map(|(name, val, desc)| {
                    Row::pack_slice(&[
                        Datum::String(name),
                        Datum::String(&val),
                        Datum::String(desc),
                    ])
                })
                .collect(),
        ))
    }

    fn sequence_reset_variable(
        &self,
        catalog: &Catalog,
        plan: plan::ResetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let name = plan.name;
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level()?;
        }
        self.session
            .vars_mut()
            .reset(Some(catalog.system_config()), &name, false)?;
        Ok(ExecuteResponse::SetVariable { name, reset: true })
    }

    fn sequence_set_variable(
        &self,
        catalog: &Catalog,
        plan: plan::SetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let (name, local) = (plan.name, plan.local);
        if &name == TRANSACTION_ISOLATION_VAR_NAME {
            self.validate_set_isolation_level()?;
        }

        let vars = self.session.vars_mut();
        let values = match plan.value {
            plan::VariableValue::Default => None,
            plan::VariableValue::Values(values) => Some(values),
        };

        match values {
            Some(values) => {
                vars.set(
                    Some(catalog.system_config()),
                    &name,
                    VarInput::SqlSet(&values),
                    local,
                )?;

                // Database or cluster value does not correspond to a catalog item.
                if name.as_str() == DATABASE_VAR_NAME
                    && matches!(
                        catalog.resolve_database(vars.database()),
                        Err(CatalogError::UnknownDatabase(_))
                    )
                {
                    let name = vars.database().to_string();
                    self.session
                        .add_notice(AdapterNotice::DatabaseDoesNotExist { name });
                } else if name.as_str() == CLUSTER_VAR_NAME
                    && matches!(
                        catalog.resolve_cluster(vars.cluster()),
                        Err(CatalogError::UnknownCluster(_))
                    )
                {
                    let name = vars.cluster().to_string();
                    self.session
                        .add_notice(AdapterNotice::ClusterDoesNotExist { name });
                } else if name.as_str() == TRANSACTION_ISOLATION_VAR_NAME {
                    let v = values.into_first().to_lowercase();
                    if v == IsolationLevel::ReadUncommitted.as_str()
                        || v == IsolationLevel::ReadCommitted.as_str()
                        || v == IsolationLevel::RepeatableRead.as_str()
                    {
                        self.session
                            .add_notice(AdapterNotice::UnimplementedIsolationLevel {
                                isolation_level: v,
                            });
                    }
                }
            }
            None => vars.reset(Some(catalog.system_config()), &name, local)?,
        }

        Ok(ExecuteResponse::SetVariable { name, reset: false })
    }

    fn sequence_set_transaction(
        &self,
        catalog: &Catalog,
        plan: plan::SetTransactionPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // TODO(jkosh44) Only supports isolation levels for now.
        for mode in plan.modes {
            match mode {
                TransactionMode::AccessMode(_) => {
                    return Err(AdapterError::Unsupported("SET TRANSACTION <access-mode>"))
                }
                TransactionMode::IsolationLevel(isolation_level) => {
                    self.validate_set_isolation_level()?;

                    self.session.vars_mut().set(
                        Some(catalog.system_config()),
                        TRANSACTION_ISOLATION_VAR_NAME.as_str(),
                        VarInput::Flat(&isolation_level.to_ast_string_stable()),
                        plan.local,
                    )?
                }
            }
        }
        Ok(ExecuteResponse::SetVariable {
            name: TRANSACTION_ISOLATION_VAR_NAME.to_string(),
            reset: false,
        })
    }

    fn validate_set_isolation_level(&self) -> Result<(), AdapterError> {
        if self.session.transaction().contains_ops() {
            Err(AdapterError::InvalidSetIsolationLevel)
        } else {
            Ok(())
        }
    }

    /// Verify a portal is still valid.
    fn verify_portal(&self, catalog: &Catalog, name: &str) -> Result<(), AdapterError> {
        let portal = match self.session.get_portal_unverified(name) {
            Some(portal) => portal,
            None => return Err(AdapterError::UnknownCursor(name.to_string())),
        };
        if let Some(revision) = Coordinator::verify_statement_revision(
            catalog,
            &self.session,
            portal.stmt.as_ref(),
            &portal.desc,
            portal.catalog_revision,
        )? {
            let portal = self
                .session
                .get_portal_unverified_mut(name)
                .expect("known to exist");
            portal.catalog_revision = revision;
        }
        Ok(())
    }

    fn now(&self) -> EpochMillis {
        (self.inner().now)()
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    /// Starts a transaction based on implicit:
    /// - `None`: InTransaction
    /// - `Some(1)`: Started
    /// - `Some(n > 1)`: InTransactionImplicit
    /// - `Some(0)`: no change
    pub fn start_transaction(&mut self, implicit: Option<usize>) -> Result<(), AdapterError> {
        let now = self.now_datetime();
        let result = match implicit {
            None => self.session.start_transaction(now, None, None),
            Some(stmts) => {
                self.session.start_transaction_implicit(now, stmts);
                Ok(())
            }
        };
        result
    }

    /// Ends a transaction.
    pub async fn end_transaction(
        &mut self,
        action: EndTransactionAction,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.from_session_response(
            self.send(|tx| Command::Commit {
                action,
                session: self.session.metadata(),
                tx,
                otel_ctx: OpenTelemetryContext::obtain(),
            })
            .await,
        )
    }

    /// Fails a transaction.
    pub fn fail_transaction(&mut self) {
        self.session.fail_transaction();
    }

    /// Fetches the catalog.
    pub async fn catalog_snapshot(&self) -> Result<Arc<Catalog>, AdapterError> {
        Ok(self
            .send(|tx| Command::CatalogSnapshot {
                if_more_recent: None,
                tx,
            })
            .await?
            .catalog
            .expect("must exist"))
    }

    /// Allocates a user id.
    pub async fn allocate_user_id(&self) -> Result<GlobalId, AdapterError> {
        self.send(|tx| Command::AllocateUserId { tx }).await
    }

    /// Dumps the catalog to a JSON string.
    pub async fn dump_catalog(&self) -> Result<CatalogDump, AdapterError> {
        Ok(self.catalog_snapshot().await?.dump()?)
    }

    /// Tells the coordinator a statement has finished execution, in the cases
    /// where we have no other reason to communicate with the coordinator.
    pub fn retire_execute(
        &mut self,
        data: ExecuteContextExtra,
        reason: StatementEndedExecutionReason,
    ) {
        if !data.is_trivial() {
            let cmd = Command::RetireExecute { data, reason };
            self.inner().send(cmd);
        }
    }

    /// Inserts a set of rows into the given table.
    ///
    /// The rows only contain the columns positions in `columns`, so they
    /// must be re-encoded for adding the default values for the remaining
    /// ones.
    pub async fn insert_rows(
        &mut self,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
        ctx_extra: ExecuteContextExtra,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.from_session_response(
            self.send(|tx| Command::CopyRows {
                id,
                columns,
                rows,
                session: self.session.metadata(),
                tx,
                ctx_extra,
            })
            .await,
        )
    }

    fn from_session_response(
        &mut self,
        resp: Result<SessionResponse, AdapterError>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let SessionResponse { response, changes } = resp?;
        for change in changes {
            match change {
                crate::session::SessionChange::Notice(notice) => self.session.add_notice(notice),
            }
        }
        Ok(response)
    }

    /// Gets the current value of all system variables.
    pub async fn get_system_vars(&mut self) -> Result<GetVariablesResponse, AdapterError> {
        self.send(|tx| Command::GetSystemVars {
            session: self.session.metadata(),
            tx,
        })
        .await
    }

    /// Updates the specified system variables to the specified values.
    pub async fn set_system_vars(
        &mut self,
        vars: BTreeMap<String, String>,
    ) -> Result<(), AdapterError> {
        self.send(|tx| Command::SetSystemVars {
            vars,
            session: self.session.metadata(),
            tx,
        })
        .await
    }

    /// Terminates the client session.
    pub async fn terminate(&mut self) {
        let res = self
            .send(|tx| Command::Terminate {
                session: self.session.metadata(),
                tx: Some(tx),
            })
            .await;
        if let Err(e) = res {
            // Nothing we can do to handle a failed terminate so we just log and ignore it.
            error!("Unable to terminate session: {e:?}");
        }
        // Prevent any communication with Coordinator after session is terminated.
        self.inner = None;
    }

    /// Returns a mutable reference to the session bound to this client.
    pub fn session(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Returns a reference to the inner client.
    pub fn inner(&self) -> &Client {
        self.inner.as_ref().expect("inner invariant violated")
    }

    async fn send<T, F>(&self, f: F) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>) -> Command,
    {
        self.send_with_cancel(f, futures::future::pending()).await
    }

    async fn send_with_cancel<T, F>(
        &self,
        f: F,
        cancel_future: impl Future<Output = std::io::Error> + Send,
    ) -> Result<T, AdapterError>
    where
        F: FnOnce(oneshot::Sender<Response<T>>) -> Command,
    {
        let mut typ = None;
        let application_name = self.session.application_name();
        let name_hint = ApplicationNameHint::from_str(application_name);
        let (tx, mut rx) = oneshot::channel();
        self.inner().send({
            let cmd = f(tx);
            // Measure the success and error rate of certain commands:
            // - declare reports success of SQL statement planning
            // - execute reports success of dataflow execution
            match cmd {
                Command::AppendWebhook { .. } => typ = Some("webhook"),
                Command::Startup { .. }
                | Command::Commit { .. }
                | Command::CancelRequest { .. }
                | Command::PrivilegedCancelRequest { .. }
                | Command::CopyRows { .. }
                | Command::GetSystemVars { .. }
                | Command::SetSystemVars { .. }
                | Command::Terminate { .. }
                | Command::RetireExecute { .. } => {}
                Command::CatalogSnapshot { .. } => {}
                Command::AllocateUserId { .. } => {}
            };
            cmd
        });

        let mut cancel_future = pin::pin!(cancel_future);
        let mut cancelled = false;
        loop {
            tokio::select! {
                res = &mut rx => {
                    let res = res.expect("sender dropped");
                    let status = if res.result.is_ok() {
                        "success"
                    } else {
                        "error"
                    };
                    if let Some(typ) = typ {
                        self.inner()
                            .metrics
                            .commands
                            .with_label_values(&[typ, status, name_hint.as_str()])
                            .inc();
                    }
                    return res.result
                },
                _err = &mut cancel_future, if !cancelled => {
                    cancelled = true;
                    self.inner().send(Command::PrivilegedCancelRequest {
                        session: self.session.metadata(),
                    });
                }
            };
        }
    }

    pub fn add_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        let timeout_dur = session.vars().idle_in_transaction_session_timeout();
        if !timeout_dur.is_zero() {
            let timeout_dur = timeout_dur.clone();
            if let Some(txn) = session.transaction().inner() {
                let txn_id = txn.id.clone();
                let timeout = TimeoutType::IdleInTransactionSession(txn_id);
                self.timeouts.add_timeout(timeout, timeout_dur);
            }
        }
    }

    pub fn remove_idle_in_transaction_session_timeout(&mut self) {
        let session = self.session();
        if let Some(txn) = session.transaction().inner() {
            let txn_id = txn.id.clone();
            self.timeouts
                .remove_timeout(&TimeoutType::IdleInTransactionSession(txn_id));
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    pub async fn recv_timeout(&mut self) -> Option<TimeoutType> {
        self.timeouts.recv().await
    }
}

impl Drop for SessionClient {
    fn drop(&mut self) {
        // We may not have a session if this client was dropped while awaiting
        // a response. In this case, it is the coordinator's responsibility to
        // terminate the session.

        // TODO: Do we need to care about ^?
        // We may not have a connection to the Coordinator if the session was
        // prematurely terminated, for example due to a timeout.
        if let Some(inner) = &self.inner {
            inner.send(Command::Terminate {
                session: self.session.metadata(),
                tx: None,
            })
        }
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum TimeoutType {
    IdleInTransactionSession(TransactionId),
}

impl Display for TimeoutType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutType::IdleInTransactionSession(txn_id) => {
                writeln!(f, "Idle in transaction session for transaction '{txn_id}'")
            }
        }
    }
}

impl From<TimeoutType> for AdapterError {
    fn from(timeout: TimeoutType) -> Self {
        match timeout {
            TimeoutType::IdleInTransactionSession(_) => {
                AdapterError::IdleInTransactionSessionTimeout
            }
        }
    }
}

struct Timeout {
    tx: mpsc::UnboundedSender<TimeoutType>,
    rx: mpsc::UnboundedReceiver<TimeoutType>,
    active_timeouts: BTreeMap<TimeoutType, AbortOnDropHandle<()>>,
}

impl Timeout {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Timeout {
            tx,
            rx,
            active_timeouts: BTreeMap::new(),
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a
    /// `tokio::select!` statement and some other branch
    /// completes first, it is guaranteed that no messages were received on this
    /// channel.
    ///
    /// <https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety>
    async fn recv(&mut self) -> Option<TimeoutType> {
        self.rx.recv().await
    }

    fn add_timeout(&mut self, timeout: TimeoutType, duration: Duration) {
        let tx = self.tx.clone();
        let timeout_key = timeout.clone();
        let handle = mz_ore::task::spawn(|| format!("{timeout_key}"), async move {
            tokio::time::sleep(duration).await;
            let _ = tx.send(timeout);
        })
        .abort_on_drop();
        self.active_timeouts.insert(timeout_key, handle);
    }

    fn remove_timeout(&mut self, timeout: &TimeoutType) {
        self.active_timeouts.remove(timeout);

        // Remove the timeout from the rx queue if it exists.
        let mut timeouts = Vec::new();
        while let Ok(pending_timeout) = self.rx.try_recv() {
            if timeout != &pending_timeout {
                timeouts.push(pending_timeout);
            }
        }
        for pending_timeout in timeouts {
            self.tx.send(pending_timeout).expect("rx is in this struct");
        }
    }
}

/// A wrapper around an UnboundedReceiver of PeekResponseUnary that records when it sees the
/// first row data in the given histogram
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RecordFirstRowStream {
    #[derivative(Debug = "ignore")]
    pub rows: Box<dyn Stream<Item = PeekResponseUnary> + Unpin + Send + Sync>,
    pub execute_started: Instant,
    pub time_to_first_row_seconds: Histogram,
    saw_rows: bool,
}

impl RecordFirstRowStream {
    /// Create a new [`RecordFirstRowStream`]
    pub fn new(
        rows: Box<dyn Stream<Item = PeekResponseUnary> + Unpin + Send + Sync>,
        execute_started: Instant,
        client: &SessionClient,
    ) -> Self {
        let histogram = Self::histogram(client);
        Self {
            rows,
            execute_started,
            time_to_first_row_seconds: histogram,
            saw_rows: false,
        }
    }

    fn histogram(client: &SessionClient) -> Histogram {
        let isolation_level = *client.session.vars().transaction_isolation();

        client
            .inner()
            .metrics()
            .time_to_first_row_seconds
            .with_label_values(&[isolation_level.as_str()])
    }

    /// If you want to match [`RecordFirstRowStream`]'s logic but don't need
    /// a UnboundedReceiver, you can tell it when to record an observation.
    pub fn record(execute_started: Instant, client: &SessionClient) {
        Self::histogram(client).observe(execute_started.elapsed().as_secs_f64());
    }

    pub async fn recv(&mut self) -> Option<PeekResponseUnary> {
        let msg = self.rows.next().await;
        if !self.saw_rows && matches!(msg, Some(PeekResponseUnary::Rows(_))) {
            self.saw_rows = true;
            self.time_to_first_row_seconds
                .observe(self.execute_started.elapsed().as_secs_f64());
        }
        msg
    }
}
