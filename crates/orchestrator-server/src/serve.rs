//! The server binary's wiring (decision 0011, steps 7 and 8): the
//! migrations, then on one listener
//!
//! - the engine port and the core's calls (the drain, the removal and,
//!   with a flow for the core's chains, `POST /internal/chains` from the
//!   core's machine alone) on the module's route of the `internal` entry
//!   point, served both at `/internal/…` and under the
//!   route prefix `/modules/orchestrator` (quality-sdk decision 0002), so
//!   they answer whether or not Traefik strips it;
//! - the people's API under the web route (`/orchestrator/api/v1/…`),
//!   which Traefik passes as is;
//!
//! and beside them the scheduler (keeping and firing the core's schedules
//! when that flow is set), the outbox sender, the watch of
//! `core.iam.changed` and the metrics file. The engine port and the
//! people's API are not reachable from each other's entry point: the web
//! route passes only `/orchestrator/…`.
//!
//! On shutdown the listener stops taking calls and ends those in flight,
//! then the loops stop; each of their steps can run again after a crash.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use orchestrator_postgres::PgStore;
use tokio::task::JoinSet;

use crate::{
    chains_router, core_router, counted, people_router, router, send_outbox, watch_iam_changes,
    CachedActions, Clock, CoreCaller, CoreEvents, CoreToken, EnginePort, Expected,
    HttpCoreSchedules, HttpEffectiveActions, HttpEvents, HttpMachineList, HttpModuleList,
    InternalCaller, JwksKeys, Metrics, OidcPeople, People, PeopleState, PortState, Scheduler,
    Settings, SystemClock, TraefikMachine, ORCHESTRATOR_MODULE,
};

/// The web route people reach the orchestrator by (its manifest's
/// `route.path`).
pub const WEB_ROUTE: &str = "/orchestrator";

/// The module's route prefix on the `internal` entry point.
pub const INTERNAL_ROUTE: &str = "/modules/orchestrator";

/// How often the scheduler ticks and the outbox is sent.
#[derive(Debug, Clone, Copy)]
pub struct Every {
    pub tick: Duration,
    pub outbox: Duration,
    /// After the core did not answer a read of `core.iam.changed`.
    pub iam_retry: Duration,
}

impl Default for Every {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(10),
            outbox: Duration::from_secs(5),
            iam_retry: Duration::from_secs(10),
        }
    }
}

/// Runs the server until `shutdown` completes. `http` reaches the core
/// and its provider (with the machine's certificate in production).
pub async fn serve(
    settings: Settings,
    http: reqwest::Client,
    every: Every,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let pool = orchestrator_postgres::connect(settings.database.clone(), 10)
        .await
        .map_err(|e| format!("the database: {e}"))?;
    orchestrator_postgres::migrate(&pool)
        .await
        .map_err(|e| format!("the migrations: {e}"))?;
    let store = PgStore::new(pool);
    let metrics = metrics();
    let token = Arc::new(CoreToken::new(
        http.clone(),
        &settings.issuer,
        settings.machine_client.clone(),
    ));
    let base = settings.core_base.as_str();
    let actions = Arc::new(CachedActions::new(
        HttpEffectiveActions::new(http.clone(), base, Arc::clone(&token)),
        SystemClock,
    ));
    let events: Arc<dyn CoreEvents> =
        Arc::new(HttpEvents::new(http.clone(), base, Arc::clone(&token)));
    let people = people(&http, &settings, actions.clone());
    let mut port = EnginePort::new(store.clone(), SystemClock)
        .with_machines(Arc::new(HttpMachineList::new(
            http.clone(),
            base,
            Arc::clone(&token),
        )))
        .with_sealer(Arc::new(settings.sealer))
        .with_metrics(Arc::clone(&metrics));
    let mut scheduler = Scheduler::new(store.clone(), SystemClock)
        .with_modules(Arc::new(HttpModuleList::new(
            http.clone(),
            base,
            Arc::clone(&token),
        )))
        .with_metrics(Arc::clone(&metrics));
    if let Some(flow) = settings.core_flow {
        let schedules = HttpCoreSchedules::new(http.clone(), base, token);
        scheduler = scheduler.with_core_schedules(Arc::new(schedules), flow.clone());
        port = port.with_core_flow(flow);
    } else {
        tracing::warn!("no ORCHESTRATOR_CORE_FLOW: the core's schedules and chains wait");
    }
    let engines = InternalCaller::new(
        JwksKeys::new(http.clone(), &settings.issuer),
        expected(&settings.issuer, ORCHESTRATOR_MODULE),
        SystemClock,
    );
    let core = CoreCaller::new(
        JwksKeys::new(http.clone(), &settings.issuer),
        expected(&settings.issuer, ORCHESTRATOR_MODULE),
        SystemClock,
    );
    let app = app(&port, Arc::new(engines), Arc::new(core), people, metrics);

    let mut loops = JoinSet::new();
    loops.spawn(ticks(scheduler, every.tick));
    loops.spawn(outbox(store, Arc::clone(&events), every.outbox));
    loops.spawn(watch_iam_changes(events, actions, every.iam_retry));
    let listener = tokio::net::TcpListener::bind(settings.listen)
        .await
        .map_err(|e| format!("{}: {e}", settings.listen))?;
    tracing::info!(listen = %settings.listen, "the orchestrator serves");
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| e.to_string());
    loops.shutdown().await;
    tracing::info!("the orchestrator stopped");
    served
}

/// The metrics, written to the metrics file when there is one.
fn metrics() -> Arc<Metrics> {
    let mut registry = prometheus_client::registry::Registry::default();
    let metrics = Metrics::register(&mut registry);
    if let Some(file) = crate::metrics_file(registry) {
        file.spawn();
    }
    metrics
}

/// What a valid token of `audience` from the provider `issuer` says.
fn expected(issuer: &str, audience: &str) -> Expected {
    Expected {
        issuer: issuer.into(),
        audience: audience.into(),
    }
}

/// The people's check: a person's token of the people's client, and the
/// actions the core says they hold, cached.
fn people(
    http: &reqwest::Client,
    settings: &Settings,
    actions: Arc<dyn crate::EffectiveActions>,
) -> People {
    let tokens = OidcPeople::new(
        JwksKeys::new(http.clone(), &settings.issuer),
        expected(&settings.issuer, &settings.people_client),
        SystemClock,
    );
    People::new(Arc::new(tokens), actions)
}

/// Every route on one router: the engine port (`engines` reads the
/// machine and the engine module's token), the core's calls (the machine
/// alone; a chain, `core` reads the core's machine and its own token) and
/// the people's API, counted by route template.
fn app<C: Clock + Clone>(
    port: &EnginePort<C>,
    engines: Arc<dyn crate::Authenticate>,
    core: Arc<dyn crate::Authenticate>,
    people: People,
    metrics: Arc<Metrics>,
) -> Router {
    let engine_state = Arc::new(PortState {
        port: port.clone(),
        auth: engines,
    });
    let core_state = Arc::new(PortState {
        port: port.clone(),
        auth: Arc::new(TraefikMachine),
    });
    let chains_state = Arc::new(PortState {
        port: port.clone(),
        auth: core,
    });
    let internal = router(engine_state)
        .merge(core_router(core_state))
        .merge(chains_router(chains_state));
    let people = people_router(Arc::new(PeopleState {
        port: port.clone(),
        people,
    }));
    let app = Router::new()
        .merge(internal.clone())
        .nest(INTERNAL_ROUTE, internal)
        .nest(WEB_ROUTE, people);
    counted(app, metrics)
}

async fn ticks<C: Clock + Clone>(scheduler: Scheduler<C>, every: Duration) {
    let mut interval = tokio::time::interval(every);
    loop {
        interval.tick().await;
        if let Err(e) = scheduler.tick().await {
            tracing::error!(error = %e, "the scheduler's tick failed; again at the next");
        }
    }
}

async fn outbox(store: PgStore, events: Arc<dyn CoreEvents>, every: Duration) {
    let mut interval = tokio::time::interval(every);
    loop {
        interval.tick().await;
        match send_outbox(&store, &*events, SystemClock.now()).await {
            Ok(done) if done.sent + done.refused > 0 => {
                tracing::info!(sent = done.sent, refused = done.refused, "events sent");
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "the outbox was not read; again later"),
        }
    }
}

/// Completes on SIGTERM (Docker's stop) or Ctrl-C.
pub async fn stop_signal() {
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!("no SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        () = term => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    tracing::info!("stopping: calls in flight end first");
}
