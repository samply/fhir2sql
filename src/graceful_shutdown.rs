use tracing::info;

pub async fn wait_for_signal() {
    use tokio::signal::unix::{signal,SignalKind};
    let mut sigterm = signal(SignalKind::terminate())
        .expect("Unable to register shutdown handler");
    let mut sigint = signal(SignalKind::interrupt())
        .expect("Unable to register shutdown handler");
    let signal = tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT"
    };
    // The following does not print in docker-compose setups but it does when run individually.
    // Probably a docker-compose error.
    info!("Received signal ({signal}) - shutting down gracefully.");
}
