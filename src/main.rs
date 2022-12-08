use std::time::Duration;

use clap::{command, Parser, Subcommand};
use figment::{providers::Env, Figment};
use log::{debug, error};
use reqwest::ClientBuilder;
use serde::Deserialize;
use sqlx::mysql::MySqlPoolOptions;
use tokio::{sync::mpsc, time};

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start HTTP.
    Http(HttpArgs),
    /// Start DB.
    Db(DbArgs),
}

#[derive(Parser, Debug)]
struct HttpArgs {
    /// Set a timeout for only the connect phase of a `Client`.
    #[arg(long, default_value_t = 15)]
    connect_timeout_ms: u64,

    /// Enables a request timeout.
    #[arg(long, default_value_t = 20)]
    timeout_ms: u64,

    /// Set a timeout for idle sockets being kept-alive.
    /// The default is set to effectively have no idle connections in the pool.
    #[arg(long, default_value_t = 1)]
    pool_idle_timeout_us: u64,

    /// Sets the maximum idle connection per host allowed in the pool.
    /// The default is set to effectively have no idle connections in the pool.
    #[arg(long, default_value_t = 1)]
    pool_max_idle_per_host: usize,

    /// URL to send request to.
    #[arg(long)]
    url: String,

    /// Interval of sending requests.
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,

    /// Number of workers to run in parallel.
    #[arg(long, default_value_t = 1)]
    parallel: usize,
}

#[derive(Parser, Debug)]
struct DbArgs {
    /// Set a timeout for idle connections being kept-alive.
    /// The default is set to effectively have no idle connections in the pool.
    #[arg(long, default_value_t = 1)]
    pool_idle_timeout_us: u64,

    /// Set a max lifetime for connections.
    /// The default is set to effectively have no idle connections in the pool.
    #[arg(long, default_value_t = 1)]
    pool_max_lifetime_us: u64,

    /// Set a timeout for only the connect phase of a connection.
    #[arg(long, default_value_t = 15)]
    connect_timeout_ms: u64,

    /// Interval of sending requests.
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Cli::parse();

    match args.command {
        Commands::Http(args) => http_main(args).await,
        Commands::Db(args) => db_main(args).await,
    }
}

#[derive(Deserialize, Debug)]
struct DbConfig {
    host: String,
    port: u64,
    username: String,
    password: String,
    database: String,
}

async fn db_main(args: DbArgs) {
    let db_config: DbConfig = Figment::new()
        .merge(Env::prefixed("DB_"))
        .extract()
        .expect("error extracting DB config");

    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .idle_timeout(Duration::from_micros(args.pool_idle_timeout_us))
        .max_lifetime(Duration::from_micros(args.pool_max_lifetime_us))
        .acquire_timeout(Duration::from_millis(args.connect_timeout_ms))
        .after_connect(|_, _| {
            Box::pin(async {
                debug!("connection created");
                Ok(())
            })
        })
        .connect_lazy(&format!(
            "mysql://{}:{}@{}:{}/{}",
            db_config.username,
            db_config.password,
            db_config.host,
            db_config.port,
            db_config.database,
        ))
        .expect("error building pool");

    let mut interval = time::interval(Duration::from_millis(args.interval_ms));
    loop {
        interval.tick().await;

        match pool.acquire().await {
            Ok(conn) => {
                let _ = conn.detach();
            }
            Err(e) => {
                error!(
                    "connection acquire error: {}. connect_timeout={}ms",
                    e, args.connect_timeout_ms
                )
            }
        }
    }
}

async fn http_main(args: HttpArgs) {
    // Create a client for every worker so that they do not benefit from pooling
    let clients: Vec<_> = (0..args.parallel)
        .into_iter()
        .map(|_| {
            ClientBuilder::new()
                .pool_idle_timeout(Duration::from_micros(args.pool_idle_timeout_us))
                .pool_max_idle_per_host(args.pool_max_idle_per_host)
                .connect_timeout(Duration::from_millis(args.connect_timeout_ms))
                .timeout(Duration::from_millis(args.timeout_ms))
                .connection_verbose(true)
                .build()
                .expect("error building client")
        })
        .collect();

    let (send, mut recv) = mpsc::channel::<()>(1);

    for client in clients.iter().take(args.parallel) {
        let url = args.url.clone();
        let client = client.clone();
        let done = send.clone();

        tokio::spawn(async move {
            let _done = done;
            let mut interval = time::interval(Duration::from_millis(args.interval_ms));

            loop {
                interval.tick().await;

                match client.get(&url).send().await {
                    Ok(_) => {}
                    Err(e) => error!(
                        "request error: {}. connect_timeout={}ms timeout={}ms",
                        e, args.connect_timeout_ms, args.timeout_ms
                    ),
                }
            }
        });
    }

    drop(send);
    let _ = recv.recv().await;
}
