//! Backend-specific connection URL validation performed before startup.

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgSslMode};

pub fn postgres_connect_options(raw: &str) -> Result<PgConnectOptions, &'static str> {
    let query = raw
        .split_once('?')
        .map(|(_, query)| query)
        .ok_or("PostgreSQL URL must specify sslmode=verify-full or sslmode=disable")?;
    let mut explicit_ssl_mode = None;
    for parameter in query.split('&') {
        let (name, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        if name == "sslmode" && explicit_ssl_mode.replace(value).is_some() {
            return Err("PostgreSQL URL must contain exactly one sslmode parameter");
        }
    }
    let ssl_mode = match explicit_ssl_mode {
        Some("verify-full") => PgSslMode::VerifyFull,
        Some("disable") => PgSslMode::Disable,
        Some(_) => return Err("PostgreSQL sslmode must be verify-full or disable"),
        None => return Err("PostgreSQL URL must specify sslmode=verify-full or sslmode=disable"),
    };
    let options: PgConnectOptions = raw
        .parse()
        .map_err(|_| "invalid PostgreSQL URL or connection option")?;
    Ok(options.ssl_mode(ssl_mode).disable_statement_logging())
}

#[cfg(test)]
mod tests {
    use super::postgres_connect_options;

    #[test]
    fn postgres_url_requires_an_explicit_supported_tls_mode_and_valid_options() {
        assert!(postgres_connect_options("postgres://localhost/iot?sslmode=verify-full").is_ok());
        assert!(postgres_connect_options("postgres://localhost/iot?sslmode=disable").is_ok());
        assert!(postgres_connect_options("postgres://localhost/iot").is_err());
        assert!(postgres_connect_options("postgres://localhost/iot?sslmode=prefer").is_err());
        assert!(postgres_connect_options("postgres://localhost/iot?sslmode=typo").is_err());
        assert!(
            postgres_connect_options(
                "postgres://localhost/iot?sslmode=disable&sslmode=verify-full"
            )
            .is_err()
        );
        assert!(
            postgres_connect_options(
                "postgres://localhost/iot?sslmode=verify-full&port=not-a-port"
            )
            .is_err()
        );
    }
}
