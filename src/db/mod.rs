use postgres::{error::Error, Client, NoTls};
use crate::Config;

pub fn connect_db(cfg: &Config) -> Result<Client, Error> {
    let cf = &cfg.cf;
    let connect_str =
            format!(
                "postgres://{}{}{}@{}{}{}{}{}",
                cf.postgresql.username,
                if cf.postgresql.password.is_empty() { "" } else { ":" },
                cf.postgresql.password,
                cf.postgresql.host,
                if cf.postgresql.port.is_empty() { "" } else { ":" },
                cf.postgresql.port,
                if cf.postgresql.database.is_empty() { "" } else { "/" },
                cf.postgresql.database
            );
    Client::connect(&connect_str, NoTls)
}

