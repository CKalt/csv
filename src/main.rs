use std::error::Error;
use std::time::SystemTime;
use chrono::{DateTime,Utc};
use csv::Reader;
use postgres::error::Error as PgError;

mod cfg;
mod db;

use cfg::Config;

fn example(file_name: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = Reader::from_path(file_name)?;
    for result in rdr.records() {
        let record = result?;
        println!("{:?}", record);
    }
    Ok(())
}

fn main() -> Result<(), PgError> {
    let cfg = Config::new();
    println!("{:?}", cfg);

            let query = "select memo,
                            import_ts,
                            import_tz
                        from foo";

    let mut client =
        match db::connect_db(&cfg) {
            Ok(clnt) => clnt,
            Err(_) => panic!("no connect"),
        };

    let rows = client.query(query, &[])?;
    for row in rows.iter() {
        let memo: String = row.get(0);
        // Systime required for timestamp ( without time zone )
        let import_ts: SystemTime       = row.get(1);
        let import_ts: DateTime<Utc>    = import_ts.into();
        // Datetime required for timestamp with time zone
        let import_tz: DateTime<Utc>    = row.get(2);

        println!("memo = {}, import_ts = {}, import_tz = {}",
            memo,
            import_ts.format("%m/%d/%Y %T"),
            import_tz.format("%m/%d/%Y %T"));
    }

    for fname in cfg.opt.file_names.iter() {
        println!("file: {}", fname);

        if true {
            if let Err(err) = example(&fname) {
                println!("error running example: {}", err);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

