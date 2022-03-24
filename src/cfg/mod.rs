use structopt::StructOpt;
use serde::{Serialize, Deserialize};
use std::env;
use std::path::PathBuf;

////// opt ///////////

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "systime", about = "Experiments with system and db timestamps.")]
pub struct Opt {
    /// File name: input csv file to be read and parsed.
    #[structopt(name = "FILE")]
    pub file_names: Vec<String>,
    /// Set config-file.
    #[structopt(short = "f", long = "config-file")]
    pub config_file: Option<String>,
}

////// config //////
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub postgresql: Postgresql,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub cf: ConfigFile,
    pub opt: Opt,
}

impl Config {
    pub fn new() -> Self {
        let opt = Opt::from_args();

        let config_path = Self::config_file_path(&opt)
            .expect("Couldn't get config file path");

        let config_text =
            match std::fs::read_to_string(&config_path) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("Unable to read config file {}:\n\
                        error= {:?}",
                        config_path.display(), e);
                    std::process::exit(0);
                }
            };

        let cf: ConfigFile = toml::from_str(&config_text).unwrap();
        Config {
            cf, opt
        }
    }
    fn config_file_path(opt: &Opt) -> Result<PathBuf, std::io::Error> {
        match opt.config_file {
            None => {
                let exe = env::current_exe()?;
                let dir = exe.parent().expect(
                    "Executable must be in some directory");
                let mut dir = dir.join("");
                dir.pop();
                dir.pop();
                dir.push("config.toml");
                Ok(dir)
            },
            Some(ref config_file) => {
                let path = std::fs::canonicalize(config_file);
                match path {
                    Ok(ref path) => {
                        println!("config file canonicalized path = {}",
                                path.display());
                    },
                    Err(ref e) =>
                        println!(
                            "oops got error = {:?} calling canonicalize on={}",
                            e, config_file),
                }
                path
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Postgresql {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

