use std::fmt;
use std::fs;
use std::path::Path;
use std::io::{self, Write};
use std::time::SystemTime;
use chrono::DateTime;
use csv::Reader;
use postgres::Client;
use serde::{Serialize, Deserialize};

mod cfg;
mod db;
mod systime_fmt;

use cfg::Config;

#[derive(Debug)]
struct AppError {
    kind: String,     // type of the error
    message: String, // error message
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.kind, self.message)
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for AppError {
    fn from(error: io::Error) -> Self {
        AppError {
            kind: String::from("io"),
            message: error.to_string(),
        }
    }
}

// Implement std::convert::From for AppError; from csv::Error
impl From<csv::Error> for AppError {
    fn from(error: csv::Error) -> Self {
        AppError {
            kind: String::from("csv"),
            message: error.to_string(),
        }
    }
}


// Implement std::convert::From for AppError; from postgres::Error
impl From<postgres::Error> for AppError {
    fn from(error: postgres::Error) -> Self {
        AppError {
            kind: String::from("postgres"),
            message: error.to_string(),
        }
    }
}

// TP1: StringRecord(["RoundId", "PlayerId", "PlayerName", "BallId", "Score", "HoleId", "HoleScore", "Start", "End"])
#[allow(dead_code)]
#[allow(non_snake_case)]
#[derive(Serialize,Deserialize,Debug)]
struct ScoreCardDetailCsv {
    RoundId: i32,
    PlayerId: i32,
    PlayerName: String,
    BallId: i32,
    Score: i16,
    HoleId: i16,
    HoleScore: i16,
    #[serde(with = "systime_fmt")]
    Start: SystemTime,
    #[serde(with = "systime_fmt")]
    End: SystemTime,
}

#[derive(Debug)]
struct ScoreCard {
    score_card_id:          Option<i32>,
    file_path:              String,
    base_file_name:         String,
    parsed_name_date:       SystemTime,
    file_create_time:       SystemTime,
    file_modified_time:     SystemTime,
    import_time:            Option<SystemTime>,
}
impl ScoreCard {
    fn new(file_path: &str) -> Result<Self, AppError> {
        let path = Path::new(file_path);
        let base_file_name: String =
            path.file_name().unwrap().to_str().unwrap().into();
        let parsed_name_date = Self::systime_from_file_name(&base_file_name)?;

        let md = fs::metadata(path)?;
        let file_path: String = file_path.into();
        let file_create_time = md.created()?;
        let file_modified_time = md.modified()?;
        let score_card_id = None;
        let import_time = None;

        let sc = ScoreCard {
            score_card_id,
            file_path,
            base_file_name,
            parsed_name_date,
            file_create_time,
            file_modified_time,
            import_time,
        };
        Ok(sc)
    }
    fn exists(&mut self, client: &mut Client) -> Result<bool, AppError> {
        let query = r#"
            SELECT c.score_card_id
              FROM score_card c
             WHERE c.file_path = $1
             LIMIT 1"#;

        match &client.query(query, &[&self.file_path])?.iter().nth(0) {
            Some(row) => {
                self.score_card_id = Some(row.get(0));
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn insert(&mut self, client: &mut Client) -> Result<(), AppError> {
        let query = r#"
            INSERT INTO score_card (
                    file_path,
                    parsed_name_date,
                    file_create_time,
                    file_modified_time
            )
            VALUES($1, $2, $3, $4)
            RETURNING score_card_id, import_time"#;


        let row = client.query_one(query, &[&self.file_path,
                    &self.parsed_name_date, &self.file_create_time,
                    &self.file_modified_time])?;

        self.score_card_id = row.get(0);
        self.import_time = row.get(1);
        Ok(())
    }
    fn date_tag_from_file_name(file_name: &str) -> Result<String,AppError> {
        if file_name.len() > 17 {
            Ok(file_name[11..17].to_string())
        } else {
            Err(
                AppError {
                    kind: "bad_name".into(),
                    message: "File name is too short".into(),
                }
            )
        }
    }
    fn systime_from_file_name(file_name: &str) -> Result<SystemTime,AppError> {
        let dt_str = format!("{}000000+0000",
            Self::date_tag_from_file_name(file_name)?);

        let dt = 
            match DateTime::parse_from_str(&dt_str, "%y%m%d%H%M%S%z") {
                Ok(dt) => dt,
                Err(_) =>
                    return Err(
                              AppError {
                                kind: "bad_name".into(),
                                message: "Invalid date in file_name".into(),
                            }),
            };

        Ok(SystemTime::from(dt))
    }
    fn import_csv_files(client: &mut Client, cfg: &Config)
                -> Result<(), AppError> {

        for file_path in cfg.opt.file_names.iter() {
            match ScoreCard::new(&file_path) {
                Ok(mut sc) => {
                    if sc.exists(client)? {
                        println!("{} already imported.", sc.base_file_name);
                    } else {
                        sc.insert(client)?;
                        sc.clear_score_card_detail(client)?;
                        println!("importing {}... ", sc.base_file_name);
                        sc.import_csv_file_detail(client)?;
                    }
                }
                Err(_) =>
                    println!("ERROR: Skipping bad file {}", file_path),
            }
        }

        Ok(())
    }
    fn clear_score_card_detail(&self, client: &mut Client)
        -> Result<u64, AppError> {

        let score_card_id = 
            match self.score_card_id {
                Some(s) => s,
                None => return 
                    Err(
                        AppError {
                            kind: "missing_value".into(),
                            message: "Clear score cards call w/ no id".into(),
                        }
                    ),
            };

        let query = r#"
            DELETE
              FROM score_card_detail
             WHERE score_card_id = $1"#;

        let row_count = client.execute(query, &[&score_card_id])?;

        Ok(row_count)
    }
    fn verify_header(headers: &csv::StringRecord, header_name: &str,
                header_index: usize) -> Result<(), AppError> {
        let header_value = headers.get(header_index);
        if header_value.is_none() ||
           header_value.unwrap().ne(header_name) {
            return Err(
                    AppError {
                        kind: "csv".into(),
                        message: format!(
                            "{} heading is missing or in wrong place",
                                header_name),
                    });
        }
        Ok(())
    }
    fn verify_headers(headers: &csv::StringRecord) -> Result<(), AppError> {
        let correct_headers = vec![
            "RoundId", "PlayerId", "PlayerName", "BallId", "Score", "HoleId",
            "HoleScore", "Start", "End"
        ];
        let num_headers = correct_headers.len();

        let got_num_headers = headers.len();
        if got_num_headers != 9 {
            return 
             Err(
                AppError {
                    kind: "csv".into(),
                    message: format!("Expected {} header names, got {}",
                        num_headers, got_num_headers),
                });
        }

        for (hdr_i, hdr) in correct_headers.iter().enumerate() {
            Self::verify_header(headers, hdr, hdr_i)?;
        }

        Ok(())
    }
    fn import_csv_file_detail(&self, client: &mut Client)
                -> Result<(), AppError> {
        let mut rdr = Reader::from_path(self.file_path.clone()).unwrap();
        let headers = rdr.headers()?;

        Self::verify_headers(&headers)?;

        let mut counter: i32 = 0;

        let mut stdout = std::io::stdout();
        for record in rdr.deserialize() {
            counter += 1;
            print!("\rInserting row {}", counter);
            stdout.flush().unwrap();
            let record: ScoreCardDetailCsv = record?;
            let mut scd = ScoreCardDetail::new(self, &record);
            scd.insert(client)?;
        }
        println!("");

        Ok(())
    }
}

#[allow(dead_code)]
struct ScoreCardDetail {
    score_card_detail_id:   Option<i32>,
    score_card_id:          i32,
    round_id:               i32,
    player_id:              i32,
    player_name:            String,
    ball_id:                i32,
    score:                  i16,
    hole_id:                i16,
    hole_score:             i16,
    start_time:             SystemTime,
    end_time:               SystemTime,
}

impl ScoreCardDetail {
    fn new(sc: &ScoreCard, csv: &ScoreCardDetailCsv) -> Self {
        Self {
            score_card_detail_id:   None,
            score_card_id:          sc.score_card_id.unwrap(),
            round_id:               csv.RoundId,
            player_id:              csv.PlayerId,
            player_name:            csv.PlayerName.to_owned(),
            ball_id:                csv.BallId,
            score:                  csv.Score,
            hole_id:                csv.HoleId,
            hole_score:             csv.HoleScore,
            start_time:             csv.Start,
            end_time:               csv.End,
        }
    }
    fn insert(&mut self, client: &mut Client) -> Result<(), AppError> {
        let query = r#"
            INSERT INTO score_card_detail (
                score_card_id,
                round_id,
                player_id,
                player_name,
                ball_id,
                score,
                hole_id,
                hole_score,
                start_time,
                end_time
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING score_card_detail_id"#;

        let row = client.query_one(query, &[
                    &self.score_card_id,
                    &self.round_id,
                    &self.player_id,
                    &self.player_name,
                    &self.ball_id,
                    &self.score,
                    &self.hole_id,
                    &self.hole_score,
                    &self.start_time,
                    &self.end_time,])?;
        self.score_card_detail_id = row.get(0);

        Ok(())
    }
}

fn main() -> Result<(), AppError> {
    let cfg = Config::new();
    let mut client = db::connect_db(&cfg)?;
    ScoreCard::import_csv_files(&mut client, &cfg)?;

    Ok(())
}
