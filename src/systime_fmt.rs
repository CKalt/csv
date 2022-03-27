use std::time::SystemTime;
use chrono::{Utc, TimeZone};
use serde::{self, Deserialize, Serializer, Deserializer};

//const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";
const FORMAT: &'static str = "%m/%d/%Y %r";

// The signature of a serialize_with function must follow the pattern:
//
//    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
//    where
//        S: Serializer
//
// although it may also be generic over the input types T.
pub fn serialize<S>(
    date: &SystemTime,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format!("{:?}", date);
    serializer.serialize_str(&s)
}

// The signature of a deserialize_with function must follow the pattern:
//
//    fn deserialize<'de, D>(D) -> Result<T, D::Error>
//    where
//        D: Deserializer<'de>
//
// although it may also be generic over the output types T.
pub fn deserialize<'de, D>(
    deserializer: D,
) -> Result<SystemTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    match Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom) {
        Ok(dt) => {
           // let dt: DateTime<Utc> = 
            let systime: SystemTime = dt.into();
            Ok(systime)
        },
        Err(e) => Err(e)
    }
}

