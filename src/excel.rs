use calamine::{open_workbook, DataType, Reader, Xlsx};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{
    de::{Deserialize, DeserializeOwned, Error},
    Deserializer,
};

use std::{
    convert::AsRef,
    io::{Read, Seek},
    path::Path,
};

pub trait FromXlsx {
    fn from_xlsx_reader<RS>(reader: RS) -> Result<Vec<Self>, calamine::Error>
    where
        Self: Sized + DeserializeOwned,
        RS: Read + Seek,
    {
        Self::from_xlsx(Xlsx::new(reader)?)
    }

    fn from_xlsx_path<P>(path: P) -> Result<Vec<Self>, calamine::Error>
    where
        Self: Sized + DeserializeOwned,
        P: AsRef<Path>,
    {
        Self::from_xlsx(open_workbook(path)?)
    }

    fn from_xlsx<RS>(workbook: Xlsx<RS>) -> Result<Vec<Self>, calamine::Error>
    where
        Self: Sized + DeserializeOwned,
        RS: Read + Seek;
}

// Excel apparently considers 1900 to be a leap year
const NUM_DAYS_1900_01_01_FROM_CE: i32 = 693594;

pub mod excel_date {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data_type = DataType::deserialize(deserializer)?;
        match data_type {
            DataType::Float(f) | DataType::DateTime(f) => {
                let days = f.trunc() as i32;

                Ok(NaiveDate::from_num_days_from_ce(
                    days + NUM_DAYS_1900_01_01_FROM_CE,
                ))
            }
            x => Err(Error::custom(format!("invalid date: {:?}", x))),
        }
    }
}

pub mod excel_date_opt {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data_type = DataType::deserialize(deserializer)?;
        match data_type {
            DataType::String(s) => {
                if s.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::custom(format!("invalid date: {:?}", s)))
                }
            }
            DataType::Empty => Ok(None),
            DataType::Float(f) | DataType::DateTime(f) => {
                let days = f.trunc() as i32;

                Ok(Some(NaiveDate::from_num_days_from_ce(
                    days + NUM_DAYS_1900_01_01_FROM_CE,
                )))
            }
            x => Err(Error::custom(format!("invalid date: {:?}", x))),
        }
    }
}

pub mod excel_datetime {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data_type = DataType::deserialize(deserializer)?;
        match data_type {
            DataType::Float(f) | DataType::DateTime(f) => {
                let days = f.trunc() as i32;
                let time = f.fract() * 24.0 * 60.0 * 60.0;
                let secs = time.round() as u32;

                Ok(
                    NaiveDate::from_num_days_from_ce(days + NUM_DAYS_1900_01_01_FROM_CE)
                        .and_time(NaiveTime::from_num_seconds_from_midnight(secs, 0)),
                )
            }
            x => Err(Error::custom(format!("invalid datetime: {:?}", x))),
        }
    }
}

pub mod excel_datetime_opt {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data_type = DataType::deserialize(deserializer)?;
        match data_type {
            DataType::String(s) => {
                if s.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::custom(format!("invalid datetime: {:?}", s)))
                }
            }
            DataType::Empty => Ok(None),
            DataType::Float(f) | DataType::DateTime(f) => {
                let days = f.trunc() as i32;
                let time = f.fract() * 24.0 * 60.0 * 60.0;
                let secs = time.round() as u32;

                Ok(Some(
                    NaiveDate::from_num_days_from_ce(days + NUM_DAYS_1900_01_01_FROM_CE)
                        .and_time(NaiveTime::from_num_seconds_from_midnight(secs, 0)),
                ))
            }
            x => Err(Error::custom(format!("invalid datetime: {:?}", x))),
        }
    }
}
