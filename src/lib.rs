use csv::StringRecord;
use serde::de::DeserializeOwned;

use std::io::Read;
use std::path::Path;

pub trait FromCsv {
    fn from_csv_reader<R>(reader: R) -> Result<Vec<Self>, csv::Error>
    where
        Self: Sized + DeserializeOwned,
        R: Read,
    {
        let mut rdr = csv::Reader::from_reader(reader);
        Ok(rdr
            .deserialize()
            .filter_map(|r| {
                r.map_err(|err| {
                    eprintln!("Failed deserializing record: {}", &err);
                    err
                })
                .ok()
            })
            .collect())
    }

    fn from_bytes(bytes: &Vec<u8>) -> Result<Vec<Self>, csv::Error>
    where
        Self: Sized + DeserializeOwned + std::fmt::Debug,
    {
        let mut rdr = csv::Reader::from_reader(bytes.as_slice());
        let byte_headers = rdr.byte_headers().ok().cloned();
        let string_headers = byte_headers
            .clone()
            .map(|h| StringRecord::from_byte_record(h).ok())
            .flatten();
        Ok(rdr
            .byte_records()
            .filter_map(|byte_record_r| {
                byte_record_r
                    .and_then(|byte_record| {
                        byte_record
                            .deserialize(byte_headers.as_ref())
                            .or_else(|err| {
                                eprintln!(
                                    "Failed deserializing record, attempting lossy: {}",
                                    &err
                                );

                                StringRecord::from_byte_record_lossy(byte_record)
                                    .deserialize(string_headers.as_ref())
                            })
                    })
                    .ok()
            })
            .collect())
    }

    fn from_csv<P>(path: P) -> Result<Vec<Self>, csv::Error>
    where
        Self: Sized + DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut rdr = csv::Reader::from_path(path)?;
        Ok(rdr
            .deserialize()
            .filter_map(|r| {
                r.map_err(|err| {
                    eprintln!("Failed deserializing record: {}", &err);
                    err
                })
                .ok()
            })
            .collect())
    }

    fn from_tsv_reader<R>(reader: R) -> Result<Vec<Self>, crate::Error>
    where
        Self: Sized + DeserializeOwned,
        R: Read,
    {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(reader);
        Ok(rdr
            .deserialize()
            .filter_map(|r| {
                r.map_err(|err| {
                    eprintln!("Failed deserializing record: {:?}", &err);
                    err
                })
                .ok()
            })
            .collect())
    }
}

pub mod zero_one_bool {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_ref() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => Err(serde::de::Error::custom("Not one or zero")),
        }
    }

    pub fn serialize<S>(val: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match val {
            true => "1",
            false => "0",
        })
    }
}

pub mod yes_no_bool {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_ref() {
            "Yes" => Ok(true),
            "No" => Ok(false),
            _ => Err(serde::de::Error::custom("Not yes or no")),
        }
    }

    pub fn serialize<S>(val: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match val {
            true => "Yes",
            false => "No",
        })
    }
}

pub mod non_null_bool {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_ref() {
            "" | "NULL" | "0" => Ok(false),
            _ => Ok(true),
        }
    }

    pub fn serialize<S>(val: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match val {
            true => "1",
            false => "0",
        })
    }
}

pub mod zero_one_int_bool {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = i32::deserialize(deserializer)?;
        match i {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(serde::de::Error::custom("Not one or zero")),
        }
    }
}

pub mod nullable_bool {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_ref() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        })
    }

    pub fn serialize<S>(val: &Option<bool>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match val {
            Some(true) => "1",
            Some(false) => "0",
            _ => "",
        })
    }

    pub fn default_true() -> Option<bool> {
        Some(true)
    }
    pub fn default_false() -> Option<bool> {
        Some(false)
    }
}

pub mod nullable_string {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_ref() {
            "NULL" | "" => None,
            x => Some(x.to_string()),
        })
    }
}

pub mod semi_separated_list {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        Ok(s.split(";").map(|s| s.to_owned()).collect())
    }
}

pub mod timeless_mm_dd_yyyy_date {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod mm_dd_yyyy_date {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDate::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod mm_dd_yyyy_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod mm_dd_yyyy_date_opt {
    use chrono::NaiveDate;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NaiveDate::parse_from_str(&s, FORMAT).ok())
    }
}

pub mod mm_dd_yyyy_datetime_opt {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NaiveDateTime::parse_from_str(&s, FORMAT).ok())
    }
}

pub mod yyyy_mm_dd_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod nullable_yyyy_mm_dd_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NaiveDateTime::parse_from_str(&s, FORMAT).ok())
    }
}

pub mod va_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod va_datetime_opt {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%m/%d/%Y %H:%M";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NaiveDateTime::parse_from_str(&s, FORMAT).ok())
    }
}

pub mod mssql_date {
    use chrono::{NaiveDate, NaiveDateTime};
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S.%3f";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT)
            .map(|dt| dt.date())
            .map_err(serde::de::Error::custom)
    }
}

pub mod mssql_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S.%3f";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

pub mod nullable_mssql_datetime {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S.%3f";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NaiveDateTime::parse_from_str(&s, FORMAT).ok())
    }
}

pub mod currency {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        s = s.trim().replace(&['$', ','] as &[_], "");
        s.parse::<f64>().map_err(serde::de::Error::custom)
    }
}

pub mod currency_opt {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(None)
        } else {
            s = s.trim().replace(&['$', ','] as &[_], "");
            Ok(Some(s.parse::<f64>().map_err(serde::de::Error::custom)?))
        }
    }
}

pub mod nullable_field {
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(T::deserialize(deserializer).ok())
    }
}

pub mod enum_from_id {
    use serde::{de, Deserialize, Deserializer, Serializer};
    use std::convert::TryFrom;

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: TryFrom<i32>,
    {
        let id = i32::deserialize(deserializer)?;
        T::try_from(id).map_err(|_e| {
            de::Error::invalid_value(de::Unexpected::Unsigned(id as _), &"a valid id")
        })
    }

    pub fn serialize<S, T>(val: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Into<i32>,
    {
        serializer.serialize_i32(val.into())
    }
}

pub mod enum_from_id_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::convert::TryFrom;

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: TryFrom<i32>,
    {
        Ok(i32::deserialize(deserializer)
            .ok()
            .and_then(|id| T::try_from(id).ok()))
    }

    pub fn serialize<'a, S, T>(val: &'a Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        &'a T: Into<i32>,
    {
        if let Some(ref val) = val {
            serializer.serialize_i32(val.into())
        } else {
            serializer.serialize_str("")
        }
    }

    pub fn default<T>() -> Option<T>
    where
        T: Default,
    {
        Some(T::default())
    }
}

pub mod enum_from_id_or_default {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::convert::TryFrom;

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: TryFrom<i32> + Default,
    {
        let id = i32::deserialize(deserializer)?;
        Ok(T::try_from(id).unwrap_or_default())
    }

    pub fn serialize<'a, S, T>(val: &'a T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        &'a T: Into<i32>,
    {
        serializer.serialize_i32(val.into())
    }
}

pub fn serialize_id_empty<S>(val: &i32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *val == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_i32(*val)
    }
}
