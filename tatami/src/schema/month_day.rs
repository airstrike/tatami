//! Opaque [`MonthDay`] — a validated `(month, day)` pair.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A calendar month, 1..=12. Defined locally because `jiff` 0.2 exposes
/// months as `i8`; when jiff gains a dedicated `Month` enum we will
/// re-export that instead.
// TODO(jiff): swap for `jiff::civil::Month` when upstream ships it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Month {
    /// January
    January = 1,
    /// February
    February = 2,
    /// March
    March = 3,
    /// April
    April = 4,
    /// May
    May = 5,
    /// June
    June = 6,
    /// July
    July = 7,
    /// August
    August = 8,
    /// September
    September = 9,
    /// October
    October = 10,
    /// November
    November = 11,
    /// December
    December = 12,
}

/// Recurring `(month, day)` — for example, a fiscal-year start of April 1.
///
/// `MonthDay::new(month, day)` is the boundary constructor; it rejects
/// `day == 0`, days beyond the month's non-leap length, and explicitly
/// rejects `February 29` as ambiguous (this is a *recurring* date — leap-day
/// recurrence is intentionally out of scope).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MonthDay {
    month: Month,
    day: u8,
}

impl MonthDay {
    /// Boundary constructor. See the type-level doc for rejection rules.
    pub fn new(month: Month, day: u8) -> Result<Self, Error> {
        if day == 0 {
            return Err(Error::DayZero);
        }
        // Feb 29 is called out explicitly before the range check so callers
        // see the intent ("leap-day is ambiguous") rather than "28 is the
        // max".
        if month == Month::February && day == 29 {
            return Err(Error::LeapDayAmbiguous);
        }
        let max = max_day(month);
        if day > max {
            return Err(Error::DayOutOfRange {
                month,
                day,
                max_day: max,
            });
        }
        Ok(Self { month, day })
    }

    /// Accessor for the month component.
    #[must_use]
    pub fn month(&self) -> Month {
        self.month
    }

    /// Accessor for the day component (1-based).
    #[must_use]
    pub fn day(&self) -> u8 {
        self.day
    }
}

impl fmt::Display for MonthDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Simple ISO-style `MM-DD` for logs; not intended as a serde form.
        write!(f, "{:02}-{:02}", self.month as u8, self.day)
    }
}

/// Serde shape: `{"month": 4, "day": 1}`. Month is stored as a 1..=12 integer
/// (jiff's `Month` lacks a derive-serde representation, so we hand-roll the
/// conversion).
#[derive(Debug, Serialize, Deserialize)]
struct Wire {
    month: u8,
    day: u8,
}

impl Serialize for MonthDay {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Wire {
            month: self.month as u8,
            day: self.day,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MonthDay {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = Wire::deserialize(deserializer)?;
        let month = month_from_u8(wire.month).map_err(serde::de::Error::custom)?;
        Self::new(month, wire.day).map_err(serde::de::Error::custom)
    }
}

fn month_from_u8(n: u8) -> Result<Month, Error> {
    match n {
        1 => Ok(Month::January),
        2 => Ok(Month::February),
        3 => Ok(Month::March),
        4 => Ok(Month::April),
        5 => Ok(Month::May),
        6 => Ok(Month::June),
        7 => Ok(Month::July),
        8 => Ok(Month::August),
        9 => Ok(Month::September),
        10 => Ok(Month::October),
        11 => Ok(Month::November),
        12 => Ok(Month::December),
        other => Err(Error::MonthOutOfRange(other)),
    }
}

fn max_day(m: Month) -> u8 {
    match m {
        Month::January
        | Month::March
        | Month::May
        | Month::July
        | Month::August
        | Month::October
        | Month::December => 31,
        Month::April | Month::June | Month::September | Month::November => 30,
        Month::February => 28,
    }
}

/// Errors produced by [`MonthDay::new`] and deserialisation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// `day` was zero; valid days are `1..=days_in_month`.
    #[error("day must be at least 1")]
    DayZero,
    /// `day` exceeded the non-leap length of `month`.
    #[error("day {day} out of range for {month:?} (max {max_day})")]
    DayOutOfRange {
        /// The month whose range was violated.
        month: Month,
        /// The rejected day.
        day: u8,
        /// The maximum valid day in that month.
        max_day: u8,
    },
    /// February 29 is explicitly rejected (ambiguous across non-leap years).
    #[error("February 29 is ambiguous as a recurring month-day")]
    LeapDayAmbiguous,
    /// The wire-form `month` field was not in `1..=12`.
    #[error("month {0} is not in 1..=12")]
    MonthOutOfRange(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_day_accepts_fiscal_april_first() {
        let md = MonthDay::new(Month::April, 1).expect("valid");
        assert_eq!(md.month(), Month::April);
        assert_eq!(md.day(), 1);
    }

    #[test]
    fn month_day_rejects_february_29_leap_ambiguous() {
        assert!(matches!(
            MonthDay::new(Month::February, 29),
            Err(Error::LeapDayAmbiguous)
        ));
    }

    #[test]
    fn month_day_rejects_april_31() {
        assert!(matches!(
            MonthDay::new(Month::April, 31),
            Err(Error::DayOutOfRange { .. })
        ));
    }

    #[test]
    fn month_day_rejects_january_0() {
        assert!(matches!(
            MonthDay::new(Month::January, 0),
            Err(Error::DayZero)
        ));
    }

    #[test]
    fn month_day_accepts_feb_28() {
        let md = MonthDay::new(Month::February, 28).expect("valid");
        assert_eq!(md.day(), 28);
    }

    #[test]
    fn month_day_roundtrip_stable() {
        let md = MonthDay::new(Month::April, 1).expect("valid");
        let json = serde_json::to_string(&md).expect("serialize");
        assert_eq!(json, r#"{"month":4,"day":1}"#);
        let back: MonthDay = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(md, back);
    }

    #[test]
    fn month_day_deserialize_rejects_feb_29() {
        let err = serde_json::from_str::<MonthDay>(r#"{"month":2,"day":29}"#);
        assert!(err.is_err());
    }

    #[test]
    fn month_day_deserialize_rejects_month_out_of_range() {
        let err = serde_json::from_str::<MonthDay>(r#"{"month":13,"day":1}"#);
        assert!(err.is_err());
    }
}
