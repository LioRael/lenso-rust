use crate::{validate_name, Schedule, SchedulerError};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;

pub(crate) const MINUTE: i64 = 60_000;
// Explicit supported domain, avoiding arithmetic overflow and historical sub-minute offsets.
const START: i64 = 946_684_800_000; // 2000-01-01T00:00:00Z, inclusive
const END: i64 = 4_102_444_800_000; // 2100-01-01T00:00:00Z, exclusive
pub(crate) fn validate_instant(value: i64) -> Result<(), SchedulerError> {
    if !(START..END).contains(&value) {
        return Err(SchedulerError::InvalidOptions);
    }
    Ok(())
}
pub(crate) fn validate(schedule: &Schedule) -> Result<Calendar, SchedulerError> {
    validate_name(&schedule.key)?;
    validate_name(&schedule.task)?;
    validate_instant(schedule.start_after)?;
    fn depth(value: &serde_json::Value, remaining: usize) -> bool {
        if remaining == 0 {
            return false;
        }
        match value {
            serde_json::Value::Array(items) => items.iter().all(|v| depth(v, remaining - 1)),
            serde_json::Value::Object(items) => items.values().all(|v| depth(v, remaining - 1)),
            _ => true,
        }
    }
    if !depth(&schedule.input, 33) || schedule.input.to_string().len() > 65_536 {
        return Err(SchedulerError::InvalidOptions);
    }
    Calendar::parse(&schedule.cron, &schedule.timezone)
}
pub(crate) struct Calendar {
    cron: Cron,
    timezone: Tz,
}
impl Calendar {
    pub fn parse(cron: &str, timezone: &str) -> Result<Self, SchedulerError> {
        if cron.len() > 256 || cron.split_whitespace().count() != 5 || timezone.len() > 256 {
            return Err(SchedulerError::InvalidOptions);
        }
        Ok(Self {
            cron: cron.parse().map_err(|_| SchedulerError::InvalidOptions)?,
            timezone: timezone
                .parse()
                .map_err(|_| SchedulerError::InvalidOptions)?,
        })
    }
    pub fn matches(&self, instant: i64) -> Result<bool, SchedulerError> {
        let utc = DateTime::<Utc>::from_timestamp_millis(instant)
            .ok_or(SchedulerError::InvalidOptions)?;
        // Advance in UTC, never construct ambiguous/nonexistent local wall times.
        self.cron
            .is_time_matching(&utc.with_timezone(&self.timezone))
            .map_err(|_| SchedulerError::InvalidOptions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn at(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_millis()
    }
    #[test]
    fn five_field_cron_preserves_steps_ranges_names_dom_dow_or_and_leap_days() {
        let business = Calendar::parse("*/15 9-17 * JAN MON-FRI", "UTC").unwrap();
        assert!(business.matches(at("2024-01-01T09:15:00Z")).unwrap());
        assert!(!business.matches(at("2024-01-01T09:16:00Z")).unwrap());
        assert!(!business.matches(at("2024-01-06T09:15:00Z")).unwrap());
        let either = Calendar::parse("0 0 1 * MON", "UTC").unwrap();
        assert!(either.matches(at("2024-02-01T00:00:00Z")).unwrap());
        assert!(either.matches(at("2024-02-05T00:00:00Z")).unwrap());
        assert!(!either.matches(at("2024-02-06T00:00:00Z")).unwrap());
        for sunday in ["0 0 * * 0", "0 0 * * 7"] {
            assert!(Calendar::parse(sunday, "UTC")
                .unwrap()
                .matches(at("2024-01-07T00:00:00Z"))
                .unwrap());
        }
        let leap = Calendar::parse("0 0 29 FEB *", "UTC").unwrap();
        assert!(leap.matches(at("2024-02-29T00:00:00Z")).unwrap());
        assert!(!leap.matches(at("2023-03-01T00:00:00Z")).unwrap());
    }
}
