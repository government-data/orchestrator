//! When a schedule fires next (decision 0011): a standard 5-field cron (the
//! one a crontab takes) or a [`ScheduleTrigger`] (once, every N days or
//! weeks, some months and days, with repetition, a random delay and an
//! expiry), evaluated in the schedule's time zone and returned in UTC.
//!
//! Pure: no I/O, no clock; `now` comes in as an argument. Missed fires
//! follow the installation's policy (`orchestration_model::fires_to_run`);
//! after them, the next one is computed from now.

mod cron;
mod trigger;

pub use cron::{next_fire, Calendar, ScheduleError, ScheduleSpec};
pub use trigger::{
    next_trigger, random_delay, validate_trigger, Recurrence, Repeat, ScheduleTrigger,
};
