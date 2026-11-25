//! When a schedule fires next (decision 0011): [`ScheduleTrigger`]s (once,
//! every N days or weeks, some months and days, with repetition, a random
//! delay and an expiry), evaluated in the schedule's time zone and returned
//! in UTC.
//!
//! Pure: no I/O, no clock; `now` comes in as an argument.

mod trigger;

pub use trigger::{
    next_trigger, random_delay, validate_trigger, Recurrence, Repeat, ScheduleTrigger,
};
