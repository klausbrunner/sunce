//! Calendar periods and position sampling, shared by the library and CLI.

use crate::data::time_utils::at_or_after_local;
use crate::{Error, Step, TimeZone};
use jiff::{
    ZonedDateTime,
    civil::{Date, Time},
};

/// Inclusive dates for a day, month, year, or ISO date interval.
pub(crate) fn date_bounds(period: &str) -> Result<(Date, Date), Error> {
    let parse = |value: &str| {
        value
            .parse::<Date>()
            .map_err(|e| Error::Input(format!("Invalid date '{value}': {e}")))
    };

    if let Some((start, end)) = period.split_once('/') {
        if start.len() != 10 || end.len() != 10 {
            return Err(Error::Input(
                "Date ranges require YYYY-MM-DD/YYYY-MM-DD".into(),
            ));
        }
        let (start, end) = (parse(start)?, parse(end)?);
        if start > end {
            return Err(Error::Input("Start date must not be after end date".into()));
        }
        return Ok((start, end));
    }

    match period.len() {
        4 => Ok((
            parse(&format!("{period}-01-01"))?,
            parse(&format!("{period}-12-31"))?,
        )),
        7 => {
            let start = parse(&format!("{period}-01"))?;
            Ok((start, start.last_of_month()))
        }
        10 => {
            let date = parse(period)?;
            Ok((date, date))
        }
        _ => Err(Error::Input(format!(
            "Unsupported date format: '{period}'. Use YYYY, YYYY-MM, YYYY-MM-DD, or YYYY-MM-DD/YYYY-MM-DD"
        ))),
    }
}

/// Position instants for a day, month, year, or inclusive `YYYY-MM-DD/YYYY-MM-DD` range.
/// Sampling advances in elapsed time and preserves named-zone DST changes.
pub fn position_times(
    period: &str,
    timezone: TimeZone,
    step: Step,
) -> Result<impl Iterator<Item = Result<ZonedDateTime, Error>> + use<>, Error> {
    if !step.0.is_positive() {
        return Err(Error::Input("Step must be positive".into()));
    }
    let (start, end) = date_bounds(period)?;
    let after_end = end
        .tomorrow()
        .map_err(|err| Error::Input(err.to_string()))?;
    let boundary = |date: Date| {
        at_or_after_local(&timezone, date.to_datetime(Time::MIN)).ok_or_else(|| {
            Error::Input(format!("Cannot resolve date boundary in timezone: {date}"))
        })
    };
    let start = boundary(start)?;
    // An exclusive next-date boundary retains a repeated final hour.
    let end = boundary(after_end)?;
    Ok(
        std::iter::successors((start < end).then_some(start), move |time| {
            time.checked_add(step.0).ok().filter(|next| *next < end)
        })
        .map(move |time| Ok(time.to_zoned_datetime(timezone.clone()))),
    )
}

/// One position instant per local date in a period, at the given clock time.
/// Gaps advance to the next valid instant; overlaps use the earlier occurrence.
/// Times resolving outside their requested date produce no sample.
pub fn daily_times(
    period: &str,
    timezone: TimeZone,
    clock: Time,
) -> Result<impl Iterator<Item = Result<ZonedDateTime, Error>> + use<>, Error> {
    let (start, end) = date_bounds(period)?;
    Ok(event_dates(start, end)?.filter_map(move |date| {
        let Some(time) = at_or_after_local(&timezone, date.to_datetime(clock)) else {
            return Some(Err(Error::Input(format!(
                "Cannot resolve local time on {date}"
            ))));
        };
        let time = time.to_zoned_datetime(timezone.clone());
        (time.date() == date).then_some(Ok(time))
    }))
}

/// Inclusive local calendar dates. Date iteration does not assume 24-hour days.
pub fn event_dates(start: Date, end: Date) -> Result<impl Iterator<Item = Date>, Error> {
    if start > end {
        return Err(Error::Input("Start date must not be after end date".into()));
    }
    Ok(std::iter::successors(Some(start), move |date| {
        date.tomorrow().ok().filter(|date| *date <= end)
    }))
}
