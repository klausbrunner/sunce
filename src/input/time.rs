//! Calendar periods and position sampling, shared by the library and CLI.

use crate::{Error, Step, Timezone};
use chrono::{DateTime, Days, FixedOffset, Months, NaiveDate, NaiveTime};

/// Inclusive dates for a day, month, year, or ISO date interval.
pub(crate) fn date_bounds(period: &str) -> Result<(NaiveDate, NaiveDate), Error> {
    let parse = |value: &str| {
        value
            .parse::<NaiveDate>()
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
            let end = start
                .checked_add_months(Months::new(1))
                .and_then(|date| date.pred_opt())
                .ok_or_else(|| Error::Input("Calendar date out of range".into()))?;
            Ok((start, end))
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
    timezone: Timezone,
    step: Step,
) -> Result<impl Iterator<Item = Result<DateTime<FixedOffset>, Error>> + use<>, Error> {
    if step.0 <= chrono::Duration::zero() {
        return Err(Error::Input("Step must be positive".into()));
    }
    let (start, end) = date_bounds(period)?;
    let after_end = end
        .succ_opt()
        .ok_or_else(|| Error::Input("Calendar date out of range".into()))?;
    let boundary = |date: NaiveDate| {
        timezone
            .at_or_after_local(&date.and_time(NaiveTime::MIN))
            .ok_or_else(|| {
                Error::Input(format!("Cannot resolve date boundary in timezone: {date}"))
            })
    };
    let start = boundary(start)?;
    // An exclusive next-date boundary retains a repeated final hour.
    let end = boundary(after_end)?.naive_utc();
    Ok(
        std::iter::successors((start.naive_utc() < end).then_some(start), move |time| {
            time.naive_utc()
                .checked_add_signed(step.0)
                .filter(|next| *next < end)
                .map(|next| timezone.to_datetime_from_utc(&next))
        })
        .map(Ok),
    )
}

/// One position instant per local date in a period, at the given clock time.
/// Gaps advance to the next valid instant; overlaps use the earlier occurrence.
/// Times resolving outside their requested date produce no sample.
pub fn daily_times(
    period: &str,
    timezone: Timezone,
    clock: NaiveTime,
) -> Result<impl Iterator<Item = Result<DateTime<FixedOffset>, Error>> + use<>, Error> {
    let (start, end) = date_bounds(period)?;
    Ok(event_dates(start, end)?.filter_map(move |date| {
        match timezone.at_or_after_local(&date.and_time(clock)) {
            Some(time) if time.date_naive() == date => Some(Ok(time)),
            Some(_) => None,
            None => Some(Err(Error::Input(format!(
                "Cannot resolve local time on {date}"
            )))),
        }
    }))
}

/// Inclusive local calendar dates. Date iteration does not assume 24-hour days.
pub fn event_dates(
    start: NaiveDate,
    end: NaiveDate,
) -> Result<impl Iterator<Item = NaiveDate>, Error> {
    if start > end {
        return Err(Error::Input("Start date must not be after end date".into()));
    }
    Ok(std::iter::successors(Some(start), move |date| {
        date.checked_add_days(Days::new(1))
            .filter(|date| *date <= end)
    }))
}
