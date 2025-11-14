use super::{
    write_csv_position, write_csv_sunrise, write_json_position, write_json_sunrise,
    write_streaming_text_table, write_text_position, write_text_sunrise,
};
use crate::compute::CalculationResult;
use crate::data::{Command, DataSource, Parameters};
use std::io::Write;

pub trait Formatter {
    fn write(
        &mut self,
        results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    ) -> Result<usize, String>;
}

pub struct CsvFormatter<'a, W: Write> {
    writer: &'a mut W,
    params: &'a Parameters,
    command: Command,
    show_inputs: bool,
    headers: bool,
    flush_each: bool,
}

impl<'a, W: Write> CsvFormatter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        params: &'a Parameters,
        command: Command,
        flush_each: bool,
    ) -> Self {
        Self {
            writer,
            params,
            command,
            show_inputs: params.output.show_inputs.unwrap_or(false),
            headers: params.output.headers,
            flush_each,
        }
    }
}

impl<'a, W: Write> Formatter for CsvFormatter<'a, W> {
    fn write(
        &mut self,
        results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    ) -> Result<usize, String> {
        let mut count = 0;
        for (index, result_or_err) in results.enumerate() {
            let result = result_or_err?;
            let first = index == 0;
            match self.command {
                Command::Position => write_csv_position(
                    &result,
                    self.show_inputs,
                    self.headers,
                    first,
                    self.params,
                    self.writer,
                )
                .map_err(|e| e.to_string())?,
                Command::Sunrise => {
                    write_csv_sunrise(&result, self.show_inputs, self.headers, first, self.writer)
                        .map_err(|e| e.to_string())?
                }
            }
            count += 1;
            if self.flush_each {
                self.writer.flush().map_err(|e| e.to_string())?;
            }
        }
        Ok(count)
    }
}

pub struct JsonFormatter<'a, W: Write> {
    writer: &'a mut W,
    params: &'a Parameters,
    command: Command,
    show_inputs: bool,
    flush_each: bool,
}

impl<'a, W: Write> JsonFormatter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        params: &'a Parameters,
        command: Command,
        flush_each: bool,
    ) -> Self {
        Self {
            writer,
            params,
            command,
            show_inputs: params.output.show_inputs.unwrap_or(false),
            flush_each,
        }
    }
}

impl<'a, W: Write> Formatter for JsonFormatter<'a, W> {
    fn write(
        &mut self,
        results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    ) -> Result<usize, String> {
        let mut count = 0;
        for result_or_err in results {
            let result = result_or_err?;
            match self.command {
                Command::Position => {
                    write_json_position(&result, self.show_inputs, self.params, self.writer)
                        .map_err(|e| e.to_string())?
                }
                Command::Sunrise => write_json_sunrise(&result, self.show_inputs, self.writer)
                    .map_err(|e| e.to_string())?,
            }
            count += 1;
            if self.flush_each {
                self.writer.flush().map_err(|e| e.to_string())?;
            }
        }
        Ok(count)
    }
}

pub struct TextFormatter<'a, W: Write> {
    writer: &'a mut W,
    params: &'a Parameters,
    command: Command,
    data_source: DataSource,
    flush_each: bool,
}

impl<'a, W: Write> TextFormatter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        params: &'a Parameters,
        command: Command,
        data_source: DataSource,
        flush_each: bool,
    ) -> Self {
        Self {
            writer,
            params,
            command,
            data_source,
            flush_each,
        }
    }
}

impl<'a, W: Write> Formatter for TextFormatter<'a, W> {
    fn write(
        &mut self,
        results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    ) -> Result<usize, String> {
        if matches!(self.command, Command::Position) && self.params.output.format == "text" {
            return write_streaming_text_table(
                results,
                self.params,
                self.data_source.clone(),
                self.writer,
                self.flush_each,
            )
            .map_err(|e| e.to_string());
        }

        let show_inputs = self.params.output.show_inputs.unwrap_or(false);
        let mut count = 0;
        for result_or_err in results {
            let result = result_or_err?;
            match self.command {
                Command::Position => write_text_position(
                    &result,
                    show_inputs,
                    self.params.output.elevation_angle,
                    self.writer,
                )
                .map_err(|e| e.to_string())?,
                Command::Sunrise => write_text_sunrise(&result, show_inputs, self.writer)
                    .map_err(|e| e.to_string())?,
            }
            if self.flush_each {
                self.writer.flush().map_err(|e| e.to_string())?;
            }
            count += 1;
        }
        Ok(count)
    }
}
