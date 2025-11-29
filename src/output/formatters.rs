use super::{
    write_csv_position, write_csv_sunrise, write_json_position, write_json_sunrise,
    write_streaming_text_table, write_text_position, write_text_sunrise,
};
use crate::compute::CalculationResult;
use crate::data::{Command, DataSource, OutputFormat, Parameters};
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
            show_inputs: params.output.should_show_inputs(),
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
                Command::Position => {
                    let row = super::normalize_position_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for position output".to_string()
                    })?;
                    let fields = super::position_fields(&row, self.params, self.show_inputs);
                    write_csv_position(&fields, self.headers, first, self.writer)
                        .map_err(|e| e.to_string())?
                }
                Command::Sunrise => {
                    let row = super::normalize_sunrise_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for sunrise output".to_string()
                    })?;
                    let fields = super::sunrise_fields(&row, self.show_inputs);
                    write_csv_sunrise(&fields, self.headers, first, self.writer)
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
            show_inputs: params.output.should_show_inputs(),
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
                    let row = super::normalize_position_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for position output".to_string()
                    })?;
                    let fields = super::position_fields(&row, self.params, self.show_inputs);
                    write_json_position(&fields, self.writer).map_err(|e| e.to_string())?
                }
                Command::Sunrise => {
                    let row = super::normalize_sunrise_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for sunrise output".to_string()
                    })?;
                    let fields = super::sunrise_fields(&row, self.show_inputs);
                    write_json_sunrise(&fields, self.writer).map_err(|e| e.to_string())?
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
        if matches!(self.command, Command::Position)
            && self.params.output.format == OutputFormat::Text
        {
            return write_streaming_text_table(
                results,
                self.params,
                self.data_source.clone(),
                self.writer,
                self.flush_each,
            )
            .map_err(|e| e.to_string());
        }

        let show_inputs = self.params.output.should_show_inputs();
        let mut count = 0;
        for result_or_err in results {
            let result = result_or_err?;
            match self.command {
                Command::Position => {
                    let row = super::normalize_position_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for position output".to_string()
                    })?;
                    let fields = super::position_fields(&row, self.params, show_inputs);
                    write_text_position(&fields, self.writer).map_err(|e| e.to_string())?
                }
                Command::Sunrise => {
                    let row = super::normalize_sunrise_result(&result).ok_or_else(|| {
                        "Unexpected calculation result for sunrise output".to_string()
                    })?;
                    let fields = super::sunrise_fields(&row, show_inputs);
                    write_text_sunrise(&fields, self.writer).map_err(|e| e.to_string())?
                }
            }
            if self.flush_each {
                self.writer.flush().map_err(|e| e.to_string())?;
            }
            count += 1;
        }
        Ok(count)
    }
}
