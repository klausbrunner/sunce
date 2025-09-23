# Architecture Overview

Sunce is a high-performance command-line solar position calculator written in Rust. The application provides two primary commands for calculating solar coordinates and sunrise/sunset times, built around a streaming architecture that maintains constant memory usage regardless of input size.

Sunce provides almost-feature-parity with the original Java project, `solarpos`, while adding several enhancements like unix timestamp support and optimized coordinate range processing. It's based on a similar architecture though adapted to Rust's strengths.

## Core Architecture

The application follows a pipeline architecture where data flows through parsing, calculation, and output stages. The main entry point in `main.rs` orchestrates this flow by delegating to specialized modules. The `engine.rs` module serves as the business logic coordinator, handling the execution of both position and sunrise commands through a common pattern of parameter preparation, iterator creation, and result output.

## Command Structure

Sunce supports two primary commands. The position command calculates topocentric solar coordinates (azimuth, elevation) for given coordinates and times. The sunrise command computes sunrise, sunset, and twilight times for specified locations and dates. Both commands share a common input processing pipeline but diverge in their calculation engines and output formatters.

## Input Processing Pipeline

Input handling is distributed across several specialized modules. The `cli.rs` module defines the command-line interface using clap, while `parsing.rs` provides the high-level parsing coordination. The consolidated `input_parsing.rs` module handles all input parsing including coordinate processing (latitude/longitude values with range syntax), datetime parsing (various formats, partial dates, and unix timestamps), and input type determination. The `file_input.rs` module manages file-based input including stdin support with streaming iterators.

## Streaming Architecture

The application implements true streaming throughout its processing pipeline. The `iterators.rs` module creates lazy calculation iterators that process data on-demand without materializing intermediate results, including optimized coordinate range processing with split-mode calculations for geographic sweeps. This design ensures constant memory usage whether processing a single coordinate pair or infinite coordinate streams. The `time_series.rs` module handles temporal sequences, supporting partial date specifications that expand into full datetime ranges. File input parsing uses direct iterator chains without intermediate Vec allocations, maintaining zero-allocation parsing for individual lines.

## Calculation Layer

Solar calculations are abstracted through calculation engines defined in `calculation.rs`. The module provides both position and sunrise calculation engines that wrap the underlying solar-positioning library. These engines implement a common trait pattern allowing the iterator infrastructure to work uniformly across different calculation types.

## Output Management

Output formatting is handled by two separate systems. Position results flow through `output.rs`, which provides formatters for human-readable, CSV, and JSON output formats. Sunrise results use a separate path through `sunrise_output.rs` and `sunrise_formatters.rs`, reflecting the different data structures and formatting requirements for sunrise/sunset calculations.

## Timezone and Time Handling

Timezone processing occurs in `timezone.rs`, which manages the conversion between user-specified timezones and the UTC requirements of the underlying solar calculations. The module handles timezone inference, DST transitions, and maintains timezone information throughout the processing pipeline. Supporting utilities in `datetime_utils.rs` provide common datetime operations.

## Type System

The `types.rs` module defines the core data structures used throughout the application. This includes output format enumerations, error types for comprehensive error handling, and utility functions for datetime formatting that ensure compatibility with the Java solarpos tool.

## Data Flow

Input processing begins with command-line argument parsing in `cli.rs`, followed by high-level input structuring in `parsing.rs`. Specialized parsers then process coordinates, datetimes, and file inputs into structured data. The engine layer coordinates calculation parameter preparation and iterator creation. Calculations execute through lazy iterators that yield results as they are computed. Finally, output formatters consume these iterators and produce formatted output in the requested format.

The entire pipeline maintains streaming semantics, ensuring that the first result appears immediately and memory usage remains constant regardless of input size. This architecture enables the application to process infinite coordinate ranges or endless time series without memory growth.

The architecture prioritizes streaming and memory efficiency, focusing on single-threaded streaming performance and optimized algorithms for maximum throughput.
