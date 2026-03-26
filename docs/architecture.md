# Architecture

Sunce is a streaming CLI for solar position and sunrise/sunset calculations. The top-level flow is:

`parse -> validate -> plan -> execute`

The design goal is to keep each stage narrow:

- parsing handles syntax and raw option values
- validation applies command semantics and produces executable requests
- planning decides execution shape
- execution computes results and writes output

## Execution Modes

Sunce has two internal execution modes:

- `stream`: expand inputs, compute records, and write text/CSV/JSON/Parquet output
- `predicate`: evaluate one boolean condition for one explicit location and instant, optionally waiting until it becomes true

This split is intentional. Predicate mode is not routed through the normal output pipeline.

## Module Boundaries

- `src/main.rs`: top-level dispatcher. It coordinates parse/validate/plan/execute and maps errors to exit codes.
- `src/cli.rs`: manual CLI parser. It builds parsed commands and reports syntax-level errors.
- `src/parsed.rs`: raw parsed command structures used between parsing and validation.
- `src/validate.rs`: semantic validation. It turns parsed commands into valid stream requests or predicate jobs.
- `src/planner.rs`: execution planning. It turns validated commands into either a stream plan or a predicate job.
- `src/compute.rs`: stream orchestration and shared result types.
- `src/position.rs`: solar position calculations and SPA cache support.
- `src/sunrise.rs`: sunrise/twilight calculations, solar-state classification, and next-state transitions.
- `src/predicate.rs`: predicate evaluation and wait-until logic.
- `src/output.rs`: text/CSV/JSON output.
- `src/parquet.rs`: Parquet output when the `parquet` feature is enabled.
- `src/data/`: shared data types, validation helpers, time parsing, and input expansion.

## Data Flow

### Parse

`cli` parses options and positional arguments into `ParsedCommand`. At this stage the program preserves raw user intent such as:

- command choice (`position` or `sunrise`)
- input shape (explicit values, files, ranges, `now`)
- raw predicate flags
- raw option usage

### Validate

`validate` is the semantic boundary. It:

- resolves whether a time input is a single instant or a range
- enforces command-specific option rules
- enforces predicate restrictions
- produces either:
  - a validated stream request (`position` or `sunrise`)
  - a validated `PredicateJob`

After validation, the program should not need to re-check CLI semantics elsewhere.

### Plan

`planner` converts validated commands into one of:

- `RunPlan::Stream`
- `RunPlan::Predicate`

For stream mode, planning decides:

- how inputs are expanded
- whether SPA time caching is allowed
- whether output should flush per record

### Execute

In stream mode:

- `data::expansion` produces a lazy stream of `(lat, lon, datetime)` records
- `compute` dispatches to `position` or `sunrise`
- `output` or `parquet` writes results incrementally

In predicate mode:

- `predicate` evaluates one condition against one resolved location/instant
- optional `--wait` stays inside predicate mode

## Streaming Model

The normal command path is designed for bounded memory use:

- input expansion is iterator-based
- calculations stream record by record
- outputs are written incrementally
- large result sets are not collected in memory

For location ranges, expansion materializes only the smaller coordinate dimension and streams the other, which keeps cartesian products practical without fully buffering them.

## Solar Domain Split

The solar domain logic is split by responsibility:

- `position` owns topocentric solar position and elevation-angle derivation
- `sunrise` owns sunrise/sunset/twilight event calculation and solar-state reasoning
- `predicate` consumes those domain primitives but does not implement solar math itself

This keeps command semantics and automation behavior out of the calculation modules.

## Time and Timezone Rules

Time parsing and timezone resolution are centralized in `data` and validation:

- explicit offsets in input are preserved unless overridden
- otherwise timezone resolution is: `--timezone` -> `TZ` -> system timezone
- partial dates become ranges where appropriate
- predicate mode requires one explicit instant

DST gaps and ambiguous local times are handled during datetime resolution, not later in the compute path.

## Output Design

Stream output formats share one calculation pipeline. Formatting is separated from computation:

- `compute` produces typed calculation results
- `output` and `parquet` map those results into concrete encodings

Predicate mode intentionally does not use these writers; it communicates through exit status.
