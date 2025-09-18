# sunce

A high-performance command-line solar position calculator.

This is an experimental port of [solarpos](https://github.com/klausbrunner/solarpos), aiming to provide identical functionality for calculating topocentric solar coordinates and sunrise/sunset/twilight times.

Yet another "rewrite in Rust"? Why?

- To test the [solar-positioning](https://crates.io/crates/solar-positioning) library.
- To learn the language and understand its benefits and limitations.
- Rust (like other AOT-compiled languages) may be a better choice for a CLI tool, both in terms of performance and ease of distribution.

## Status

**Early beta** - basic functionality works and seems to be on par with solarpos, tests are in place, but not every detail has been checked.

## License

MIT
