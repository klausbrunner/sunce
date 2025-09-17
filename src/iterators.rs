use crate::parsing::Coordinate;

/// Streaming iterator for coordinate ranges (no Vec collection)
pub struct CoordinateRangeIterator {
    current: f64,
    end: f64,
    step: f64,
    ascending: bool,
    finished: bool,
}

impl CoordinateRangeIterator {
    pub fn new(start: f64, end: f64, step: f64) -> Self {
        let ascending = step > 0.0;
        Self {
            current: start,
            end,
            step: step.abs(),
            ascending,
            finished: false,
        }
    }
}

impl Iterator for CoordinateRangeIterator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let current = self.current;

        // Check if we've reached or passed the end
        let at_or_past_end = if self.ascending {
            current >= self.end
        } else {
            current <= self.end
        };

        if at_or_past_end {
            self.finished = true;
            Some(current)
        } else {
            // Advance to next value
            if self.ascending {
                self.current += self.step;
            } else {
                self.current -= self.step;
            }
            Some(current)
        }
    }
}

/// Create a streaming iterator for a coordinate (single value or range)
pub fn create_coordinate_iterator(coord: &Coordinate) -> Box<dyn Iterator<Item = f64>> {
    match coord {
        Coordinate::Single(val) => Box::new(std::iter::once(*val)),
        Coordinate::Range { start, end, step } => {
            Box::new(CoordinateRangeIterator::new(*start, *end, *step))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascending_range() {
        let iter = CoordinateRangeIterator::new(1.0, 3.0, 1.0);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_descending_range() {
        let iter = CoordinateRangeIterator::new(3.0, 1.0, -1.0);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![3.0, 2.0, 1.0]);
    }

    #[test]
    fn test_fractional_step() {
        let iter = CoordinateRangeIterator::new(0.0, 1.0, 0.5);
        let values: Vec<f64> = iter.collect();
        assert_eq!(values, vec![0.0, 0.5, 1.0]);
    }
}
