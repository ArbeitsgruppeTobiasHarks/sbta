use crate::timer::Timer;

pub trait TerminationCondition {
    /// Advances to the next iteration, returns false if the termination condition is met.
    fn advance(&mut self, num_iterations: usize, timer: &mut Timer) -> bool;
}

pub struct AtEquilibrium {}
impl TerminationCondition for AtEquilibrium {
    fn advance(&mut self, _: usize, _: &mut Timer) -> bool {
        true
    }
}

pub struct IterationTerminationCondition {
    pub max_iterations: Option<usize>,
}

impl TerminationCondition for IterationTerminationCondition {
    fn advance(&mut self, num_iterations: usize, _: &mut Timer) -> bool {
        if let Some(max_iterations) = self.max_iterations {
            if num_iterations >= max_iterations {
                return false;
            }
        }
        true
    }
}
