use std::sync::mpsc;

use itertools::Itertools;
use rayon::iter::{ParallelBridge, ParallelIterator};

pub struct ParMapUntilIterator<T, U> {
    receiver: mpsc::Receiver<(usize, ParMapResult<T, U>)>,
    buffer: Vec<(usize, ParMapResult<T, U>)>,
    next_idx: usize,
    found: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParMapResult<T, U> {
    Found(T),
    NotFound(U),
}

pub trait ParMapUntil<S: Send, T: Send>: Iterator<Item = S> {
    /// Creates an iterator that maps `map` over the items of this iterator in parallel.
    /// The iterator will stop as soon as a `Found` value is found as reported by `map`.
    ///
    /// The (non-parallel) iterator yields the items in their original order.
    /// The operation `map` may be called on items that occur after the first `Found` value,
    /// in which case the results of the `map` operation are not yielded.
    fn par_map_until<U: Send, F: Fn(S) -> ParMapResult<T, U> + Sync>(
        self,
        map: F,
    ) -> ParMapUntilIterator<T, U>;
}

impl<S: Send, T: Send, I: Iterator<Item = S> + Send> ParMapUntil<S, T> for I {
    fn par_map_until<U: Send, F: Fn(S) -> ParMapResult<T, U> + Sync>(
        self,
        map: F,
    ) -> ParMapUntilIterator<T, U> {
        par_map_until::<S, T, U>(self, map)
    }
}

fn par_map_until<S: Send, T: Send, U: Send>(
    iter: impl Iterator<Item = S> + Send,
    map: impl Fn(S) -> ParMapResult<T, U> + Sync,
) -> ParMapUntilIterator<T, U> {
    let (sender, receiver) = mpsc::channel();
    let _ = iter
        .enumerate()
        .par_bridge()
        .map(|(idx, u)| (idx, map(u)))
        .try_for_each_with(sender, |sender, (idx, op)| {
            let should_err = matches!(op, ParMapResult::Found(_));
            sender.send((idx, op)).unwrap();
            if should_err {
                Err(idx)
            } else {
                Ok(())
            }
        });

    ParMapUntilIterator {
        receiver,
        buffer: vec![],
        next_idx: 0,
        found: false,
    }
}

impl<T, U> Iterator for ParMapUntilIterator<T, U> {
    type Item = ParMapResult<T, U>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.found {
            return None;
        }

        // Check buffer
        if let Some(to_handle) = self.buffer.iter().find_position(|it| it.0 == self.next_idx) {
            let buffer_index = to_handle.0;
            let item: (usize, ParMapResult<T, U>) = self.buffer.swap_remove(buffer_index);
            self.next_idx += 1;
            if matches!(item.1, ParMapResult::Found(_)) {
                self.found = true;
            }
            return Some(item.1);
        }

        // Wait until the item with the correct index was sent
        // (or wait for Err if all senders finished)
        while let Ok(received) = self.receiver.recv() {
            if received.0 == self.next_idx {
                self.next_idx += 1;
                if matches!(received.1, ParMapResult::Found(_)) {
                    self.found = true;
                }
                return Some(received.1);
            } else {
                self.buffer.push(received);
            }
        }

        assert_eq!(self.buffer.len(), 0);
        None
    }
}

impl<T, U> ParMapUntilIterator<T, U> {
    pub fn for_each_count<F: FnMut(ParMapResult<T, U>)>(mut self, mut f: F) -> usize {
        for item in self.by_ref() {
            f(item);
        }
        self.next_idx
    }
}
