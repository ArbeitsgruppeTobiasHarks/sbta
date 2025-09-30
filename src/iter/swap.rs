use std::iter::Map;

pub trait Swap<I, A, B>
where
    I: Iterator<Item = (A, B)>,
{
    fn swap(self) -> Map<I, impl FnMut((A, B)) -> (B, A)>;
}

impl<I, A, B> Swap<I, A, B> for I
where
    I: Iterator<Item = (A, B)>,
{
    fn swap(self) -> Map<I, impl FnMut((A, B)) -> (B, A)> {
        self.map(|(a, b)| (b, a))
    }
}
