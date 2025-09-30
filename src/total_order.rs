pub trait TotalOrder {
    fn total_cmp(&self, other: &Self) -> std::cmp::Ordering;
}

macro_rules! totalorder_impl_fl {
    ($T:ident) => {
        impl TotalOrder for $T {
            #[inline]
            fn total_cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Forward to the core implementation
                Self::total_cmp(&self, other)
            }
        }
    };
}

macro_rules! totalorder_impl_ord {
    ($T:ident) => {
        impl TotalOrder for $T {
            #[inline]
            fn total_cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Forward to the core implementation
                Self::cmp(&self, other)
            }
        }
    };
}
totalorder_impl_fl!(f32);
totalorder_impl_fl!(f64);
totalorder_impl_ord!(i32);
totalorder_impl_ord!(u32);
totalorder_impl_ord!(i64);
totalorder_impl_ord!(u64);
