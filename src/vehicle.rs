use std::fmt::Debug;

use crate::primitives::Time;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtStationId(pub u32);
impl Debug for ExtStationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("s#{}", self.0))
    }
}

#[derive(Debug)]
pub struct ExtFirstStop {
    pub station: ExtStationId,
    pub departure: Time,
}

#[derive(Debug)]
pub struct ExtMiddleStop {
    pub station: ExtStationId,
    pub arrival: Time,
    pub departure: Time,
}

#[derive(Debug)]
pub struct ExtLastStop {
    pub station: ExtStationId,
    pub arrival: Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VehicleId(pub u32);

#[derive(Debug)]
pub struct ExtVehicle {
    pub id: VehicleId,
    pub capacity: f64,
    pub first_stop: ExtFirstStop,
    pub middle_stops: Vec<ExtMiddleStop>,
    pub last_stop: ExtLastStop,
}
