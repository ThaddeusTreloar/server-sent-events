#![doc = include_str!("../README.md")]

pub mod event;
#[cfg(feature = "hyper")]
pub mod hyper;
#[cfg(feature = "reqwest")]
pub mod reqwest;
pub mod stream;
