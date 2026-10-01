//! Astercore: MoonProto server-side engine for Aster.
//!
//! Adapted from TInvestCore (`../TInvestCore`), which is not edited from here.
//! `PLAN.md` records what carries over, what is replaced, and what had to be
//! deleted rather than left beside the new code.

pub mod aster;
pub mod book;
pub mod candles5m;
pub mod engine;
pub mod feed;
pub mod key_store;
pub mod load;
pub mod model;
pub mod prices;
pub mod stderr_log;
pub mod strategies;
pub mod stream_health;
pub mod subs;
pub mod trades_stream;
