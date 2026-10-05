//! Astercore: MoonProto server-side engine for Aster.
//!
//! Adapted from TInvestCore (`../TInvestCore`), which is not edited from here.
//! `PLAN.md` records what carries over, what is replaced, and what had to be
//! deleted rather than left beside the new code.

pub mod account;
pub mod api_meter;
pub mod aster;
pub mod autostop;
pub mod book;
pub mod bvsv;
pub mod candles5m;
pub mod chart;
pub mod clock;
pub mod control;
pub mod drops;
pub mod emulator;
pub mod engine;
pub mod feed;
pub mod guards;
pub mod hook;
pub mod key_store;
pub mod levman;
pub mod load;
pub mod model;
pub mod moonshot;
pub mod order_store;
pub mod orders;
pub mod prices;
pub mod reports;
pub mod screener;
pub mod settings;
pub mod setup;
pub mod stderr_log;
pub mod stops;
pub mod strategies;
pub mod strategy_file;
pub mod stream_health;
pub mod strike;
pub mod subs;
pub mod tape;
pub mod telegram;
pub mod trades_stream;
pub mod trading;
pub mod update;
pub mod web;
pub mod windows;
