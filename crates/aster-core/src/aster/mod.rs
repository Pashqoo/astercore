//! The Aster side of the core: REST gateway, wire forms, and (from M1) the
//! WebSocket streams.
//!
//! Aster speaks the Binance USDⓈ-M Futures dialect, which is the dialect
//! MoonProto itself was shaped around — so unlike TInvestCore's T-Invest
//! adapter, nothing here translates between two different models of a market.

pub mod json;
pub mod rest;
