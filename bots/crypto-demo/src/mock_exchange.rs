//! `MockExchange` — minimal `ExchangeClient` impl that pretends every
//! action succeeds without contacting a real exchange.
//!
//! Equivalent to the one in `examples/noop-bot/src/main.rs`. Lives in its
//! own module here so the example can grow without a giant `main.rs`.

use async_trait::async_trait;
use rustrade::{ExchangeClient, Order, Position, Result, Symbol};

/// Always-successful mock exchange. Records nothing.
pub struct MockExchange;

#[async_trait]
impl ExchangeClient for MockExchange {
    fn name(&self) -> &str {
        "mock"
    }

    async fn place_order(&self, _order: &Order) -> Result<String> {
        Ok("mock-order-id".to_string())
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> Result<usize> {
        Ok(0)
    }

    async fn close_position(&self, _symbol: &Symbol, _position: &Position) -> Result<String> {
        Ok("mock-close-id".to_string())
    }

    async fn get_position(&self, _symbol: &Symbol) -> Result<Position> {
        Ok(Position::FLAT)
    }

    async fn get_balance(&self, _currency: &str) -> Result<f64> {
        Ok(10_000.0)
    }
}
