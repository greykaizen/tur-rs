//! Terminal UI frontend for tur-rs.
//!
//! This module provides the interactive TUI (ratatui/crossterm) experience.
//! It is only compiled when the `tui` feature is enabled.

mod app;
mod input;
mod render;

pub use app::TuiApp;
