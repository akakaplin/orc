//! Turning sealed WAL segments into sorted, deduplicated Parquet.

pub mod merge;
pub mod parquet;
pub mod planner;
pub mod read;
pub mod view;
