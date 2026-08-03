//! Concrete Feature Store producers.
//!
//! Each producer reads via its own [`Reader`] on the caller's async task and
//! emits [`FeatureRow`]s (point-in-time correct, spec 20 I3). The single writer
//! persists them and refreshes the wide views.

pub mod cadence;

pub use cadence::CadenceRegularityProducer;
