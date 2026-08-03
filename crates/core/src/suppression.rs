//! Suppression writeback DTOs (specs/10 §2 `suppression` table, E1).
//!
//! The external delivery system POSTs per-outcome writebacks (targeted /
//! delivered / converted / opted-out / bounced) which the engine persists via
//! Q3 and later reads in `Exclude` anti-joins (specs/12 §4, specs/20 §5). The
//! client supplies `suppression_id` for idempotency (a re-POST with the same id
//! writes nothing new).

use serde::{Deserialize, Serialize};

/// The delivery channel a suppression outcome was observed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SuppressionChannel {
    /// SMS.
    Sms,
    /// Email.
    Email,
    /// Push notification.
    Push,
    /// Paid display/ads.
    Ads,
}

impl SuppressionChannel {
    /// The wire label (`"sms"` / `"email"` / `"push"` / `"ads"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sms => "sms",
            Self::Email => "email",
            Self::Push => "push",
            Self::Ads => "ads",
        }
    }

    /// Parse the wire label back into a channel. Returns `None` for unknown
    /// labels (reject, never coerce — boundary input).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "sms" => Some(Self::Sms),
            "email" => Some(Self::Email),
            "push" => Some(Self::Push),
            "ads" => Some(Self::Ads),
            _ => None,
        }
    }
}

/// The suppression outcome written back by the delivery system (E1). `Targeted`
/// and `Delivered` drive the `Exclude` rules (specs/20 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SuppressionAction {
    /// The user was targeted for the campaign.
    Targeted,
    /// The message was delivered.
    Delivered,
    /// The user converted.
    Converted,
    /// The user opted out.
    OptedOut,
    /// The message bounced.
    Bounced,
}

impl SuppressionAction {
    /// The wire label (`"targeted"` / `"delivered"` / `"converted"` /
    /// `"opted_out"` / `"bounced"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Targeted => "targeted",
            Self::Delivered => "delivered",
            Self::Converted => "converted",
            Self::OptedOut => "opted_out",
            Self::Bounced => "bounced",
        }
    }

    /// Parse the wire label back into an action. Returns `None` for unknown
    /// labels.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "targeted" => Some(Self::Targeted),
            "delivered" => Some(Self::Delivered),
            "converted" => Some(Self::Converted),
            "opted_out" => Some(Self::OptedOut),
            "bounced" => Some(Self::Bounced),
            _ => None,
        }
    }
}

/// One row of the `suppression` table (specs/10 §2). Travels
/// `ingress → ingestion (Q3) → storage`.
#[derive(Debug, Clone, PartialEq)]
pub struct SuppressionRow {
    /// Dedupe key, supplied by the client (UUIDv7); re-POSTing the same id is a
    /// no-op. Logical key: `suppression_id`.
    pub suppression_id: String,
    /// The campaign the outcome belongs to.
    pub campaign_id: String,
    /// Pseudonymous subject id (D12).
    pub user_id: String,
    /// The delivery channel.
    pub channel: SuppressionChannel,
    /// The outcome.
    pub action: SuppressionAction,
    /// When the outcome occurred (ISO-8601 UTC), per the delivery system.
    pub occurred_ts: String,
    /// When the engine ingested the writeback (ISO-8601 UTC; lag audit).
    pub received_ts: String,
}
