#![forbid(unsafe_code)]

//! Authoritative HIP-3 (builder-deployed perp) market identity, asset-ID
//! resolution, and execution-capability admission.
//!
//! Wire identity is always the canonical prefixed string (`dex:COIN`).
//! Integer Hyperliquid asset IDs are derived from the authoritative
//! `perpDexs` order, never hardcoded per market:
//! `HIP3_ASSET_BASE + dex_index * HIP3_DEX_STRIDE + local_index`.

use std::collections::{BTreeMap, BTreeSet};

/// Base offset for all builder-perp assets (Hyperliquid protocol constant).
pub const HIP3_ASSET_BASE: u32 = 100_000;
/// Stride reserved per builder DEX in the integer asset namespace.
pub const HIP3_DEX_STRIDE: u32 = 10_000;
/// DEX explicitly sunset; never admitted even if present in discovery order.
pub const EXCLUDED_HIP3_DEX: &str = "hyna";

/// Canonical market identity without rewriting broader market plumbing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PerpMarket<'a> {
    Native(&'a str),
    Hip3 { dex: &'a str, coin: &'a str },
}

/// Parse `BTC` -> Native, `xyz:NVDA` -> Hip3{dex: xyz, coin: NVDA}.
///
/// Malformed colon shapes (`:COIN`, `dex:`, `a:b:c`) are returned as Hip3
/// with empty parts so downstream capability checks fail closed while the
/// wire string itself stays distinct for venue separation.
pub fn parse_perp_market(asset: &str) -> PerpMarket<'_> {
    match asset.split_once(':') {
        None => PerpMarket::Native(asset),
        Some((dex, coin)) => PerpMarket::Hip3 { dex, coin },
    }
}

/// True for any colon-prefixed wire market (HIP-3 candidate shape).
pub fn is_hip3_market(asset: &str) -> bool {
    asset.contains(':')
}

/// DEX component of a wire market (`""` for native).
pub fn dex_for_market(asset: &str) -> &str {
    asset.split_once(':').map_or("", |(dex, _)| dex)
}

/// DEX name is a valid builder-perp identifier (lowercase alnum, 1..=32).
pub fn valid_dex_name(dex: &str) -> bool {
    !dex.is_empty()
        && dex.len() <= 32
        && dex
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Authoritative market -> execution asset-ID mapping.
///
/// Native perp IDs are their native metadata indices. HIP-3 IDs are
/// `HIP3_ASSET_BASE + dex_index * HIP3_DEX_STRIDE + local_index`, where
/// `dex_index` is the position in the authoritative `perpDexs` order
/// (whose first entry must be the empty default DEX) and `local_index`
/// is the position within that DEX's slice of the merged universe.
pub fn execution_asset_id(universe: &[String], dex_order: &[String], asset: &str) -> Option<u32> {
    let dex = dex_for_market(asset);
    let offset = if dex.is_empty() {
        0
    } else {
        if !valid_dex_name(dex) || dex == EXCLUDED_HIP3_DEX {
            return None;
        }
        let index = u32::try_from(dex_order.iter().position(|name| name == dex)?).ok()?;
        if index == 0 || dex_order.first().is_none_or(|name| !name.is_empty()) {
            return None;
        }
        HIP3_ASSET_BASE.checked_add(index.checked_mul(HIP3_DEX_STRIDE)?)?
    };
    let local = universe
        .iter()
        .filter(|candidate| dex_for_market(candidate) == dex)
        .position(|candidate| candidate == asset)?;
    offset.checked_add(u32::try_from(local).ok()?)
}

/// Collateral token for a DEX. Native uses USDC; each admitted builder DEX
/// must have an explicit entry (defaulted to USDC at discovery, overridable
/// by operators). Missing entries fail closed.
pub fn collateral_for_dex(dex: &str, registry: &BTreeMap<String, String>) -> Option<String> {
    if dex.is_empty() {
        return Some("USDC".to_string());
    }
    if dex == EXCLUDED_HIP3_DEX {
        return None;
    }
    registry.get(dex).cloned()
}

/// Capability set required for a market to be production-executable.
/// HIP-3 must satisfy every capability; native perps keep their existing
/// admission (this gate only adds HIP-3 support, never restricts native).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hip3Capabilities {
    pub dex: String,
    pub dex_index: u32,
    pub local_index: u32,
    pub asset_id: u32,
    pub size_decimals: u32,
    pub collateral: String,
}

/// Capability-based admission for production execution.
///
/// Returns `Some` only when: DEX discovered/admitted, market metadata
/// loaded, canonical asset ID resolved, size metadata valid, collateral
/// resolved, and live market state (mid) available. Unknown/unresolved
/// markets return `None` (fail closed).
#[allow(clippy::too_many_arguments)]
pub fn execution_capabilities(
    asset: &str,
    universe: &BTreeMap<String, u32>,
    dex_order: &[String],
    universe_order: &[String],
    mids: &BTreeSet<String>,
    collateral_registry: &BTreeMap<String, String>,
) -> Option<Hip3Capabilities> {
    match parse_perp_market(asset) {
        PerpMarket::Native(_) => None,
        PerpMarket::Hip3 { dex, coin } => {
            if dex.is_empty() || coin.is_empty() || coin.contains(':') {
                return None;
            }
            if !valid_dex_name(dex) || dex == EXCLUDED_HIP3_DEX {
                return None;
            }
            let dex_index = u32::try_from(dex_order.iter().position(|n| n == dex)?).ok()?;
            if dex_index == 0 || dex_order.first().is_none_or(|n| !n.is_empty()) {
                return None;
            }
            let size_decimals = *universe.get(asset)?;
            if size_decimals > 18 {
                return None;
            }
            let asset_id = execution_asset_id(universe_order, dex_order, asset)?;
            // Verify dex/local decomposition round-trips the asset ID.
            let local_index = asset_id
                .checked_sub(HIP3_ASSET_BASE)?
                .checked_sub(dex_index.checked_mul(HIP3_DEX_STRIDE)?)?;
            let collateral = collateral_for_dex(dex, collateral_registry)?;
            if !mids.contains(asset) {
                return None;
            }
            Some(Hip3Capabilities {
                dex: dex.to_string(),
                dex_index,
                local_index,
                asset_id,
                size_decimals,
                collateral,
            })
        }
    }
}

/// All DEXes referenced by canonical wire assets (native excluded).
pub fn dexes_for_assets<'a>(assets: impl Iterator<Item = &'a str>) -> BTreeSet<String> {
    assets
        .map(dex_for_market)
        .filter(|dex| !dex.is_empty() && *dex != EXCLUDED_HIP3_DEX)
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn universe_order() -> Vec<String> {
        vec![
            "BTC".into(),
            "GOLD".into(),
            "xyz:NVDA".into(),
            "xyz:GOLD".into(),
            "foo:GOLD".into(),
        ]
    }

    #[test]
    fn parse_preserves_venue_identity() {
        assert_eq!(parse_perp_market("BTC"), PerpMarket::Native("BTC"));
        assert_eq!(
            parse_perp_market("xyz:NVDA"),
            PerpMarket::Hip3 {
                dex: "xyz",
                coin: "NVDA"
            }
        );
        assert_eq!(
            parse_perp_market("foo:GOLD"),
            PerpMarket::Hip3 {
                dex: "foo",
                coin: "GOLD"
            }
        );
        // Same visible symbol on different venues never aliases.
        assert_ne!(parse_perp_market("GOLD"), parse_perp_market("xyz:GOLD"));
        assert_ne!(parse_perp_market("xyz:GOLD"), parse_perp_market("foo:GOLD"));
    }

    #[test]
    fn asset_ids_derive_from_dex_index_plus_local_index() {
        let order = universe_order();
        let dex_order = vec!["".to_string(), "xyz".to_string(), "foo".to_string()];
        assert_eq!(execution_asset_id(&order, &dex_order, "BTC"), Some(0));
        assert_eq!(
            execution_asset_id(&order, &dex_order, "xyz:NVDA"),
            Some(110_000)
        );
        assert_eq!(
            execution_asset_id(&order, &dex_order, "xyz:GOLD"),
            Some(110_001)
        );
        assert_eq!(
            execution_asset_id(&order, &dex_order, "foo:GOLD"),
            Some(120_000)
        );
        // Unknown DEX / market / sunset DEX fail closed.
        assert_eq!(execution_asset_id(&order, &dex_order, "bar:GOLD"), None);
        assert_eq!(execution_asset_id(&order, &dex_order, "xyz:UNKNOWN"), None);
        assert_eq!(execution_asset_id(&order, &dex_order, "hyna:GOLD"), None);
    }

    #[test]
    fn capabilities_fail_closed_without_collateral_or_live_state() {
        let universe: BTreeMap<String, u32> =
            BTreeMap::from([("xyz:NVDA".into(), 2), ("foo:GOLD".into(), 3)]);
        let order = vec!["xyz:NVDA".into(), "foo:GOLD".into()];
        let dex_order = vec!["".into(), "xyz".into(), "foo".into()];
        let mids: BTreeSet<String> = BTreeSet::from(["xyz:NVDA".into()]);
        let collateral: BTreeMap<String, String> = BTreeMap::from([("xyz".into(), "USDC".into())]);
        // Valid market admits.
        let caps = execution_capabilities(
            "xyz:NVDA",
            &universe,
            &dex_order,
            &order,
            &mids,
            &collateral,
        )
        .unwrap();
        assert_eq!(caps.asset_id, 110_000);
        assert_eq!(caps.collateral, "USDC");
        // Missing collateral fails closed.
        assert!(execution_capabilities(
            "foo:GOLD",
            &universe,
            &dex_order,
            &order,
            &BTreeSet::from(["foo:GOLD".into()]),
            &collateral,
        )
        .is_none());
        // Missing live state fails closed.
        assert!(execution_capabilities(
            "xyz:NVDA",
            &universe,
            &dex_order,
            &order,
            &BTreeSet::new(),
            &collateral,
        )
        .is_none());
        // Native is not a HIP-3 capability (native path unchanged).
        assert!(
            execution_capabilities("BTC", &universe, &dex_order, &order, &mids, &collateral,)
                .is_none()
        );
    }
}
