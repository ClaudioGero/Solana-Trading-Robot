/// Numeric formatting utilities shared across crates.
///
/// IMPORTANT: These functions are intentionally kept identical to the previous
/// in-crate implementations to avoid behavior changes.

/// Compute \(10^\text{exp}\) as u128 using saturating multiply.
pub fn pow10_u128(exp: u8) -> u128 {
    let mut v: u128 = 1;
    for _ in 0..exp {
        v = v.saturating_mul(10);
    }
    v
}

/// Format large numbers in a compact human-readable form (K/M/B/...).
pub fn format_compact(mut x: f64) -> String {
    if !x.is_finite() {
        return "n/a".into();
    }
    if x < 0.0 {
        x = -x;
    }
    let units = [
        ("", 1.0),
        ("K", 1e3),
        ("M", 1e6),
        ("B", 1e9),
        ("T", 1e12),
        ("Q", 1e15),
    ];
    let mut suffix = "";
    let mut denom = 1.0;
    for (s, d) in units {
        if x >= d {
            suffix = s;
            denom = d;
        }
    }
    let v = x / denom;
    if denom == 1.0 {
        format!("{:.0}", v)
    } else if v >= 100.0 {
        format!("{:.0}{}", v, suffix)
    } else if v >= 10.0 {
        format!("{:.1}{}", v, suffix)
    } else {
        format!("{:.2}{}", v, suffix)
    }
}
