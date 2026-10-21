//! Checked translation of the reviewed exact-redb-4.4 normal/close envelopes.
//! Logical requests and this page-buffer contributor are not total heap or RSS.
use n0_error::{anyerr, Result};
use redb::private_heap::Geometry;

const PAGE: u64 = 4096;
const REGION: u64 = 1 << 20;
// redb 4.4 PageNumber's region index is 20 bits (header.rs also validates it).
const MAX_REGIONS: u64 = 1 << 20;
const ORDERS: u64 = 21;

fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| anyerr!("native envelope addition overflow"))
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| anyerr!("native envelope multiplication overflow"))
}
fn round_pages(bytes: u64) -> Result<u64> {
    bytes
        .div_ceil(PAGE)
        .max(1)
        .checked_next_power_of_two()
        .ok_or_else(|| anyerr!("native envelope page order overflow"))
}
fn height(n: u64) -> u64 {
    u64::from(n.max(1).ilog2()) + 1
}
fn nodes(n: u64) -> Result<u64> {
    if n == 0 {
        Ok(0)
    } else {
        Ok(mul(n, 2)? - 1)
    }
}

/// Bound for the fixed-Hash point-only lineage. `value_bytes` also covers
/// historical values, rather than only values in the current command.
pub(super) fn leaf_pages(value_bytes: u64) -> Result<u64> {
    let s = add(32, value_bytes)?;
    let m = 32;
    let d = 4;
    let li = add(mul(mul(3, m + d)?, s)?, 8 * m)?.div_ceil(m - d);
    let ld = add(mul(m + d, add(PAGE, mul(6, s)?)?)?, 24 * m)?.div_ceil(3 * (m - d));
    let pages = round_pages(PAGE.max(add(s, 4 + d)?).max(li).max(ld))?;
    if pages > REGION || mul((mul(PAGE, pages)? - 4) / (m + d), 2)? > u16::MAX.into() {
        return Err(anyerr!("native point shape exceeds reviewed format domain"));
    }
    Ok(pages)
}

fn bitmap_bytes(mut count: u64, mut capacity: u64) -> Result<u64> {
    let mut levels = 0;
    let mut bytes = 0;
    loop {
        levels += 1;
        bytes = add(bytes, add(4, mul(count.div_ceil(64), 8)?)?)?;
        if capacity <= 64 {
            break;
        }
        count = count.div_ceil(64);
        capacity = capacity.div_ceil(64);
    }
    add(add(4, mul(levels, 4)?)?, bytes)
}
fn buddy_bytes(pages: u64) -> Result<u64> {
    let mut bytes = add(8, mul(4, ORDERS)?)?;
    for order in 0..ORDERS {
        bytes = add(bytes, bitmap_bytes(pages >> order, REGION >> order)?)?;
    }
    Ok(bytes)
}
fn tracker_bytes(regions: u64) -> Result<u64> {
    add(
        add(4, mul(4, ORDERS)?)?,
        mul(ORDERS, bitmap_bytes(regions, REGION)?)?,
    )
}

fn checked_file_upper(usable: u64, regions: u64, tracker: u64) -> Result<u64> {
    if regions == 0
        || regions > MAX_REGIONS
        || tracker < regions
        || tracker > MAX_REGIONS
        || usable == 0
        || usable > mul(REGION, regions)?
    {
        return Err(anyerr!(
            "native envelope geometry exceeds the reviewed format domain"
        ));
    }
    // Fresh V3 has no region headers. Match the native header's maximum
    // addressable file length, including the one super-header page.
    let file = mul(add(usable, 1)?, PAGE)?;
    let maximum = add(PAGE, mul(MAX_REGIONS, mul(REGION, PAGE)?)?)?;
    if file > maximum {
        return Err(anyerr!(
            "native envelope file length exceeds the reviewed format domain"
        ));
    }
    Ok(file)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Family {
    pub entries: u64,
    pub inserts: u64,
    pub leaf_pages: u64,
}

/// Coherent additive bounds, aged from the same successful private owner.
/// Neither a filesystem scan nor a generic native open establishes this seed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct History {
    pub entries: [u64; 3],
    pub selected_system_records: u64,
    pub retained_system_records: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Envelope {
    pub normal_page_requests: u64,
    pub tail_page_requests: u64,
    pub retired_user_handles: u64,
    pub new_system_records: u64,
    pub usable_page_high_water: u64,
    pub region_high_water: u64,
    pub tracker_high_water: u64,
    pub close_page_order: u64,
    pub close_passes: u64,
    pub close_serialized_value_bytes: u64,
    pub next: History,
}

/// The fixed private actor admits only inserts/replacements, so V=0 here.
/// The checked fresh bootstrap has the four fixed user catalog definitions,
/// no AST, savepoints, escaped readers, application-None commits or extra schema.
pub(crate) fn estimate(
    geometry: Geometry,
    history: History,
    families: [Family; 3],
) -> Result<Envelope> {
    let entry_file_upper = checked_file_upper(
        geometry.usable_pages,
        geometry.regions.into(),
        geometry.tracker_regions.into(),
    )?;
    if geometry.page_bytes != PAGE as u32
        || geometry.region_page_capacity != REGION as u32
        || geometry.regions == 0
        || geometry.usable_pages == 0
        || geometry.tracker_regions < geometry.regions
        || geometry.file_bytes != entry_file_upper
    {
        return Err(anyerr!(
            "native geometry is outside the private 4.4 lineage"
        ));
    }
    let mut app = 0;
    let mut retired = 1; // The old fixed user catalog leaf.
    let mut largest = 1;
    let mut next = history;
    for (index, family) in families.into_iter().enumerate() {
        if family.entries != history.entries[index]
            || !family.leaf_pages.is_power_of_two()
            || family.leaf_pages > REGION
        {
            return Err(anyerr!("native family seed or shape is not coherent"));
        }
        let n = add(family.entries, family.inserts)?;
        let h = height(n);
        let per_insert = add(add(mul(2, h)?, 1)?, mul(2, family.leaf_pages - 1)?)?;
        app = add(app, mul(family.inserts, per_insert)?)?;
        retired = add(retired, mul(h, family.inserts)?)?;
        largest = largest.max(family.leaf_pages);
        next.entries[index] = n;
    }
    let r = retired.div_ceil(400);
    let old = history.selected_system_records;
    let retained = history.retained_system_records;
    let all_old = add(old, retained)?;
    let tail_handles = add(add(nodes(r)?, nodes(retained)?)?, 1)?;
    let new_records = tail_handles.div_ceil(200);
    let main = add(
        add(
            mul(r, add(mul(2, height(r))?, 1)?)?,
            mul(old, add(mul(3, height(all_old))?, 2)?)?,
        )?,
        1,
    )?;
    let tail = add(
        add(
            mul(r, add(mul(3, height(r))?, 2)?)?,
            mul(
                new_records,
                add(mul(2, height(add(retained, new_records)?))?, 1)?,
            )?,
        )?,
        1,
    )?;
    let normal = add(add(add(app, 1)?, main)?, tail)?;
    let current_regions = u64::from(geometry.regions);
    let current_tracker = u64::from(geometry.tracker_regions);
    let small = add(
        mul(2, geometry.usable_pages)?,
        mul(mul(2, largest)?, add(normal, 3)?)?,
    )?;
    let (regions, usable, tracker) = if current_regions == 1 && small < REGION {
        (1, small, current_tracker)
    } else {
        let regions = add(
            add(current_regions, 2)?,
            mul(largest, normal)?.div_ceil(REGION),
        )?;
        (regions, mul(REGION, regions)?, current_tracker.max(regions))
    };
    checked_file_upper(usable, regions, tracker)?;
    let close_records = add(retained, new_records)?;
    let close_new = add(nodes(close_records)?, 1)?.div_ceil(200);
    let close_body = add(
        add(1, mul(retained, add(mul(3, height(close_records))?, 2)?)?)?,
        mul(
            close_new,
            add(mul(2, height(add(new_records, close_new)?))?, 1)?,
        )?,
    )?;
    let lower = geometry.usable_pages.div_ceil(2);
    let mut q = 8;
    // A failed self-size trial only grows the integer order. The reviewed
    // general certificate requires 5*q < M; no native work has started here.
    let (close_usable, close_regions, close_tracker, value_bytes, passes) = loop {
        if mul(5, q)? >= REGION {
            return Err(anyerr!(
                "private close envelope is outside the sufficient certificate"
            ));
        }
        let small_close = add(mul(2, usable)?, mul(mul(2, q)?, add(close_body, 18)?)?)?;
        let (u, g, h, v, passes) = if regions == 1 && small_close < REGION {
            checked_file_upper(small_close, 1, tracker)?;
            let v = buddy_bytes(small_close)?
                .max(tracker_bytes(tracker)?)
                .max(8);
            (small_close, 1, tracker, v, height(small_close / lower))
        } else {
            let numerator = add(
                mul(REGION, add(regions, 2)?)?,
                mul(q, add(10, close_body)?)?,
            )?;
            let g = numerator.div_ceil(REGION - mul(5, q)?);
            let h = tracker.max(g);
            let u = mul(REGION, g)?;
            checked_file_upper(u, g, h)?;
            let v = buddy_bytes(REGION)?.max(tracker_bytes(h)?).max(8);
            let lower_regions = lower.div_ceil(REGION).max(1);
            let doubling = if lower >= REGION {
                0
            } else {
                (REGION.div_ceil(lower)).next_power_of_two().ilog2().into()
            };
            let passes = add(
                add(
                    doubling,
                    g.checked_sub(lower_regions)
                        .ok_or_else(|| anyerr!("private close lower geometry mismatch"))?,
                )?,
                2,
            )?;
            (u, g, h, v, passes)
        };
        let required = round_pages(add(v, 13)?)?.max(8);
        if required <= q {
            break (u, g, h, v, passes);
        }
        q = required;
    };
    next.selected_system_records = retained;
    next.retained_system_records = new_records;
    Ok(Envelope {
        normal_page_requests: normal,
        tail_page_requests: tail,
        retired_user_handles: retired,
        new_system_records: new_records,
        usable_page_high_water: usable.max(close_usable),
        region_high_water: regions.max(close_regions),
        tracker_high_water: tracker.max(close_tracker),
        close_page_order: q,
        close_passes: passes,
        close_serialized_value_bytes: value_bytes,
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_close_geometry_must_fit_native_region_and_file_domain() -> Result<()> {
        // A conditional permitted input to the certificate, not a claim that
        // a fresh private owner has reached this geometry in a runtime test.
        let regions = 1_048_000u32;
        let region_count = u64::from(regions);
        let usable = mul(REGION, region_count)?;
        let geometry = Geometry {
            page_bytes: 4096,
            region_page_capacity: 1 << 20,
            usable_pages: usable,
            regions,
            tracker_regions: regions,
            file_bytes: checked_file_upper(usable, region_count, region_count)?,
        };
        let families = [1, 16, 16].map(|leaf_pages| Family {
            leaf_pages,
            ..Family::default()
        });
        assert!(estimate(geometry, History::default(), families).is_err());
        let maximum_file = checked_file_upper(mul(REGION, MAX_REGIONS)?, MAX_REGIONS, MAX_REGIONS)?;
        assert_eq!(
            maximum_file,
            add(PAGE, mul(MAX_REGIONS, mul(REGION, PAGE)?)?)?
        );
        assert!(
            checked_file_upper(mul(REGION, MAX_REGIONS)?, MAX_REGIONS, MAX_REGIONS + 1).is_err()
        );
        assert!(
            checked_file_upper(mul(REGION, MAX_REGIONS)? + 1, MAX_REGIONS, MAX_REGIONS).is_err()
        );
        Ok(())
    }

    #[test]
    fn default_inline_geometry_and_retained_generations() -> Result<()> {
        assert_eq!(leaf_pages(16 * 1024)?, 16);
        assert_eq!(leaf_pages(13)?, 1);
        assert_eq!(buddy_bytes(REGION)?, 266960);
        let seed = Geometry {
            page_bytes: 4096,
            region_page_capacity: 1 << 20,
            usable_pages: 39,
            regions: 1,
            tracker_regions: 1000,
            file_bytes: 40 * 4096,
        };
        let history = History {
            selected_system_records: 7,
            retained_system_records: 3,
            ..History::default()
        };
        let families = [
            Family {
                leaf_pages: 1,
                inserts: 1,
                ..Family::default()
            },
            Family {
                leaf_pages: 16,
                ..Family::default()
            },
            Family {
                leaf_pages: 16,
                ..Family::default()
            },
        ];
        let envelope = estimate(seed, history, families)?;
        assert_eq!(envelope.next.selected_system_records, 3);
        assert_eq!(
            envelope.next.retained_system_records,
            envelope.new_system_records
        );
        assert_eq!(envelope.next.entries, [1, 0, 0]);
        assert!(envelope.usable_page_high_water >= 39);
        assert!(envelope.tracker_high_water >= 1000);
        assert!(envelope.close_page_order >= 8);
        Ok(())
    }

    #[test]
    fn shape_and_history_overflow_fail_before_native_mutation() {
        assert!(leaf_pages(u64::MAX).is_err());
        let geometry = Geometry {
            page_bytes: 4096,
            region_page_capacity: 1 << 20,
            usable_pages: 39,
            regions: 1,
            tracker_regions: 1000,
            file_bytes: 40 * 4096,
        };
        let history = History {
            entries: [u64::MAX, 0, 0],
            ..History::default()
        };
        let family = Family {
            entries: u64::MAX,
            inserts: 1,
            leaf_pages: 1,
        };
        assert!(estimate(
            geometry,
            history,
            [
                family,
                Family {
                    leaf_pages: 16,
                    ..Family::default()
                },
                Family {
                    leaf_pages: 16,
                    ..Family::default()
                }
            ]
        )
        .is_err());
    }
}
