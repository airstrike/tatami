//! `Query → ResolvedQuery` transformation — Phase 5c of MAP_PLAN.md §5.
//!
//! Public [`Query`] values are shape-valid by construction (see `tatami`'s
//! opaque constructors: `Name::parse`, `Path::of`, `Tuple::of`, the `Axes`
//! variant closure). What they do *not* carry is proof that every `Name`
//! they embed resolves against a concrete [`Schema`] + member catalogue.
//!
//! This module closes that gap. [`resolve`] walks a query top-down and
//! either returns a [`ResolvedQuery`] — whose ref-bearing fields have been
//! replaced by direct handles into the schema or catalogue — or the first
//! resolution error it encounters. Evaluation code in Phase 5d–g operates
//! on `&ResolvedQuery<'_>` and cannot be called with an un-resolved
//! [`Query`]; `Result` appears exactly here, per §3.6.
//!
//! The resolved tree stores shape; Phase 5d–g consumes the fields. Until
//! that wiring lands, a module-scoped `allow(dead_code)` keeps the build
//! warning-free; individual resolution tests still exercise the fields
//! via pattern matching.
#![allow(dead_code)]

use std::collections::HashSet;
use std::num::NonZeroUsize;

use tatami::query::{Axes, MemberRef, Path, Predicate, Set, Tuple};
use tatami::schema::{
    Dimension, Hierarchy, Level, Measure, Metric, Name, NamedSet, Schema, dimension, metric,
};
use tatami::{Query, query};

use crate::Error;
use crate::catalogue::Catalogue;

/// Zero-cost handle to a [`Dimension`] resolved against a schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DimHandle<'s> {
    /// The borrowed dimension definition.
    pub(crate) dim: &'s Dimension,
}

/// Zero-cost handle to a [`Hierarchy`] resolved against a schema's dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct HierarchyHandle<'s> {
    /// The borrowed hierarchy definition.
    pub(crate) hierarchy: &'s Hierarchy,
}

/// Zero-cost handle to a [`Level`] within a hierarchy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LevelHandle<'s> {
    /// The borrowed level definition.
    pub(crate) level: &'s Level,
    /// 0-based index of the level within its hierarchy. Useful for
    /// `Catalogue::members_at` and for depth-based navigation.
    pub(crate) index: usize,
}

/// Either a measure or a metric, resolved by name.
///
/// `Query.metrics[*]` and `OrderBy.metric` both hold a bare [`Name`] that
/// could point at either kind; [`MetricHandle`] records which.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub(crate) enum MetricHandle<'s> {
    /// The name resolved to a stored measure.
    Measure(&'s Measure),
    /// The name resolved to a derived metric (formula).
    Metric(&'s Metric),
}

/// A query that has been bound to a specific schema and member catalogue.
///
/// Every value of this type satisfies §3.6's bullet list (refs resolve,
/// cross-joins are disjoint, `Lag` is over a Time dim, etc.). Eval takes
/// `&ResolvedQuery<'_>` only — it cannot be called with a raw [`Query`].
#[derive(Debug)]
pub(crate) struct ResolvedQuery<'s> {
    /// The schema this query was resolved against.
    pub(crate) schema: &'s Schema,
    /// The per-hierarchy member catalogue.
    pub(crate) catalogue: &'s Catalogue,
    /// Axis projection, resolved.
    pub(crate) axes: ResolvedAxes<'s>,
    /// Slicer tuple, resolved.
    pub(crate) slicer: ResolvedTuple<'s>,
    /// Metrics to evaluate per cell, resolved.
    pub(crate) metrics: Vec<MetricHandle<'s>>,
    /// Query-level tuning knobs. Carried through verbatim; `options.order`'s
    /// metrics have been validated during resolution but are kept as
    /// [`query::Options`] to avoid duplicating the ordering struct.
    pub(crate) options: query::Options,
}

/// Mirror of the public [`Axes`] sum with resolved-set payloads.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ResolvedAxes<'s> {
    /// Zero axes.
    Scalar,
    /// One axis — rows.
    Series {
        /// Rows axis.
        rows: ResolvedSet<'s>,
    },
    /// Two axes — rows × columns.
    Pivot {
        /// Rows axis.
        rows: ResolvedSet<'s>,
        /// Columns axis.
        columns: ResolvedSet<'s>,
    },
    /// Three axes — rows × columns × pages.
    Pages {
        /// Rows axis.
        rows: ResolvedSet<'s>,
        /// Columns axis.
        columns: ResolvedSet<'s>,
        /// Pages axis.
        pages: ResolvedSet<'s>,
    },
}

/// A set-algebra term whose dim/hierarchy/level/member refs all resolve
/// against the schema + catalogue.
///
/// Variants mirror the public [`Set`]; only the name payloads are replaced
/// by resolved handles.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ResolvedSet<'s> {
    /// All members of `dim`'s `hierarchy` at `level`.
    Members {
        /// The dimension.
        dim: DimHandle<'s>,
        /// The hierarchy within the dimension.
        hierarchy: HierarchyHandle<'s>,
        /// The level within the hierarchy.
        level: LevelHandle<'s>,
    },
    /// Inclusive range between two members at the same level.
    Range {
        /// The dimension.
        dim: DimHandle<'s>,
        /// The hierarchy within the dimension.
        hierarchy: HierarchyHandle<'s>,
        /// Lower endpoint.
        from: ResolvedMember<'s>,
        /// Upper endpoint.
        to: ResolvedMember<'s>,
    },
    /// Reference to a named set — the inner expression has been resolved
    /// recursively.
    Named {
        /// The resolved inner set expression.
        set: Box<ResolvedSet<'s>>,
    },
    /// Explicit list of members.
    Explicit {
        /// The members.
        members: Vec<ResolvedMember<'s>>,
    },
    /// Immediate children of every member in `of`.
    Children {
        /// Parent set.
        of: Box<ResolvedSet<'s>>,
    },
    /// All descendants of `of`, down to `to_level`.
    Descendants {
        /// Ancestor set.
        of: Box<ResolvedSet<'s>>,
        /// Target level.
        to_level: LevelHandle<'s>,
    },
    /// Filter by a predicate.
    Filter {
        /// Source set.
        set: Box<ResolvedSet<'s>>,
        /// Predicate.
        pred: ResolvedPredicate<'s>,
    },
    /// Top-N tuples by metric.
    TopN {
        /// Source set.
        set: Box<ResolvedSet<'s>>,
        /// Number of tuples to retain.
        n: NonZeroUsize,
        /// Metric to rank by.
        by: MetricHandle<'s>,
    },
    /// Cartesian product of two sets addressing disjoint dims.
    CrossJoin {
        /// Left operand.
        left: Box<ResolvedSet<'s>>,
        /// Right operand.
        right: Box<ResolvedSet<'s>>,
    },
    /// Union of two sets addressing identical dims.
    Union {
        /// Left operand.
        left: Box<ResolvedSet<'s>>,
        /// Right operand.
        right: Box<ResolvedSet<'s>>,
    },
}

/// A member reference whose `dim`/`hierarchy` have been resolved and whose
/// `path` has been validated against the catalogue.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedMember<'s> {
    /// The dimension.
    pub(crate) dim: DimHandle<'s>,
    /// The hierarchy within the dimension.
    pub(crate) hierarchy: HierarchyHandle<'s>,
    /// The path within the hierarchy. Owned rather than borrowed — owning
    /// avoids threading a path-lifetime through the resolved tree, and paths
    /// are cheap (`Name` is a small opaque owning type).
    pub(crate) path: Path,
}

/// Resolved counterpart of [`Tuple`] — every member has been located in the
/// catalogue.
///
/// `Eq` / `Hash` delegate through `members`, which lets Phase 5d dedup
/// tuples in `Union` evaluation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResolvedTuple<'s> {
    /// The members; per `Tuple::of`, dims are already distinct.
    pub(crate) members: Vec<ResolvedMember<'s>>,
}

impl<'s> ResolvedTuple<'s> {
    /// Construct a resolved tuple from a member list. Phase 5d calls this
    /// when assembling cross-join products and flat one-member tuples.
    pub(crate) fn from_members(members: Vec<ResolvedMember<'s>>) -> Self {
        Self { members }
    }
}

/// Resolved counterpart of [`Predicate`] — metric refs and dims have been
/// located.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ResolvedPredicate<'s> {
    /// `metric == value`.
    Eq {
        /// The metric handle.
        metric: MetricHandle<'s>,
        /// Comparison target.
        value: f64,
    },
    /// `metric > value`.
    Gt {
        /// The metric handle.
        metric: MetricHandle<'s>,
        /// Comparison target.
        value: f64,
    },
    /// `metric < value`.
    Lt {
        /// The metric handle.
        metric: MetricHandle<'s>,
        /// Comparison target.
        value: f64,
    },
    /// The filtered member's coordinate on `dim` starts with `path_prefix`.
    In {
        /// The dimension.
        dim: DimHandle<'s>,
        /// Path prefix to match.
        path_prefix: Path,
    },
    /// Negation of [`ResolvedPredicate::In`].
    NotIn {
        /// The dimension.
        dim: DimHandle<'s>,
        /// Path prefix that must *not* match.
        path_prefix: Path,
    },
}

impl<'s> ResolvedSet<'s> {
    /// The set of dimension names this resolved set addresses.
    ///
    /// - [`ResolvedSet::CrossJoin`] is the disjoint union of its sides.
    /// - [`ResolvedSet::Union`] must have matching dim-sets on both sides
    ///   (enforced at resolve time); return either side's dims.
    /// - [`ResolvedSet::Filter`], [`ResolvedSet::TopN`], and
    ///   [`ResolvedSet::Children`] / [`ResolvedSet::Descendants`] delegate
    ///   to their inner set.
    /// - [`ResolvedSet::Named`] delegates to its inner expression.
    /// - [`ResolvedSet::Members`], [`ResolvedSet::Range`],
    ///   [`ResolvedSet::Explicit`] are single-dim (for the simple
    ///   single-hierarchy case this phase supports).
    pub(crate) fn dims(&self) -> Vec<Name> {
        match self {
            Self::Members { dim, .. } | Self::Range { dim, .. } => vec![dim.dim.name.clone()],
            Self::Explicit { members } => {
                // `members` may span multiple dims if a caller hand-built an
                // explicit set across dims; dedup while preserving insertion
                // order so single-dim Explicit sets stay single-dim.
                let mut out: Vec<Name> = Vec::new();
                for m in members {
                    let name = m.dim.dim.name.clone();
                    if !out.contains(&name) {
                        out.push(name);
                    }
                }
                out
            }
            Self::Named { set }
            | Self::Children { of: set }
            | Self::Descendants { of: set, .. }
            | Self::Filter { set, .. }
            | Self::TopN { set, .. } => set.dims(),
            Self::CrossJoin { left, right } | Self::Union { left, right } => {
                let mut out = left.dims();
                for d in right.dims() {
                    if !out.contains(&d) {
                        out.push(d);
                    }
                }
                out
            }
        }
    }

    /// The output level of a resolved set, if well-defined.
    ///
    /// `None` for cross-joins and heterogeneous compositions. Used by
    /// [`ResolvedSet::Descendants`]' level-below-source check and as a
    /// sanity signal for [`ResolvedSet::Children`].
    pub(crate) fn output_level(&self) -> Option<LevelHandle<'s>> {
        match self {
            Self::Members { level, .. } => Some(*level),
            Self::Range {
                from, hierarchy, ..
            } => {
                // `from` and `to` are at the same level (enforced in
                // `resolve_set`); pick `from`'s level.
                level_at_depth(hierarchy, from.path.len())
            }
            Self::Explicit { members } => {
                let first = members.first()?;
                let depth = first.path.len();
                // All members must share level for a single output level.
                if members
                    .iter()
                    .any(|m| m.path.len() != depth || m.dim.dim.name != first.dim.dim.name)
                {
                    return None;
                }
                level_at_depth(&first.hierarchy, depth)
            }
            Self::Named { set } | Self::Filter { set, .. } | Self::TopN { set, .. } => {
                set.output_level()
            }
            Self::Descendants { to_level, .. } => Some(*to_level),
            Self::Children { of } => {
                // Children are one level below `of`'s output level.
                let parent = of.output_level()?;
                // Need the hierarchy of `of` to look up the child level —
                // only available when `of` is single-dim.
                let hierarchy = set_hierarchy(of)?;
                level_at_depth(&hierarchy, parent.index + 2)
            }
            Self::Union { left, right } => {
                let l = left.output_level()?;
                let r = right.output_level()?;
                if l.level.name == r.level.name {
                    Some(l)
                } else {
                    None
                }
            }
            Self::CrossJoin { .. } => None,
        }
    }
}

/// The single hierarchy a resolved set addresses, if single-dim.
fn set_hierarchy<'s>(set: &ResolvedSet<'s>) -> Option<HierarchyHandle<'s>> {
    match set {
        ResolvedSet::Members { hierarchy, .. } | ResolvedSet::Range { hierarchy, .. } => {
            Some(*hierarchy)
        }
        ResolvedSet::Explicit { members } => members.first().map(|m| m.hierarchy),
        ResolvedSet::Named { set }
        | ResolvedSet::Children { of: set }
        | ResolvedSet::Descendants { of: set, .. }
        | ResolvedSet::Filter { set, .. }
        | ResolvedSet::TopN { set, .. } => set_hierarchy(set),
        ResolvedSet::Union { left, right } => {
            let l = set_hierarchy(left)?;
            let r = set_hierarchy(right)?;
            if l.hierarchy.name == r.hierarchy.name {
                Some(l)
            } else {
                None
            }
        }
        ResolvedSet::CrossJoin { .. } => None,
    }
}

/// Level at the given 1-based depth within a hierarchy. `depth == 0` is the
/// virtual root (no level); `depth == 1` is the top level, etc.
fn level_at_depth<'s>(hierarchy: &HierarchyHandle<'s>, depth: usize) -> Option<LevelHandle<'s>> {
    if depth == 0 {
        return None;
    }
    let index = depth - 1;
    hierarchy
        .hierarchy
        .levels
        .get(index)
        .map(|level| LevelHandle { level, index })
}

/// Resolve a public [`Query`] against a schema and member catalogue.
///
/// Walks the query top-down and fails on the first resolution error — a
/// "collect all errors then report" mode is out of scope for v0.1.
///
/// Succeeds iff §3.6's invariant bullet list holds:
/// - every `Expr::Ref` / `Query.metrics[*]` / predicate metric resolves;
/// - every `MemberRef` locates a member in the catalogue;
/// - every `Set::CrossJoin` addresses disjoint dims;
/// - every `Set::Union` addresses identical dims;
/// - every `Expr::Lag` names a Time-kind dim;
/// - every `Expr::PeriodsToDate` level lives in a Time-kind hierarchy;
/// - named sets resolve and do not cycle.
pub(crate) fn resolve<'s>(
    q: &Query,
    schema: &'s Schema,
    catalogue: &'s Catalogue,
) -> Result<ResolvedQuery<'s>, Error> {
    let ctx = Ctx { schema, catalogue };
    let axes = resolve_axes(&ctx, &q.axes)?;
    let slicer = resolve_tuple(&ctx, &q.slicer)?;
    let metrics = q
        .metrics
        .iter()
        .map(|name| resolve_metric_ref(&ctx, name))
        .collect::<Result<Vec<_>, _>>()?;
    // Walk each resolved metric's expr tree for Time-kind / level
    // membership checks that the schema builder does not perform.
    for handle in &metrics {
        if let MetricHandle::Metric(metric) = handle {
            check_expr(&ctx, &metric.expr)?;
        }
    }
    // Validate `options.order[*].metric` references.
    for ob in &q.options.order {
        let _ = resolve_metric_ref(&ctx, &ob.metric)?;
    }
    Ok(ResolvedQuery {
        schema,
        catalogue,
        axes,
        slicer,
        metrics,
        options: q.options.clone(),
    })
}

/// Borrowed resolution context. Keeps the schema and catalogue in one
/// place so helper fns don't need five parameters each.
struct Ctx<'s> {
    schema: &'s Schema,
    catalogue: &'s Catalogue,
}

impl<'s> Ctx<'s> {
    fn dim(&self, name: &Name) -> Result<DimHandle<'s>, Error> {
        self.schema
            .dimensions
            .iter()
            .find(|d| d.name == *name)
            .map(|dim| DimHandle { dim })
            .ok_or_else(|| Error::ResolveUnknownDimension { dim: name.clone() })
    }

    fn hierarchy(&self, dim: &DimHandle<'s>, name: &Name) -> Result<HierarchyHandle<'s>, Error> {
        dim.dim
            .hierarchies
            .iter()
            .find(|h| h.name == *name)
            .map(|hierarchy| HierarchyHandle { hierarchy })
            .ok_or_else(|| Error::ResolveUnknownHierarchy {
                dim: dim.dim.name.clone(),
                hierarchy: name.clone(),
            })
    }

    fn level(
        &self,
        dim: &DimHandle<'s>,
        hierarchy: &HierarchyHandle<'s>,
        name: &Name,
    ) -> Result<LevelHandle<'s>, Error> {
        hierarchy
            .hierarchy
            .levels
            .iter()
            .enumerate()
            .find(|(_, lvl)| lvl.name == *name)
            .map(|(index, level)| LevelHandle { level, index })
            .ok_or_else(|| Error::ResolveUnknownLevel {
                dim: dim.dim.name.clone(),
                hierarchy: hierarchy.hierarchy.name.clone(),
                level: name.clone(),
            })
    }

    fn named_set(&self, name: &Name) -> Result<&'s NamedSet, Error> {
        self.schema
            .named_sets
            .iter()
            .find(|ns| ns.name == *name)
            .ok_or_else(|| Error::ResolveUnknownNamedSet { name: name.clone() })
    }
}

fn resolve_metric_ref<'s>(ctx: &Ctx<'s>, name: &Name) -> Result<MetricHandle<'s>, Error> {
    let as_measure = ctx.schema.measures.iter().find(|m| m.name == *name);
    let as_metric = ctx.schema.metrics.iter().find(|m| m.name == *name);
    match (as_measure, as_metric) {
        (Some(measure), None) => Ok(MetricHandle::Measure(measure)),
        (None, Some(metric)) => Ok(MetricHandle::Metric(metric)),
        // Schema builder rejects measure/metric collisions at build time; if
        // both match here, surface a dedicated error rather than silently
        // preferring one side.
        (Some(_), Some(_)) => Err(Error::ResolveAmbiguousRef { name: name.clone() }),
        (None, None) => Err(Error::ResolveUnresolvedRef { name: name.clone() }),
    }
}

fn resolve_axes<'s>(ctx: &Ctx<'s>, axes: &Axes) -> Result<ResolvedAxes<'s>, Error> {
    Ok(match axes {
        Axes::Scalar => ResolvedAxes::Scalar,
        Axes::Series { rows } => ResolvedAxes::Series {
            rows: resolve_set(ctx, rows, &mut NamedSetTrail::new())?,
        },
        Axes::Pivot { rows, columns } => ResolvedAxes::Pivot {
            rows: resolve_set(ctx, rows, &mut NamedSetTrail::new())?,
            columns: resolve_set(ctx, columns, &mut NamedSetTrail::new())?,
        },
        Axes::Pages {
            rows,
            columns,
            pages,
        } => ResolvedAxes::Pages {
            rows: resolve_set(ctx, rows, &mut NamedSetTrail::new())?,
            columns: resolve_set(ctx, columns, &mut NamedSetTrail::new())?,
            pages: resolve_set(ctx, pages, &mut NamedSetTrail::new())?,
        },
        // `Axes` is `#[non_exhaustive]` in the public crate; reject unknown
        // future variants explicitly rather than panicking.
        _ => {
            return Err(Error::ResolveSetCompositionIllFormed {
                reason: "unknown Axes variant",
            });
        }
    })
}

/// Trail of named-set names currently being resolved; used to detect
/// `Set::Named` cycles.
struct NamedSetTrail<'a> {
    stack: Vec<&'a Name>,
}

impl<'a> NamedSetTrail<'a> {
    fn new() -> Self {
        Self { stack: Vec::new() }
    }

    fn contains(&self, name: &Name) -> bool {
        self.stack.iter().any(|n| **n == *name)
    }
}

fn resolve_set<'s>(
    ctx: &Ctx<'s>,
    set: &Set,
    trail: &mut NamedSetTrail<'s>,
) -> Result<ResolvedSet<'s>, Error> {
    match set {
        Set::Members {
            dim,
            hierarchy,
            level,
        } => {
            let dim = ctx.dim(dim)?;
            let hierarchy = ctx.hierarchy(&dim, hierarchy)?;
            let level = ctx.level(&dim, &hierarchy, level)?;
            Ok(ResolvedSet::Members {
                dim,
                hierarchy,
                level,
            })
        }
        Set::Range {
            dim,
            hierarchy,
            from,
            to,
        } => {
            let dim_h = ctx.dim(dim)?;
            let hierarchy_h = ctx.hierarchy(&dim_h, hierarchy)?;
            let from_r = resolve_member_ref(ctx, from)?;
            let to_r = resolve_member_ref(ctx, to)?;
            if from_r.path.len() != to_r.path.len() {
                let from_level = level_at_depth(&hierarchy_h, from_r.path.len())
                    .map(|l| l.level.name.clone())
                    .unwrap_or_else(|| from.path.head().clone());
                let to_level = level_at_depth(&hierarchy_h, to_r.path.len())
                    .map(|l| l.level.name.clone())
                    .unwrap_or_else(|| to.path.head().clone());
                return Err(Error::ResolveRangeMembersAtDifferentLevels {
                    from_level,
                    to_level,
                });
            }
            if from_r.dim.dim.name != dim_h.dim.name
                || to_r.dim.dim.name != dim_h.dim.name
                || from_r.hierarchy.hierarchy.name != hierarchy_h.hierarchy.name
                || to_r.hierarchy.hierarchy.name != hierarchy_h.hierarchy.name
            {
                // The endpoint refs' own dim/hierarchy must match the outer
                // Range's — `from` and `to` can't be on a different axis.
                return Err(Error::ResolveSetCompositionIllFormed {
                    reason: "range endpoints must live in the outer dim and hierarchy",
                });
            }
            Ok(ResolvedSet::Range {
                dim: dim_h,
                hierarchy: hierarchy_h,
                from: from_r,
                to: to_r,
            })
        }
        Set::Named { name } => {
            if trail.contains(name) {
                return Err(Error::ResolveNamedSetCycle { name: name.clone() });
            }
            let ns = ctx.named_set(name)?;
            trail.stack.push(&ns.name);
            let inner = resolve_set(ctx, &ns.set, trail);
            trail.stack.pop();
            Ok(ResolvedSet::Named {
                set: Box::new(inner?),
            })
        }
        Set::Explicit { members } => {
            let resolved: Vec<ResolvedMember> = members
                .iter()
                .map(|m| resolve_member_ref(ctx, m))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedSet::Explicit { members: resolved })
        }
        Set::Children { of } => {
            let inner = resolve_set(ctx, of, trail)?;
            // Require `of`'s output level be well-defined — otherwise
            // "children of" is ambiguous.
            if inner.output_level().is_none() {
                return Err(Error::ResolveSetCompositionIllFormed {
                    reason: "Children requires a set with a single output level",
                });
            }
            Ok(ResolvedSet::Children {
                of: Box::new(inner),
            })
        }
        Set::Descendants { of, to_level } => {
            let inner = resolve_set(ctx, of, trail)?;
            let source_level =
                inner
                    .output_level()
                    .ok_or(Error::ResolveSetCompositionIllFormed {
                        reason: "Descendants requires a set with a single output level",
                    })?;
            let hierarchy = set_hierarchy(&inner).ok_or(Error::ResolveSetCompositionIllFormed {
                reason: "Descendants requires a single-hierarchy source set",
            })?;
            // Resolve `to_level` as a level name within that hierarchy.
            let dims = inner.dims();
            let dim_name = dims.first().ok_or(Error::ResolveSetCompositionIllFormed {
                reason: "Descendants source set has no addressed dim",
            })?;
            let dim_h = ctx.dim(dim_name)?;
            let to_level_h = ctx.level(&dim_h, &hierarchy, to_level)?;
            if to_level_h.index <= source_level.index {
                return Err(Error::ResolveDescendantsLevelNotBelow {
                    set_level: source_level.level.name.clone(),
                    to_level: to_level_h.level.name.clone(),
                });
            }
            Ok(ResolvedSet::Descendants {
                of: Box::new(inner),
                to_level: to_level_h,
            })
        }
        Set::Filter { set, pred } => {
            let inner = resolve_set(ctx, set, trail)?;
            let pred_r = resolve_predicate(ctx, pred)?;
            Ok(ResolvedSet::Filter {
                set: Box::new(inner),
                pred: pred_r,
            })
        }
        Set::TopN { set, n, by } => {
            let inner = resolve_set(ctx, set, trail)?;
            let by_h = resolve_metric_ref(ctx, by)?;
            Ok(ResolvedSet::TopN {
                set: Box::new(inner),
                n: *n,
                by: by_h,
            })
        }
        Set::CrossJoin { left, right } => {
            let l = resolve_set(ctx, left, trail)?;
            let r = resolve_set(ctx, right, trail)?;
            let l_dims: HashSet<Name> = l.dims().into_iter().collect();
            for d in r.dims() {
                if l_dims.contains(&d) {
                    return Err(Error::ResolveCrossJoinDimsOverlap { dim: d });
                }
            }
            Ok(ResolvedSet::CrossJoin {
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        Set::Union { left, right } => {
            let l = resolve_set(ctx, left, trail)?;
            let r = resolve_set(ctx, right, trail)?;
            let mut l_dims = l.dims();
            let mut r_dims = r.dims();
            l_dims.sort();
            r_dims.sort();
            if l_dims != r_dims {
                return Err(Error::ResolveUnionDimsMismatch {
                    left_dims: l_dims,
                    right_dims: r_dims,
                });
            }
            Ok(ResolvedSet::Union {
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        // `Set` is `#[non_exhaustive]`; reject unknown future variants
        // rather than panicking.
        _ => Err(Error::ResolveSetCompositionIllFormed {
            reason: "unknown Set variant",
        }),
    }
}

fn resolve_member_ref<'s>(ctx: &Ctx<'s>, m: &MemberRef) -> Result<ResolvedMember<'s>, Error> {
    let dim = ctx.dim(&m.dim)?;
    let hierarchy = ctx.hierarchy(&dim, &m.hierarchy)?;
    // Structural check: path length must fit in the hierarchy.
    if m.path.len() > hierarchy.hierarchy.levels.len() {
        return Err(Error::ResolveUnknownMember {
            dim: m.dim.clone(),
            hierarchy: m.hierarchy.clone(),
            path: m.path.clone(),
        });
    }
    // Catalogue check: the path must locate a real node. `path_exists`
    // returns `None` only if the catalogue lacks the `(dim, hierarchy)`
    // pair, which the hierarchy lookup above already ruled out — so treat
    // `None` as a catalogue/schema mismatch-shaped `UnknownMember`.
    let found = ctx
        .catalogue
        .path_exists(&m.dim, &m.hierarchy, &m.path)
        .unwrap_or(false);
    if !found {
        return Err(Error::ResolveUnknownMember {
            dim: m.dim.clone(),
            hierarchy: m.hierarchy.clone(),
            path: m.path.clone(),
        });
    }
    Ok(ResolvedMember {
        dim,
        hierarchy,
        path: m.path.clone(),
    })
}

fn resolve_tuple<'s>(ctx: &Ctx<'s>, tuple: &Tuple) -> Result<ResolvedTuple<'s>, Error> {
    let members = tuple
        .members()
        .iter()
        .map(|m| resolve_member_ref(ctx, m))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResolvedTuple { members })
}

fn resolve_predicate<'s>(ctx: &Ctx<'s>, pred: &Predicate) -> Result<ResolvedPredicate<'s>, Error> {
    match pred {
        Predicate::Eq { metric, value } => Ok(ResolvedPredicate::Eq {
            metric: resolve_metric_ref(ctx, metric)?,
            value: *value,
        }),
        Predicate::Gt { metric, value } => Ok(ResolvedPredicate::Gt {
            metric: resolve_metric_ref(ctx, metric)?,
            value: *value,
        }),
        Predicate::Lt { metric, value } => Ok(ResolvedPredicate::Lt {
            metric: resolve_metric_ref(ctx, metric)?,
            value: *value,
        }),
        Predicate::In { dim, path_prefix } => Ok(ResolvedPredicate::In {
            dim: ctx.dim(dim)?,
            path_prefix: path_prefix.clone(),
        }),
        Predicate::NotIn { dim, path_prefix } => Ok(ResolvedPredicate::NotIn {
            dim: ctx.dim(dim)?,
            path_prefix: path_prefix.clone(),
        }),
        // `Predicate` is `#[non_exhaustive]`; reject unknown future variants.
        _ => Err(Error::ResolveSetCompositionIllFormed {
            reason: "unknown Predicate variant",
        }),
    }
}

/// Walk a metric's [`metric::Expr`] tree to enforce the §3.6 invariants that
/// the schema builder does not check:
///
/// - every `Lag.dim` is a Time-kind dim;
/// - every `PeriodsToDate.level` appears in some Time-kind dim's hierarchy;
/// - every `At.at` tuple resolves (dims and hierarchies exist).
///
/// `Ref` existence is already guaranteed by the schema builder — we do not
/// re-verify it here.
fn check_expr(ctx: &Ctx<'_>, expr: &metric::Expr) -> Result<(), Error> {
    match expr {
        metric::Expr::Ref { .. } | metric::Expr::Const { .. } => Ok(()),
        metric::Expr::Binary { l, r, .. } => {
            check_expr(ctx, l)?;
            check_expr(ctx, r)
        }
        metric::Expr::Lag { of, dim, .. } => {
            let handle = ctx.dim(dim)?;
            if !matches!(handle.dim.kind, dimension::Kind::Time { .. }) {
                return Err(Error::ResolveLagDimNotTime { dim: dim.clone() });
            }
            check_expr(ctx, of)
        }
        metric::Expr::PeriodsToDate { of, level } => {
            let found_in_time = ctx.schema.dimensions.iter().any(|d| {
                matches!(d.kind, dimension::Kind::Time { .. })
                    && d.hierarchies
                        .iter()
                        .any(|h| h.levels.iter().any(|lvl| lvl.name == *level))
            });
            if !found_in_time {
                return Err(Error::ResolvePeriodsToDateLevelNotInTime {
                    level: level.clone(),
                });
            }
            check_expr(ctx, of)
        }
        metric::Expr::At { of, at } => {
            let _ = resolve_tuple(ctx, at)?;
            check_expr(ctx, of)
        }
        // `metric::Expr` is `#[non_exhaustive]`; be defensive against
        // future variants.
        _ => Err(Error::ResolveSetCompositionIllFormed {
            reason: "unknown metric::Expr variant",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_core::df;
    use polars_core::prelude::DataFrame;
    use tatami::query::{Axes, MemberRef, Options, Predicate, Set, Tuple};
    use tatami::schema::{
        Aggregation, Calendar, Dimension, Hierarchy, Level, Measure, Metric, NamedSet, Schema,
    };

    use crate::InMemoryCube;

    fn n(s: &str) -> Name {
        Name::parse(s).expect("valid name")
    }

    fn mr(dim: &str, hier: &str, head: &str) -> MemberRef {
        MemberRef::new(n(dim), n(hier), Path::of(n(head)))
    }

    /// Geography (two-level) + Time (Year → Quarter → Month) + amount.
    fn hewton_shaped_schema() -> Schema {
        Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Fiscal"))
                        .level(Level::new(n("Year"), n("year")))
                        .level(Level::new(n("Quarter"), n("quarter")))
                        .level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .build()
            .expect("schema valid")
    }

    fn hewton_shaped_frame() -> DataFrame {
        df! {
            "region"  => ["EMEA", "APAC"],
            "country" => ["UK", "JP"],
            "year"    => ["2026", "2026"],
            "quarter" => ["Q1", "Q1"],
            "month"   => ["Jan", "Jan"],
            "amount"  => [100.0_f64, 200.0],
        }
        .expect("frame valid")
    }

    fn hewton_cube() -> InMemoryCube {
        InMemoryCube::new(hewton_shaped_frame(), hewton_shaped_schema()).expect("cube")
    }

    #[test]
    fn resolves_scalar_query_against_hewton_schema() {
        let cube = hewton_cube();
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let rq = resolve(&q, &cube.schema, cube.catalogue()).expect("resolve ok");
        assert!(matches!(rq.axes, ResolvedAxes::Scalar));
        assert_eq!(rq.metrics.len(), 1);
        assert!(matches!(rq.metrics[0], MetricHandle::Measure(_)));
    }

    #[test]
    fn resolves_pivot_with_descendants_of_range() {
        // §3.5(b) shape: descendants of a time range, quarters on rows.
        let cube = hewton_cube();
        let rows = Set::range(
            n("Time"),
            n("Fiscal"),
            mr("Time", "Fiscal", "2026"),
            mr("Time", "Fiscal", "2026"),
        )
        .descendants_to(n("Quarter"));
        let columns = Set::members(n("Geography"), n("Default"), n("Region"));
        let q = Query {
            axes: Axes::Pivot { rows, columns },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let rq = resolve(&q, &cube.schema, cube.catalogue()).expect("resolve ok");
        assert!(matches!(rq.axes, ResolvedAxes::Pivot { .. }));
    }

    #[test]
    fn resolves_named_set_through_schema() {
        let schema = Schema::builder()
            .dimension(
                Dimension::regular(n("Geography")).hierarchy(
                    Hierarchy::new(n("Default"))
                        .level(Level::new(n("Region"), n("region")))
                        .level(Level::new(n("Country"), n("country"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(NamedSet::new(
                n("AllRegions"),
                Set::members(n("Geography"), n("Default"), n("Region")),
            ))
            .build()
            .expect("schema");
        let df = df! {
            "region"  => ["EMEA"],
            "country" => ["UK"],
            "amount"  => [1.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Series {
                rows: Set::named(n("AllRegions")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let rq = resolve(&q, &cube.schema, cube.catalogue()).expect("resolve ok");
        let ResolvedAxes::Series { rows } = rq.axes else {
            panic!("expected series");
        };
        assert!(matches!(rows, ResolvedSet::Named { .. }));
    }

    #[test]
    fn resolve_rejects_unknown_metric_ref() {
        let cube = hewton_cube();
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("NoSuchMetric")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("unresolved");
        match err {
            Error::ResolveUnresolvedRef { name } => assert_eq!(name.as_str(), "NoSuchMetric"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn resolve_rejects_lag_over_non_time_dim() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("Lagged"),
                metric::Expr::Lag {
                    of: Box::new(metric::Expr::Ref { name: n("amount") }),
                    dim: n("Geography"),
                    n: 1,
                },
            ))
            .build()
            .expect("schema");
        let df = df! {
            "region" => ["EMEA"],
            "amount" => [1.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("Lagged")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("non-time lag");
        match err {
            Error::ResolveLagDimNotTime { dim } => assert_eq!(dim.as_str(), "Geography"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn resolve_rejects_periods_to_date_with_level_outside_time() {
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .dimension(
                Dimension::time(n("Time"), vec![Calendar::gregorian(n("Gregorian"))]).hierarchy(
                    Hierarchy::new(n("Default")).level(Level::new(n("Month"), n("month"))),
                ),
            )
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .metric(Metric::new(
                n("PTD"),
                metric::Expr::PeriodsToDate {
                    of: Box::new(metric::Expr::Ref { name: n("amount") }),
                    // "Region" lives under Geography, not Time.
                    level: n("Region"),
                },
            ))
            .build()
            .expect("schema");
        let df = df! {
            "region" => ["EMEA"],
            "month"  => ["2026-01"],
            "amount" => [1.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::empty(),
            metrics: vec![n("PTD")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("ptd level");
        match err {
            Error::ResolvePeriodsToDateLevelNotInTime { level } => {
                assert_eq!(level.as_str(), "Region");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn resolve_rejects_crossjoin_with_overlapping_dims() {
        let cube = hewton_cube();
        // Two Geography sets crossed — same dim, not disjoint.
        let left = Set::members(n("Geography"), n("Default"), n("Region"));
        let right = Set::members(n("Geography"), n("Default"), n("Country"));
        let q = Query {
            axes: Axes::Series {
                rows: left.cross(right),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("overlap");
        match err {
            Error::ResolveCrossJoinDimsOverlap { dim } => assert_eq!(dim.as_str(), "Geography"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn resolve_rejects_union_with_mismatched_dims() {
        let cube = hewton_cube();
        let geo = Set::members(n("Geography"), n("Default"), n("Region"));
        let time = Set::members(n("Time"), n("Fiscal"), n("Year"));
        let q = Query {
            axes: Axes::Series {
                rows: geo.union(time),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("union dims");
        assert!(matches!(err, Error::ResolveUnionDimsMismatch { .. }));
    }

    #[test]
    fn resolve_rejects_unknown_member() {
        // A path longer than the hierarchy's level depth trips the
        // structural member check.
        let cube = hewton_cube();
        let deep_path = Path::with(n("EMEA"), vec![n("UK"), n("London")]);
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::single(MemberRef::new(n("Geography"), n("Default"), deep_path)),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("too-deep path");
        assert!(matches!(err, Error::ResolveUnknownMember { .. }));
    }

    #[test]
    fn resolve_rejects_unknown_level() {
        let cube = hewton_cube();
        let q = Query {
            axes: Axes::Series {
                rows: Set::members(n("Geography"), n("Default"), n("NoSuchLevel")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("unknown level");
        match err {
            Error::ResolveUnknownLevel { level, .. } => assert_eq!(level.as_str(), "NoSuchLevel"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn resolve_rejects_descendants_with_to_level_above_set_level() {
        let cube = hewton_cube();
        // Start at Country (leaf) and try to descend to Region (higher).
        let of = Set::members(n("Geography"), n("Default"), n("Country"));
        let q = Query {
            axes: Axes::Series {
                rows: of.descendants_to(n("Region")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("to_level above");
        assert!(matches!(err, Error::ResolveDescendantsLevelNotBelow { .. }));
    }

    #[test]
    fn resolve_rejects_range_at_different_levels() {
        let cube = hewton_cube();
        let from = MemberRef::new(n("Time"), n("Fiscal"), Path::of(n("2026")));
        // "2026/Q1" exists in the hewton fixture; "2026" is one level up.
        let to = MemberRef::new(n("Time"), n("Fiscal"), Path::with(n("2026"), vec![n("Q1")]));
        let q = Query {
            axes: Axes::Series {
                rows: Set::range(n("Time"), n("Fiscal"), from, to),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("range levels");
        assert!(matches!(
            err,
            Error::ResolveRangeMembersAtDifferentLevels { .. }
        ));
    }

    #[test]
    fn resolve_rejects_cyclic_named_set() {
        // NamedSet A references NamedSet B references A.
        let schema = Schema::builder()
            .dimension(Dimension::regular(n("Geography")).hierarchy(
                Hierarchy::new(n("Default")).level(Level::new(n("Region"), n("region"))),
            ))
            .measure(Measure::new(n("amount"), Aggregation::sum()))
            .named_set(NamedSet::new(n("A"), Set::named(n("B"))))
            .named_set(NamedSet::new(n("B"), Set::named(n("A"))))
            .build()
            .expect("schema");
        let df = df! {
            "region" => ["EMEA"],
            "amount" => [1.0_f64],
        }
        .expect("frame");
        let cube = InMemoryCube::new(df, schema).expect("cube");
        let q = Query {
            axes: Axes::Series {
                rows: Set::named(n("A")),
            },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("cycle");
        assert!(matches!(err, Error::ResolveNamedSetCycle { .. }));
    }

    #[test]
    fn resolve_rejects_member_absent_from_catalogue() {
        // "Mars" is a syntactically valid name and fits the hierarchy
        // depth, but the catalogue has no such region.
        let cube = hewton_cube();
        let q = Query {
            axes: Axes::Scalar,
            slicer: Tuple::single(MemberRef::new(
                n("Geography"),
                n("Default"),
                Path::of(n("Mars")),
            )),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let err = resolve(&q, &cube.schema, cube.catalogue()).expect_err("absent member");
        assert!(matches!(err, Error::ResolveUnknownMember { .. }));
    }

    #[test]
    fn resolve_accepts_filter_with_metric_predicate() {
        let cube = hewton_cube();
        let rows = Set::members(n("Geography"), n("Default"), n("Region")).filter(Predicate::Gt {
            metric: n("amount"),
            value: 0.0,
        });
        let q = Query {
            axes: Axes::Series { rows },
            slicer: Tuple::empty(),
            metrics: vec![n("amount")],
            options: Options::default(),
        };
        let _ = resolve(&q, &cube.schema, cube.catalogue()).expect("filter resolves");
    }
}
