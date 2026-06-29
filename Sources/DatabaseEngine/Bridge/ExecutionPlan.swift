// ExecutionPlan.swift
// DatabaseEngine - Canonical read execution plan.
//
// The planner materializes an `ExecutionPlan<T>` once from a `SelectQuery`.
// The executor then follows it mechanically: the plan encodes which index
// to use, which key fields are pinned (equality bindings), what range scan
// to perform, which direction to scan, and which residual work
// (filter / orderBy / limit / offset) the executor still has to carry out.
//
// This separation exists so the planner's decisions are inspectable
// (via `explain`) and reproducible across call sites. The executor must not
// make plan-level choices — if something cannot be honored, the plan is
// rejected at build time via `CanonicalReadError`.

import Foundation
import Core
import QueryIR

// MARK: - ExecutionPlan

/// Top-level execution plan for a canonical single-table read.
public enum ExecutionPlan<T: Persistable>: Sendable {
    /// Read through a named index with an optional range + direction.
    case indexAccess(IndexAccessPlan<T>)

    /// Full scan fallback. Used when no index is applicable.
    case fullScan(FullScanPlan<T>)

    /// Residual filter the executor must apply after the fetch.
    /// `nil` means the filter was fully pushed or was absent.
    public var residualFilter: QueryIR.Expression? {
        switch self {
        case .indexAccess(let plan): return plan.residualFilter
        case .fullScan(let plan): return plan.residualFilter
        }
    }

    /// Residual orderBy the executor must apply after the fetch.
    /// `nil` means the orderBy was fully pushed or was absent.
    public var residualOrderBy: [SortKey]? {
        switch self {
        case .indexAccess(let plan): return plan.residualOrderBy
        case .fullScan(let plan): return plan.residualOrderBy
        }
    }

    /// Limit the executor must enforce. `nil` means no limit.
    public var limit: Int? {
        switch self {
        case .indexAccess(let plan): return plan.limit
        case .fullScan(let plan): return plan.limit
        }
    }

    /// Offset the executor must enforce. `nil` means no offset.
    public var offset: Int? {
        switch self {
        case .indexAccess(let plan): return plan.offset
        case .fullScan(let plan): return plan.offset
        }
    }
}

// MARK: - IndexAccessPlan

/// Index-backed access plan.
///
/// `bindings` pin the leading key fields of the index to single values
/// (equality). `range` optionally constrains the first unpinned field.
/// `direction` is the scan direction along the index key order.
public struct IndexAccessPlan<T: Persistable>: Sendable {
    public var indexName: String
    public var bindings: [KeyFieldBinding]
    public var range: KeyRangeBound?
    public var direction: ScanDirection
    public var residualFilter: QueryIR.Expression?
    public var residualOrderBy: [SortKey]?
    public var limit: Int?
    public var offset: Int?

    public init(
        indexName: String,
        bindings: [KeyFieldBinding] = [],
        range: KeyRangeBound? = nil,
        direction: ScanDirection = .forward,
        residualFilter: QueryIR.Expression? = nil,
        residualOrderBy: [SortKey]? = nil,
        limit: Int? = nil,
        offset: Int? = nil
    ) {
        self.indexName = indexName
        self.bindings = bindings
        self.range = range
        self.direction = direction
        self.residualFilter = residualFilter
        self.residualOrderBy = residualOrderBy
        self.limit = limit
        self.offset = offset
    }
}

// MARK: - FullScanPlan

/// Full-scan access plan. No index pruning; every row in the collection is
/// visited and the residual filter decides whether it survives.
public struct FullScanPlan<T: Persistable>: Sendable {
    public var residualFilter: QueryIR.Expression?
    public var residualOrderBy: [SortKey]?
    public var limit: Int?
    public var offset: Int?

    public init(
        residualFilter: QueryIR.Expression? = nil,
        residualOrderBy: [SortKey]? = nil,
        limit: Int? = nil,
        offset: Int? = nil
    ) {
        self.residualFilter = residualFilter
        self.residualOrderBy = residualOrderBy
        self.limit = limit
        self.offset = offset
    }
}

// MARK: - KeyFieldBinding

/// Equality binding on a single index key field.
///
/// A binding pins `fieldName` to exactly one `value`. Bindings for the
/// leading prefix of an index turn into the fixed prefix of the scanned
/// range, leaving at most one trailing field open for `KeyRangeBound`.
public struct KeyFieldBinding: Sendable, Equatable {
    public var fieldName: String
    public var value: FieldValue

    public init(fieldName: String, value: FieldValue) {
        self.fieldName = fieldName
        self.value = value
    }
}

// MARK: - KeyRangeBound

/// Range constraint on a single key field.
///
/// `lower` / `upper` are independently optional so the bound can model
/// half-open ranges (`>= x`, `< y`) as well as closed / open intervals.
/// An absent bound means unbounded on that side.
public struct KeyRangeBound: Sendable, Equatable {
    public enum Endpoint: Sendable, Equatable {
        case inclusive(FieldValue)
        case exclusive(FieldValue)

        public var value: FieldValue {
            switch self {
            case .inclusive(let v), .exclusive(let v): return v
            }
        }

        public var isInclusive: Bool {
            if case .inclusive = self { return true }
            return false
        }
    }

    public var fieldName: String
    public var lower: Endpoint?
    public var upper: Endpoint?

    public init(fieldName: String, lower: Endpoint? = nil, upper: Endpoint? = nil) {
        self.fieldName = fieldName
        self.lower = lower
        self.upper = upper
    }
}

// MARK: - ScanDirection

/// Direction of an index scan along the key order.
public enum ScanDirection: Sendable, Equatable {
    case forward
    case backward
}

// MARK: - Explain

extension ExecutionPlan {
    /// Human-readable explanation of this plan, one line per section.
    ///
    /// Intended for `admin.explain()` output and debug logging. The format is
    /// not part of the public API contract — do not parse it programmatically.
    public func explain() -> String {
        switch self {
        case .indexAccess(let plan):
            return plan.explain()
        case .fullScan(let plan):
            return plan.explain()
        }
    }
}

extension IndexAccessPlan {
    public func explain() -> String {
        var lines: [String] = []
        lines.append("IndexAccess(\(indexName))")
        if !bindings.isEmpty {
            let boundStr = bindings
                .map { "\($0.fieldName)=\(describeFieldValue($0.value))" }
                .joined(separator: ", ")
            lines.append("  bindings: \(boundStr)")
        }
        if let range {
            lines.append("  range: \(describeRange(range))")
        }
        lines.append("  direction: \(direction == .forward ? "asc" : "desc")")
        if residualFilter != nil {
            lines.append("  residualFilter: present")
        }
        if let residualOrderBy, !residualOrderBy.isEmpty {
            lines.append("  residualOrderBy: \(residualOrderBy.count) key(s)")
        }
        if let limit {
            lines.append("  limit: \(limit)")
        }
        if let offset {
            lines.append("  offset: \(offset)")
        }
        return lines.joined(separator: "\n")
    }
}

extension FullScanPlan {
    public func explain() -> String {
        var lines: [String] = []
        lines.append("FullScan(\(T.persistableType))")
        if residualFilter != nil {
            lines.append("  residualFilter: present")
        }
        if let residualOrderBy, !residualOrderBy.isEmpty {
            lines.append("  residualOrderBy: \(residualOrderBy.count) key(s)")
        }
        if let limit {
            lines.append("  limit: \(limit)")
        }
        if let offset {
            lines.append("  offset: \(offset)")
        }
        return lines.joined(separator: "\n")
    }
}

private func describeFieldValue(_ value: FieldValue) -> String {
    switch value {
    case .null: return "null"
    case .string(let s): return "\"\(s)\""
    case .int64(let i): return String(i)
    case .double(let d): return String(d)
    case .bool(let b): return String(b)
    case .data(let d): return "<\(d.count) bytes>"
    default: return "\(value)"
    }
}

private func describeRange(_ range: KeyRangeBound) -> String {
    let lower = range.lower.map { endpoint -> String in
        let op = endpoint.isInclusive ? ">=" : ">"
        return "\(op) \(describeFieldValue(endpoint.value))"
    }
    let upper = range.upper.map { endpoint -> String in
        let op = endpoint.isInclusive ? "<=" : "<"
        return "\(op) \(describeFieldValue(endpoint.value))"
    }
    let parts = [lower, upper].compactMap { $0 }
    if parts.isEmpty {
        return "\(range.fieldName) unbounded"
    }
    return "\(range.fieldName) \(parts.joined(separator: " AND "))"
}
