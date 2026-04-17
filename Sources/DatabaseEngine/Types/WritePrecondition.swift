// WritePrecondition.swift
// DatabaseEngine - Write precondition for explicit operation semantics
//
// Precondition evaluated at save() time. Violations raise
// FDBContextError.preconditionFailed instead of silently falling back
// (per CLAUDE.md "silent fallback 禁止").

import Foundation

/// Precondition that a write operation asserts about the stored row at commit time.
///
/// Each explicit operation on `FDBContext` (`create` / `upsert` / `replace` / `delete`)
/// defaults to the precondition that matches its intent. Callers may override the
/// default to opt into a different safety level.
///
/// | Precondition | Meaning |
/// |---|---|
/// | `.none` | No existence check. Blind write. |
/// | `.notExists` | The key must not exist in storage. Used by `create`. |
/// | `.exists` | The key must exist in storage. Used by `replace` and `delete`. |
/// | `.matchesStored(version:)` | The stored row's versionstamp must equal the given bytes. Optimistic concurrency control. |
/// | `.matchesStoredOrAbsent(version:)` | Either the row is absent, or its versionstamp matches. Idempotent OCC. |
public enum WritePrecondition: Sendable, Equatable {
    /// No check. The write proceeds unconditionally.
    case none

    /// The key must be absent from storage at commit time.
    case notExists

    /// The key must be present in storage at commit time.
    case exists

    /// The stored row's versionstamp must equal the given bytes.
    case matchesStored(version: [UInt8])

    /// Either the key is absent, or its versionstamp matches the given bytes.
    case matchesStoredOrAbsent(version: [UInt8])

    /// Whether evaluating this precondition requires reading the row from
    /// storage first. `.none` skips the read entirely.
    internal var requiresExistenceRead: Bool {
        switch self {
        case .none:
            return false
        case .notExists, .exists, .matchesStored, .matchesStoredOrAbsent:
            return true
        }
    }
}
