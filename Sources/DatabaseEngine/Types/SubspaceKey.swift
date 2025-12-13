// SubspaceKey.swift
// DatabaseEngine - Centralized subspace key definitions
//
// All subspace keys are defined here to ensure consistency across the codebase.
// Values are single characters for storage efficiency; names provide semantics.

import Foundation

/// Subspace key constants for FDB data layout
///
/// Data layout:
/// ```
/// [fdb]/R/[PersistableType]/[id]        → ItemEnvelope (inline or external ref)
/// [fdb]/I/[indexName]/[values...]/[id]  → Index entry
/// [fdb]/S/...                           → Store metadata
/// [fdb]/T/[indexName]                   → Index state
/// ```
public enum SubspaceKey {
    /// Items subspace key - stores Persistable instances
    ///
    /// Layout: `[subspace]/R/[typeName]/[id]`
    public static let items = "R"

    /// Indexes subspace key - stores index entries
    ///
    /// Layout: `[subspace]/I/[indexName]/[values...]/[id]`
    public static let indexes = "I"

    /// Store info subspace key - stores store metadata
    ///
    /// Layout: `[subspace]/S/[key]`
    public static let storeInfo = "S"

    /// Former indexes subspace key - tracks removed indexes
    ///
    /// Layout: `[subspace]/S/F/[indexName]`
    public static let formerIndexes = "F"

    /// Index state subspace key - stores index states
    ///
    /// Layout: `[subspace]/T/[indexName]`
    public static let state = "T"

    /// Metadata subspace key - stores metadata including violations
    ///
    /// Layout: `[subspace]/M/[key]`
    public static let metadata = "M"

    /// Blobs subspace key - stores large value chunks
    ///
    /// Large values (>90KB) are split into chunks and stored here,
    /// keeping the items subspace clean for range scans.
    ///
    /// Layout: `[subspace]/B/[Tuple([itemKeyBytes])]/[chunkIndex]`
    public static let blobs = "B"
}
