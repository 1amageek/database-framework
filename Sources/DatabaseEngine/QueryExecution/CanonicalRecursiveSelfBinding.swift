import DatabaseKit

/// Seed-selected working-table metadata, without a completed plan or runtime owner.
struct CanonicalRecursiveSelfBinding: Sendable {
    let name: String
    let columns: [QueryColumn]
}
