// DynamicSortDescriptor.swift
// fdb-cli - Sort descriptor for dynamic schema queries
//
// Reuses QueryAST's SortDirection and NullOrdering for consistency
// across the database-framework ecosystem.

import QueryAST

/// Sort descriptor for dynamic schema queries
///
/// Unlike `SortKey` which uses typed `Expression`, this struct uses
/// field names as strings to support dynamic schemas with `[String: Any]`.
///
/// **Usage**:
/// ```swift
/// // Ascending by name (default)
/// let sort = DynamicSortDescriptor(field: "name")
///
/// // Descending by age with nulls last
/// let sort = DynamicSortDescriptor(
///     field: "age",
///     direction: .descending,
///     nulls: .last
/// )
/// ```
public struct DynamicSortDescriptor: Sendable, Equatable, Hashable {
    /// Field name to sort by
    public let field: String

    /// Sort direction (reused from QueryAST)
    public let direction: SortDirection

    /// NULL ordering (reused from QueryAST)
    public let nulls: NullOrdering?

    public init(
        field: String,
        direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil
    ) {
        self.field = field
        self.direction = direction
        self.nulls = nulls
    }
}

// MARK: - Convenience Extensions

extension DynamicSortDescriptor {
    /// Create an ascending sort descriptor
    public static func ascending(_ field: String, nulls: NullOrdering? = nil) -> DynamicSortDescriptor {
        DynamicSortDescriptor(field: field, direction: .ascending, nulls: nulls)
    }

    /// Create a descending sort descriptor
    public static func descending(_ field: String, nulls: NullOrdering? = nil) -> DynamicSortDescriptor {
        DynamicSortDescriptor(field: field, direction: .descending, nulls: nulls)
    }
}
