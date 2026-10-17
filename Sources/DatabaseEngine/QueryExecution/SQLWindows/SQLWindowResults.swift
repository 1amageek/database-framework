import DatabaseKit

/// Immutable window cells indexed by the parent's original post-group row order.
struct SQLWindowResults: Sendable {
    let values: [DatabaseContext.ProjectedQueryValue]
    let payloadLease: DatabaseQueryPayloadLease
}
