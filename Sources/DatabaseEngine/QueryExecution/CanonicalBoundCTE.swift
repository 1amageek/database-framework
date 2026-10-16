import DatabaseKit

/// Immutable lexical definition metadata; runtime cache identity adds its invocation.
struct CanonicalBoundCTE: Sendable {
    let identity: Int
    let definition: NamedSubquery
    let plan: CanonicalBoundSelect
    let referenceCount: Int
    let isVolatile: Bool
}
