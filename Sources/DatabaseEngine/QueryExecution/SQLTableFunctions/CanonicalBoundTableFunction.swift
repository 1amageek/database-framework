import DatabaseKit

/// An immutable FROM-function signature selected before argument evaluation.
struct CanonicalBoundTableFunction: Sendable {
    enum Kind: Sendable, Equatable { case array, json, patternMatches, patternSplit, series, scalar }
    let bound: CanonicalBoundExpression
    let columns: [QueryColumn]
    let kind: Kind
    let withOrdinality: Bool
}
