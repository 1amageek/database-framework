import DatabaseEngine

/// Result of a non-allocating SPARQL row compatibility and footprint pass.
enum SPARQLBindingMergeFootprint: Sendable, Equatable {
    case incompatible
    case compatible(DatabaseIntermediateFootprint)
}
