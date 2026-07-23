import DatabaseEngine

enum SPARQLPropertyPathBindingFootprint: Sendable, Equatable {
    case incompatible
    case compatible(DatabaseIntermediateFootprint)
}
