/// One physical predicate scan contributing to an ontology-entailed property.
/// Direction is relative to the requested logical property path.
struct OntologyEntailedPropertyScan: Sendable, Hashable {
    let predicateIRI: String
    let isInverse: Bool
}
