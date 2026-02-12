// FDBContainer+Ontology.swift
// GraphIndex - FDBContainer extension for Schema-ontology integration
//
// Decodes the type-erased Schema.Ontology back to OWLOntology
// and loads it into OntologyStore.

import Foundation
import Core
import Graph
import DatabaseEngine

extension FDBContainer {

    /// Load the schema-attached ontology into OntologyStore.
    ///
    /// Decodes `Schema.Ontology` (type-erased) back to `OWLOntology` and loads
    /// it into `OntologyStore`. Skips if the same IRI already exists.
    ///
    /// This method is NOT called from `FDBContainer.init` (DatabaseEngine cannot
    /// see this extension). Call it explicitly from GraphIndex consumers.
    ///
    /// **Usage**:
    /// ```swift
    /// let container = try await FDBContainer(for: schema)
    /// try await container.loadSchemaOntology()
    /// ```
    public func loadSchemaOntology() async throws {
        guard let schemaOntology = schema.ontology else { return }

        // Decode type-erased Schema.Ontology back to OWLOntology
        let owlOntology = try OWLOntology(schemaOntology: schemaOntology)

        let context = newContext()
        let api = context.ontology

        // Skip if already loaded
        let exists = try await api.exists(iri: owlOntology.iri)
        if exists { return }

        try await api.load(owlOntology)
    }
}
