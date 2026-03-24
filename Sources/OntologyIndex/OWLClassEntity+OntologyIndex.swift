// OWLClassEntity+OntologyIndex.swift
// OntologyIndex - Auto-register OWL triple index for @OWLClass entities
//
// Provides a static property that Schema can use to include the OWL triple
// index descriptor alongside the entity's own descriptors.

import Core
import Graph

extension OWLClassEntity {
    /// OWL triple index descriptor for this entity type.
    ///
    /// Generates an IndexDescriptor that enables OWLTripleMaintainer to
    /// automatically materialize entity properties as SPO triple entries.
    ///
    /// **Usage in Schema**:
    /// ```swift
    /// let schema = Schema(
    ///     [Person.self, Organization.self],
    ///     indexDescriptors: [
    ///         Person.owlTripleIndexDescriptor(),
    ///         Organization.owlTripleIndexDescriptor(),
    ///     ]
    /// )
    /// ```
    public static func owlTripleIndexDescriptor(
        graph: String = "default",
        prefix: String = "entity"
    ) -> IndexDescriptor {
        OWLTripleIndexKind<Self>.indexDescriptor(graph: graph, prefix: prefix)
    }
}
