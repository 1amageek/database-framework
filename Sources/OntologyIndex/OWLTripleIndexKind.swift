// OWLTripleIndexKind.swift
// OntologyIndex - IndexKind for materializing @OWLClass entity properties as SPO entries
//
// Converts @OWLDataProperty-annotated fields into RDF triple index entries.
// The triple entries are maintained automatically by OWLTripleMaintainerWrapper via the
// standard IndexMaintainer pipeline — no manual sync calls required.

import Foundation
import Core
import Graph

/// IndexKind that materializes @OWLClass entity properties as SPO triple entries.
///
/// When an entity with this index is saved, `OWLTripleMaintainerWrapper` automatically:
/// 1. Generates `rdf:type` triple from `ontologyClassIRI`
/// 2. Generates triples for all `@OWLDataProperty` fields via `ontologyPropertyDescriptors`
/// 3. Writes SPO/POS/OSP index entries for SPARQL queryability
///
/// **Usage**:
/// ```swift
/// @Persistable
/// @OWLClass("ex:Person")
/// struct Person {
///     @OWLDataProperty("rdfs:label") var name: String
///     @OWLDataProperty("ex:email") var email: String
///
///     #Index(OWLTripleIndexKind<Person>(graph: "default"))
/// }
///
/// // save() automatically materializes SPO entries
/// context.insert(person)
/// try await context.save()
/// ```
///
/// **Reference**: Weiss, C., Karras, P., & Bernstein, A. (2008).
/// "Hexastore: sextuple indexing for semantic web data management"
public struct OWLTripleIndexKind<Root: OWLClassEntity>: IndexKind, Sendable, Codable, Hashable {

    // MARK: - IndexKind Requirements

    public static var identifier: String { "owlTriple" }

    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    public var indexName: String {
        "\(Root.persistableType)_owlTriple"
    }

    public var fieldNames: [String] {
        Root.ontologyPropertyDescriptors.map(\.fieldName)
    }

    public static func validateTypes(_ types: [Any.Type]) throws {
        // OWL properties are serialized as string literals in RDF.
        // All Swift types that conform to CustomStringConvertible are accepted.
    }

    // MARK: - Properties

    /// Named graph IRI for the materialized triples.
    public let graph: String

    /// IRI prefix for entity IRI generation.
    ///
    /// Entity IRI format: `{prefix}:{lowercased_type_name}/{id}`
    public let prefix: String

    // MARK: - Initialization

    /// Create an OWL triple index kind.
    ///
    /// - Parameters:
    ///   - graph: Named graph IRI (default: "default")
    ///   - prefix: IRI prefix for entity IRI generation (default: "entity")
    public init(graph: String = "default", prefix: String = "entity") {
        self.graph = graph
        self.prefix = prefix
    }

    // MARK: - IndexDescriptor Factory

    /// Create an IndexDescriptor for this OWL triple index.
    ///
    /// Use this factory method because the `#Index` macro requires at least one KeyPath
    /// argument, which OWLTripleIndexKind does not use (it reads `ontologyPropertyDescriptors`
    /// instead). The `@OWLClass` macro in database-kit should eventually auto-generate this.
    ///
    /// - Parameters:
    ///   - graph: Named graph IRI (default: "default")
    ///   - prefix: IRI prefix for entity IRI generation (default: "entity")
    /// - Returns: An IndexDescriptor suitable for IndexMaintenanceService
    public static func indexDescriptor(
        graph: String = "default",
        prefix: String = "entity"
    ) -> IndexDescriptor {
        let kind = OWLTripleIndexKind<Root>(graph: graph, prefix: prefix)
        return IndexDescriptor(
            name: kind.indexName,
            keyPaths: [] as [PartialKeyPath<Root>],
            kind: kind
        )
    }
}
