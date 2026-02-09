// SHACLTargetResolver.swift
// GraphIndex - Resolve SHACL targets to focus nodes
//
// Converts SHACL target declarations into SPARQL queries
// and executes them against the graph index.
//
// Reference: W3C SHACL §2.1.3 (Targets)
// https://www.w3.org/TR/shacl/#targets

import Foundation
import FoundationDB
import Graph
import DatabaseEngine

/// Resolves SHACL targets to focus nodes using SPARQL queries
///
/// Each target type maps to a specific SPARQL pattern:
/// - `sh:targetNode` → direct set inclusion
/// - `sh:targetClass` → `{ ?node rdf:type <class> }`
/// - `sh:targetSubjectsOf` → `{ ?node <predicate> ?o }`
/// - `sh:targetObjectsOf` → `{ ?s <predicate> ?node }`
struct SHACLTargetResolver: Sendable {

    private let executor: SPARQLQueryExecutor

    init(executor: SPARQLQueryExecutor) {
        self.executor = executor
    }

    /// Resolve all targets to a set of focus node IRIs
    ///
    /// - Parameters:
    ///   - targets: The target declarations from a shape
    ///   - shapeIRI: The shape IRI (for implicit class targets)
    /// - Returns: Set of focus node IRI strings
    func resolve(
        _ targets: [SHACLTarget],
        shapeIRI: String?
    ) async throws -> Set<String> {
        var focusNodes = Set<String>()

        for target in targets {
            let nodes = try await resolveTarget(target, shapeIRI: shapeIRI)
            focusNodes.formUnion(nodes)
        }

        return focusNodes
    }

    // MARK: - Private

    private func resolveTarget(
        _ target: SHACLTarget,
        shapeIRI: String?
    ) async throws -> Set<String> {
        switch target {
        case .node(let iri):
            // Direct node — no query needed
            return [iri]

        case .class_(let classIRI):
            // { ?node rdf:type <classIRI> }
            return try await querySubjects(
                predicate: "rdf:type",
                object: classIRI
            )

        case .subjectsOf(let predicateIRI):
            // { ?node <predicateIRI> ?o }
            return try await querySubjects(predicate: predicateIRI)

        case .objectsOf(let predicateIRI):
            // { ?s <predicateIRI> ?node }
            return try await queryObjects(predicate: predicateIRI)

        case .implicitClass:
            // Shape IRI is treated as rdfs:Class
            guard let iri = shapeIRI else { return [] }
            return try await querySubjects(
                predicate: "rdf:type",
                object: iri
            )
        }
    }

    /// Query subjects matching { ?node <predicate> <object>? }
    private func querySubjects(
        predicate: String,
        object: String? = nil
    ) async throws -> Set<String> {
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?node"),
                predicate: .value(.string(predicate)),
                object: object.map { .value(.string($0)) } ?? .wildcard
            )
        ])

        let (bindings, _) = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0
        )

        var nodes = Set<String>()
        for binding in bindings {
            if let value = binding["?node"], let str = value.stringValue {
                nodes.insert(str)
            }
        }
        return nodes
    }

    /// Query objects matching { ?s <predicate> ?node }
    private func queryObjects(
        predicate: String
    ) async throws -> Set<String> {
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .wildcard,
                predicate: .value(.string(predicate)),
                object: .variable("?node")
            )
        ])

        let (bindings, _) = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0
        )

        var nodes = Set<String>()
        for binding in bindings {
            if let value = binding["?node"], let str = value.stringValue {
                nodes.insert(str)
            }
        }
        return nodes
    }
}
