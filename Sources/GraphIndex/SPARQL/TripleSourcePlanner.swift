// TripleSourcePlanner.swift
// GraphIndex - Enumerate triple-producing indexes for a federated SPARQL query
//
// Given a named graph and an ExecutionPattern, returns the set of index
// subspaces that could contribute bindings. Applies static pruning based on
// predicates and subject IRI prefixes found in the BGP, so free-form
// `GraphIndexKind` sources (Statement-like) always pass through while
// `OWLTripleIndexKind` sources are eliminated when the query cannot be
// satisfied by their predicate set or subject namespace.
//
// The planner is purely analytic — no storage access occurs here. Subspace
// resolution is deferred to the caller (via IndexQueryContext).

import Foundation
import Core
import DatabaseEngine
import Graph
import StorageKit

// MARK: - Internal Kind Access

/// Structural view over `OWLTripleIndexKind<Root>` that is independent of the
/// generic `Root` parameter.
///
/// This lets the planner inspect OWL kinds held as `any IndexKind` without
/// knowing the concrete `Root` type. `OWLTripleIndexKind` conforms via the
/// extension below.
internal protocol _OWLTripleIndexKindProtocol: Sendable {
    var graph: String { get }
    var prefix: String { get }
}

extension OWLTripleIndexKind: _OWLTripleIndexKindProtocol {}

/// Structural view over `GraphIndexKind<Root>` that is independent of the
/// generic `Root` parameter.
internal protocol _GraphIndexKindProtocol: Sendable {
    var fromField: String { get }
    var edgeField: String { get }
    var toField: String { get }
    var graphField: String? { get }
    var strategy: GraphIndexStrategy { get }
}

extension GraphIndexKind: _GraphIndexKindProtocol {}

// MARK: - Planner

/// Plans the set of triple-producing indexes that participate in a federated
/// SPARQL query against a named graph.
public struct TripleSourcePlanner: Sendable {

    /// Enumerate `TripleSource`s bound to `graph` that can answer `pattern`.
    ///
    /// Candidate sources are discovered by walking the schema's entities and
    /// their index descriptors. Two descriptor kinds contribute:
    ///
    /// - `OWLTripleIndexKind` — OWL materialized individuals. The descriptor's
    ///   `graph` must equal `graph`. The producable predicate set is derived
    ///   from the entity's `dataPropertyIRIs` plus `rdf:type`.
    /// - `GraphIndexKind` with `graphField != nil` and a non-adjacency
    ///   strategy — Statement-style free-form triples. The graph column is
    ///   per-record, so the source is always a candidate and has no static
    ///   predicate set (free-form).
    ///
    /// After enumeration, static pruning removes sources whose predicate set
    /// is disjoint with the BGP's fixed predicates, or whose IRI prefix
    /// doesn't overlap with any fixed subject IRI.
    public static func plan(
        pattern: ExecutionPattern,
        graph: String,
        queryContext: IndexQueryContext
    ) async throws -> [TripleSource] {
        let candidates = try await enumerateCandidates(graph: graph, queryContext: queryContext)
        return prune(sources: candidates, pattern: pattern)
    }

    // MARK: - Step 1: Enumeration

    private static func enumerateCandidates(
        graph: String,
        queryContext: IndexQueryContext
    ) async throws -> [TripleSource] {
        var sources: [TripleSource] = []

        for entity in queryContext.schema.entities {
            for descriptor in entity.indexDescriptors {
                if let owl = descriptor.kind as? any _OWLTripleIndexKindProtocol {
                    guard owl.graph == graph else { continue }
                    let typeSubspace = try await queryContext.indexSubspace(forEntityName: entity.name)
                    let indexSubspace = typeSubspace.subspace(descriptor.name)

                    var predicates = Set(entity.dataPropertyIRIs ?? [])
                    predicates.insert("rdf:type")

                    let subjectIRIPrefix = "\(owl.prefix):\(entity.name.lowercased())/"

                    sources.append(TripleSource(
                        entityName: entity.name,
                        indexName: descriptor.name,
                        indexSubspace: indexSubspace,
                        strategy: .tripleStore,
                        fromField: "",
                        edgeField: "",
                        toField: "",
                        graphFieldName: nil,
                        storedFieldNames: descriptor.storedFieldNames,
                        fixedGraph: owl.graph,
                        producablePredicates: predicates,
                        subjectIRIPrefix: subjectIRIPrefix
                    ))
                    continue
                }

                if let gi = descriptor.kind as? any _GraphIndexKindProtocol,
                   gi.strategy != .adjacency,
                   let graphFieldName = gi.graphField {
                    let typeSubspace = try await queryContext.indexSubspace(forEntityName: entity.name)
                    let indexSubspace = typeSubspace.subspace(descriptor.name)

                    sources.append(TripleSource(
                        entityName: entity.name,
                        indexName: descriptor.name,
                        indexSubspace: indexSubspace,
                        strategy: gi.strategy,
                        fromField: gi.fromField,
                        edgeField: gi.edgeField,
                        toField: gi.toField,
                        graphFieldName: graphFieldName,
                        storedFieldNames: descriptor.storedFieldNames,
                        fixedGraph: nil,
                        producablePredicates: nil,
                        subjectIRIPrefix: nil
                    ))
                }
            }
        }

        return sources
    }

    // MARK: - Step 2: Static Pruning

    private static func prune(
        sources: [TripleSource],
        pattern: ExecutionPattern
    ) -> [TripleSource] {
        let predicates = extractBoundPredicates(from: pattern)
        let subjectIRIs = extractBoundSubjectIRIs(from: pattern)

        return sources.filter { source in
            if let produces = source.producablePredicates,
               !predicates.isEmpty,
               predicates.isDisjoint(with: produces) {
                return false
            }

            if let sourcePrefix = source.subjectIRIPrefix,
               !subjectIRIs.isEmpty,
               !subjectIRIs.contains(where: { $0.hasPrefix(sourcePrefix) }) {
                return false
            }

            return true
        }
    }

    // MARK: - Pattern Analysis

    /// Collect fixed predicate IRIs appearing in the BGP of `pattern`.
    ///
    /// Recurses through all pattern nodes. For MINUS, only the left (positive)
    /// side is walked — predicates that appear only on the right would narrow
    /// the positive set incorrectly.
    private static func extractBoundPredicates(from pattern: ExecutionPattern) -> Set<String> {
        var result = Set<String>()
        collectPredicates(from: pattern, into: &result)
        return result
    }

    private static func collectPredicates(from pattern: ExecutionPattern, into result: inout Set<String>) {
        switch pattern {
        case .basic(let triples):
            for triple in triples {
                if case .value(.string(let iri)) = triple.predicate {
                    result.insert(iri)
                }
            }
        case .join(let left, let right),
             .optional(let left, let right),
             .union(let left, let right),
             .lateral(let left, let right):
            collectPredicates(from: left, into: &result)
            collectPredicates(from: right, into: &result)
        case .filter(let inner, _):
            collectPredicates(from: inner, into: &result)
        case .groupBy(let inner, _, _, _):
            collectPredicates(from: inner, into: &result)
        case .minus(let left, _):
            collectPredicates(from: left, into: &result)
        case .propertyPath:
            break
        }
    }

    /// Collect fixed subject IRIs appearing in the BGP of `pattern`.
    private static func extractBoundSubjectIRIs(from pattern: ExecutionPattern) -> Set<String> {
        var result = Set<String>()
        collectSubjectIRIs(from: pattern, into: &result)
        return result
    }

    private static func collectSubjectIRIs(from pattern: ExecutionPattern, into result: inout Set<String>) {
        switch pattern {
        case .basic(let triples):
            for triple in triples {
                if case .value(.string(let iri)) = triple.subject {
                    result.insert(iri)
                }
            }
        case .join(let left, let right),
             .optional(let left, let right),
             .union(let left, let right),
             .lateral(let left, let right):
            collectSubjectIRIs(from: left, into: &result)
            collectSubjectIRIs(from: right, into: &result)
        case .filter(let inner, _):
            collectSubjectIRIs(from: inner, into: &result)
        case .groupBy(let inner, _, _, _):
            collectSubjectIRIs(from: inner, into: &result)
        case .minus(let left, _):
            collectSubjectIRIs(from: left, into: &result)
        case .propertyPath(let subject, _, _):
            if case .value(.string(let iri)) = subject {
                result.insert(iri)
            }
        }
    }
}
