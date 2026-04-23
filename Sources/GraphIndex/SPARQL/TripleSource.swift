// TripleSource.swift
// GraphIndex - Descriptor for one triple-producing index in a federated SPARQL query
//
// A `TripleSource` captures everything a `SPARQLQueryExecutor` needs to scan a
// single index in the context of a graph-scoped federated query. Two kinds of
// sources are modeled uniformly:
//
// 1. `GraphIndexKind` with a `graphField` — Statement-style free-form triples.
//    The graph is stored per-record, so queries must bind `defaultGraph` at
//    execution time.
// 2. `OWLTripleIndexKind` — materialized `@OWLClass` individuals with a fixed
//    graph baked into the index descriptor. The graph value is static and we
//    know ahead of time which predicates this source can produce.

import Foundation
import Core
import Graph
import StorageKit

/// One triple-producing index participating in a federated SPARQL query.
///
/// Constructed by `TripleSourcePlanner`; consumed by `FederatedSPARQLBuilder`
/// when it spins up a `SPARQLQueryExecutor` per source.
public struct TripleSource: Sendable {

    /// Origin entity name (e.g. "Statement", "Person"). Logging / debugging only.
    public let entityName: String

    /// Index descriptor name (e.g. "Person_owlTriple"). Logging / debugging only.
    public let indexName: String

    /// Pre-resolved `[I]/[indexName]` subspace, ready to hand to the executor.
    public let indexSubspace: Subspace

    /// Storage layout of the index (tripleStore / hexastore / adjacency).
    public let strategy: GraphIndexStrategy

    /// Subject field name. Empty string for OWL indexes which have no backing
    /// Persistable field but still expose canonical "subject" in the key tuple.
    public let fromField: String

    /// Predicate field name. Empty string for OWL indexes.
    public let edgeField: String

    /// Object field name. Empty string for OWL indexes.
    public let toField: String

    /// Graph field name.
    /// - nil for OWL indexes (graph is fixed, not a per-record column)
    /// - non-nil for `GraphIndexKind` indexes with a named graph column
    public let graphFieldName: String?

    /// Property field names stored in the CoveringValue (empty for OWL).
    public let storedFieldNames: [String]

    /// For OWL indexes: the fixed graph value baked into the descriptor.
    /// For `GraphIndexKind`: nil — the graph is driven per-record.
    public let fixedGraph: String?

    /// Predicates this source can produce.
    /// - nil = free-form: any predicate is possible (Statement-like sources)
    /// - non-nil = static set known at planning time (OWL sources), used for
    ///   predicate pruning by `TripleSourcePlanner`.
    public let producablePredicates: Set<String>?

    /// Entity IRI prefix for OWL subject pruning (e.g. "entity:person/"). nil
    /// for free-form sources. Matches the generation rule in
    /// `OWLTripleIndexMaintainer`.
    public let subjectIRIPrefix: String?

    // MARK: - Initialization

    public init(
        entityName: String,
        indexName: String,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        fromField: String,
        edgeField: String,
        toField: String,
        graphFieldName: String?,
        storedFieldNames: [String],
        fixedGraph: String?,
        producablePredicates: Set<String>?,
        subjectIRIPrefix: String?
    ) {
        self.entityName = entityName
        self.indexName = indexName
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.fromField = fromField
        self.edgeField = edgeField
        self.toField = toField
        self.graphFieldName = graphFieldName
        self.storedFieldNames = storedFieldNames
        self.fixedGraph = fixedGraph
        self.producablePredicates = producablePredicates
        self.subjectIRIPrefix = subjectIRIPrefix
    }
}
