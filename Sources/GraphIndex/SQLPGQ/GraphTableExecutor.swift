/// GraphTableExecutor.swift
/// SQL/PGQ GRAPH_TABLE query executor

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB
import QueryIR

// MARK: - GraphTableRow

/// Result row from GRAPH_TABLE query
public struct GraphTableRow: Sendable {
    /// Source node ID
    public let source: String

    /// Target node ID
    public let target: String

    /// Edge label
    public let edgeLabel: String

    /// Properties from edge
    public let properties: [String: any Sendable]

    public init(
        source: String,
        target: String,
        edgeLabel: String,
        properties: [String: any Sendable]
    ) {
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
        self.properties = properties
    }
}

// MARK: - GraphTableExecutor

/// Executor for SQL/PGQ GRAPH_TABLE queries
///
/// Converts SQL/PGQ match patterns to GraphPropertyScanner calls with property filtering.
public struct GraphTableExecutor<T: Persistable>: Sendable {
    private let container: FDBContainer
    private let schema: Schema
    private let graphTableSource: GraphTableSource

    public init(
        container: FDBContainer,
        schema: Schema,
        graphTableSource: GraphTableSource
    ) {
        self.container = container
        self.schema = schema
        self.graphTableSource = graphTableSource
    }

    /// Execute GRAPH_TABLE query and return matching rows
    public func execute() async throws -> [GraphTableRow] {
        // Extract edge patterns from match pattern
        let edgePatterns = try extractEdgePatterns(from: graphTableSource.matchPattern)

        guard let firstEdge = edgePatterns.first else {
            throw GraphTableError.invalidGraphPattern("No edge patterns found in MATCH clause")
        }

        // Convert edge pattern properties to PropertyFilter
        let propertyFilters = try convertToPropertyFilters(firstEdge.properties)

        // Find graph index
        let indexDescriptor = try findGraphIndex(for: T.self)
        let kind = indexDescriptor.kind as! GraphIndexKind<T>

        // Resolve directory
        let subspace = try await container.resolveDirectory(for: T.self)
        let indexSubspace = subspace.subspace("I").subspace(indexDescriptor.name)

        // Extract from/edge/to patterns
        let (fromPattern, edgeLabel, toPattern) = try extractPatterns(from: firstEdge)

        // Create scanner
        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: kind.strategy,
            storedFieldNames: indexDescriptor.storedFieldNames
        )

        // Execute scan
        return try await container.database.withTransaction { transaction in
            let stream = scanner.scanEdges(
                from: fromPattern,
                edge: edgeLabel,
                to: toPattern,
                graph: nil,
                propertyFilters: propertyFilters.isEmpty ? nil : propertyFilters,
                transaction: transaction
            )

            var results: [GraphTableRow] = []
            for try await edge in stream {
                let row = GraphTableRow(
                    source: edge.source,
                    target: edge.target,
                    edgeLabel: edge.edgeLabel,
                    properties: edge.properties
                )
                results.append(row)

                // Apply LIMIT if specified in query
                // TODO: Extract limit from SelectQuery
            }

            return results
        }
    }

    // MARK: - Pattern Extraction

    /// Extract edge patterns from match pattern
    private func extractEdgePatterns(from matchPattern: MatchPattern) throws -> [EdgePattern] {
        var edgePatterns: [EdgePattern] = []

        for path in matchPattern.paths {
            for element in path.elements {
                if case .edge(let edgePattern) = element {
                    edgePatterns.append(edgePattern)
                }
            }
        }

        return edgePatterns
    }

    /// Extract from/edge/to patterns from edge pattern
    ///
    /// Returns: (fromPattern, edgeLabel, toPattern)
    private func extractPatterns(
        from edgePattern: EdgePattern
    ) throws -> (fromPattern: String?, edgeLabel: String?, toPattern: String?) {
        // Edge label (first label if multiple)
        let edgeLabel = edgePattern.labels?.first

        // TODO: Extract source/target node variables from path context
        // For now, return nil (scan all edges)
        return (nil, edgeLabel, nil)
    }

    // MARK: - Property Filter Conversion

    /// Convert EdgePattern properties to PropertyFilter array
    ///
    /// Simple equality and comparison expressions are supported.
    /// Complex expressions (subqueries, functions) throw an error.
    private func convertToPropertyFilters(
        _ properties: [PropertyBinding]?
    ) throws -> [PropertyFilter] {
        guard let properties = properties else { return [] }

        var filters: [PropertyFilter] = []

        for binding in properties {
            let fieldName = binding.key
            let expression = binding.value
            // Handle simple expressions only
            switch expression {
            case .literal(let literal):
                // Property equality: {since: 2020}
                let fieldValue = try convertLiteralToFieldValue(literal)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .equal,
                    value: fieldValue
                ))

            case .equal(let lhs, let rhs):
                // Property comparison: {since = 2020}
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .equal,
                    value: fieldValue
                ))

            case .notEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .notEqual,
                    value: fieldValue
                ))

            case .lessThan(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .lessThan,
                    value: fieldValue
                ))

            case .lessThanOrEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .lessThanOrEqual,
                    value: fieldValue
                ))

            case .greaterThan(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .greaterThan,
                    value: fieldValue
                ))

            case .greaterThanOrEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try extractLiteralValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .greaterThanOrEqual,
                    value: fieldValue
                ))

            case .isNull(let expr):
                try validateFieldReference(expr, fieldName: fieldName)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .isNil,
                    value: .null
                ))

            case .isNotNull(let expr):
                try validateFieldReference(expr, fieldName: fieldName)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .isNotNil,
                    value: .null
                ))

            default:
                // Complex expressions cannot be pushed to index scan
                throw GraphTableError.complexPropertyExpression(
                    "Property filter '\(fieldName)' has complex expression. " +
                    "Only simple comparisons (=, !=, <, <=, >, >=, IS NULL, IS NOT NULL) " +
                    "can be pushed to index scan. Use WHERE clause for complex filters."
                )
            }
        }

        return filters
    }

    /// Validate that expression is a field reference matching the expected field name
    private func validateFieldReference(
        _ expression: QueryIR.Expression,
        fieldName: String
    ) throws {
        switch expression {
        case .column(let columnRef):
            guard columnRef.column == fieldName else {
                throw GraphTableError.invalidColumnExpression(
                    "Expected field reference '\(fieldName)', got '\(columnRef.column)'"
                )
            }
        case .variable:
            // SPARQL variable reference (allowed)
            break
        default:
            throw GraphTableError.invalidColumnExpression(
                "Expected field reference, got complex expression"
            )
        }
    }

    /// Extract literal value from expression
    private func extractLiteralValue(from expression: QueryIR.Expression) throws -> FieldValue {
        guard case .literal(let literal) = expression else {
            throw GraphTableError.complexPropertyExpression(
                "Property comparison value must be a literal, not an expression"
            )
        }
        return try convertLiteralToFieldValue(literal)
    }

    /// Convert QueryIR.Literal to DatabaseEngine.FieldValue
    private func convertLiteralToFieldValue(_ literal: Literal) throws -> FieldValue {
        switch literal {
        case .null:
            return .null
        case .bool(let value):
            return .bool(value)
        case .int(let value):
            return .int64(value)
        case .double(let value):
            return .double(value)
        case .string(let value):
            return .string(value)
        case .date(let value):
            return .double(value.timeIntervalSince1970)
        case .timestamp(let value):
            return .double(value.timeIntervalSince1970)
        case .binary(let value):
            return .data(value)
        case .array:
            // Arrays cannot be converted to simple FieldValue
            throw GraphTableError.typeMismatch("Array literals not supported in property filters")
        case .iri(let value):
            // SPARQL IRI as string
            return .string(value)
        case .blankNode(let value):
            // SPARQL blank node as string
            return .string(value)
        case .typedLiteral(let value, _):
            // Typed literal - use value as string (ignore datatype)
            return .string(value)
        case .langLiteral(let value, _):
            // Language-tagged literal - use value as string (ignore language tag)
            return .string(value)
        case .dirLangLiteral(let value, _, _):
            return .string(value)
        }
    }

    // MARK: - Index Resolution

    /// Find graph index for the given type
    private func findGraphIndex(for type: T.Type) throws -> IndexDescriptor {
        let descriptors = T.indexDescriptors

        // Find first GraphIndexKind descriptor
        for descriptor in descriptors {
            if descriptor.kind is GraphIndexKind<T> {
                return descriptor
            }
        }

        throw GraphTableError.indexNotFound(
            "No GraphIndexKind found for type \(T.self)"
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Execute SQL/PGQ GRAPH_TABLE query
    ///
    /// Example:
    /// ```swift
    /// let source = GraphTableSource(
    ///     graphName: "SocialGraph",
    ///     matchPattern: MatchPattern(paths: [
    ///         PathPattern(elements: [
    ///             .node(NodePattern(variable: "a")),
    ///             .edge(EdgePattern(
    ///                 labels: ["KNOWS"],
    ///                 properties: [("since", .literal(.int(2020)))],
    ///                 direction: .outgoing
    ///             )),
    ///             .node(NodePattern(variable: "b"))
    ///         ])
    ///     ])
    /// )
    ///
    /// let rows = try await context.graphTable(SocialEdge.self, source: source)
    /// ```
    public func graphTable<T: Persistable>(
        _ type: T.Type,
        source: GraphTableSource
    ) async throws -> [GraphTableRow] {
        let executor = GraphTableExecutor<T>(
            container: container,
            schema: container.schema,
            graphTableSource: source
        )
        return try await executor.execute()
    }
}
