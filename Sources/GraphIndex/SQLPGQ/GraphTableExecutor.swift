/// GraphTableExecutor.swift
/// SQL/PGQ GRAPH_TABLE query executor

import Foundation
import Core
import Graph
import DatabaseEngine
import StorageKit
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

    /// Flattened row fields used by canonical graph-table projection.
    public let fields: [String: FieldValue]

    public init(
        source: String,
        target: String,
        edgeLabel: String,
        properties: [String: any Sendable],
        fields: [String: FieldValue]? = nil
    ) {
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
        self.properties = properties
        if let fields {
            self.fields = fields
        } else {
            var fields: [String: FieldValue] = [
                "source": .string(source),
                "target": .string(target),
                "edgeLabel": .string(edgeLabel)
            ]
            for (key, value) in properties {
                fields[key] = FieldValue(value) ?? .string(String(describing: value))
            }
            self.fields = fields
        }
    }
}

// MARK: - GraphTableExecutor

/// Executor for SQL/PGQ GRAPH_TABLE queries
///
/// Converts SQL/PGQ match patterns to GraphPropertyScanner calls with property filtering.
public struct GraphTableExecutor<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let graphTableSource: GraphTableSource
    private let transactionConfiguration: TransactionConfiguration

    public init(
        queryContext: IndexQueryContext,
        graphTableSource: GraphTableSource,
        transactionConfiguration: TransactionConfiguration = .default
    ) {
        self.queryContext = queryContext
        self.graphTableSource = graphTableSource
        self.transactionConfiguration = transactionConfiguration
    }

    public init(
        container: DBContainer,
        graphTableSource: GraphTableSource,
        transactionConfiguration: TransactionConfiguration = .default
    ) {
        self.init(
            queryContext: IndexQueryContext(context: FDBContext(container: container)),
            graphTableSource: graphTableSource,
            transactionConfiguration: transactionConfiguration
        )
    }

    /// Execute GRAPH_TABLE query and return matching rows
    public func execute() async throws -> [GraphTableRow] {
        let steps = try extractSteps(from: graphTableSource.matchPattern)
        guard !steps.isEmpty else {
            throw GraphTableError.invalidGraphPattern("No edge patterns found in MATCH clause")
        }

        let indexDescriptor = try findGraphIndex()
        let kind = indexDescriptor.kind as! GraphIndexKind<T>
        let indexSubspace = try await queryContext.indexSubspace(for: T.self).subspace(indexDescriptor.name)
        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: kind.strategy,
            storedFieldNames: indexDescriptor.storedFieldNames
        )

        return try await queryContext.withTransaction(configuration: transactionConfiguration) { transaction in
            var states: [MatchState] = [MatchState()]

            for step in steps {
                states = try await extend(
                    states: states,
                    with: step,
                    scanner: scanner,
                    strategy: kind.strategy,
                    transaction: transaction
                )
                if states.isEmpty {
                    return []
                }
            }

            let rows = try states.map(makeRow)
            guard let filter = graphTableSource.matchPattern.where else {
                return rows
            }
            return try rows.filter { try evaluateBoolean(filter, fields: $0.fields) }
        }
    }

    // MARK: - Pattern Evaluation

    private struct Step: Sendable {
        let left: NodePattern
        let edge: EdgePattern
        let right: NodePattern
    }

    private struct MatchedStep: Sendable {
        let step: Step
        let edge: GraphEdgeWithProperties
        let leftID: String
        let rightID: String
    }

    private struct MatchState: Sendable {
        let bindings: [String: FieldValue]
        let matchedSteps: [MatchedStep]

        init(
            bindings: [String: FieldValue] = [:],
            matchedSteps: [MatchedStep] = []
        ) {
            self.bindings = bindings
            self.matchedSteps = matchedSteps
        }
    }

    private enum NodeIdentityResolution: Sendable, Equatable {
        case any
        case exact(String)
        case impossible
    }

    private enum TraversalOrientation: Sendable {
        case outgoing
        case incoming
    }

    private func extractSteps(from matchPattern: MatchPattern) throws -> [Step] {
        var steps: [Step] = []
        for path in matchPattern.paths {
            steps.append(contentsOf: try extractSteps(from: path))
        }
        return steps
    }

    private func extractSteps(from path: PathPattern) throws -> [Step] {
        switch path.mode {
        case nil, .walk, .trail, .acyclic, .simple:
            break
        case .anyShortest, .allShortest, .shortestK:
            throw GraphTableError.invalidGraphPattern(
                "Shortest-path GRAPH_TABLE execution is not supported on the canonical read path"
            )
        }

        guard !path.elements.isEmpty else { return [] }
        var steps: [Step] = []
        var index = 0
        while index + 2 < path.elements.count {
            guard case .node(let left) = path.elements[index],
                  case .edge(let edge) = path.elements[index + 1],
                  case .node(let right) = path.elements[index + 2] else {
                throw GraphTableError.invalidGraphPattern(
                    "GRAPH_TABLE currently supports linear node-edge-node paths only"
                )
            }
            steps.append(Step(left: left, edge: edge, right: right))
            index += 2
        }

        if index != path.elements.count - 1 {
            throw GraphTableError.invalidGraphPattern(
                "GRAPH_TABLE currently supports linear node-edge-node paths only"
            )
        }

        return steps
    }

    private func extend(
        states: [MatchState],
        with step: Step,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any Transaction
    ) async throws -> [MatchState] {
        var nextStates: [MatchState] = []
        for state in states {
            let matches = try await match(
                step: step,
                state: state,
                scanner: scanner,
                strategy: strategy,
                transaction: transaction
            )
            nextStates.append(contentsOf: matches)
        }
        return nextStates
    }

    private func match(
        step: Step,
        state: MatchState,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any Transaction
    ) async throws -> [MatchState] {
        let leftResolution = try resolveIdentity(for: step.left, bindings: state.bindings)
        let rightResolution = try resolveIdentity(for: step.right, bindings: state.bindings)
        if leftResolution == .impossible || rightResolution == .impossible {
            return []
        }

        let propertyFilters = try convertToPropertyFilters(step.edge.properties)
        let labels: [String?] = step.edge.labels?.isEmpty == false
            ? step.edge.labels!.map(Optional.some)
            : [nil]
        var matches: [MatchState] = []

        for orientation in traversals(for: step.edge.direction) {
            for label in labels {
                let stream = scanner.scanEdges(
                    from: scanFrom(
                        left: leftResolution,
                        right: rightResolution,
                        orientation: orientation,
                        strategy: strategy
                    ),
                    edge: label,
                    to: scanTo(
                        left: leftResolution,
                        right: rightResolution,
                        orientation: orientation,
                        strategy: strategy
                    ),
                    graph: nil,
                    propertyFilters: propertyFilters.isEmpty ? nil : propertyFilters,
                    transaction: transaction
                )

                for try await edge in stream {
                    let (leftID, rightID) = endpointIDs(for: edge, orientation: orientation)
                    guard endpointMatches(leftID, resolution: leftResolution),
                          endpointMatches(rightID, resolution: rightResolution) else {
                        continue
                    }
                    guard let bindings = bind(step: step, leftID: leftID, rightID: rightID, edge: edge, onto: state.bindings) else {
                        continue
                    }
                    matches.append(
                        MatchState(
                            bindings: bindings,
                            matchedSteps: state.matchedSteps + [MatchedStep(step: step, edge: edge, leftID: leftID, rightID: rightID)]
                        )
                    )
                }
            }
        }

        return deduplicated(states: matches)
    }

    private func traversals(for direction: EdgeDirection) -> [TraversalOrientation] {
        switch direction {
        case .outgoing:
            return [.outgoing]
        case .incoming:
            return [.incoming]
        case .undirected, .any:
            return [.outgoing, .incoming]
        }
    }

    private func scanFrom(
        left: NodeIdentityResolution,
        right: NodeIdentityResolution,
        orientation: TraversalOrientation,
        strategy: GraphIndexStrategy
    ) -> String? {
        guard supportsEndpointPrefixScan(strategy) else {
            return nil
        }
        switch orientation {
        case .outgoing:
            if case .exact(let value) = left { return value }
            return nil
        case .incoming:
            if case .exact(let value) = right { return value }
            return nil
        }
    }

    private func scanTo(
        left: NodeIdentityResolution,
        right: NodeIdentityResolution,
        orientation: TraversalOrientation,
        strategy: GraphIndexStrategy
    ) -> String? {
        guard supportsEndpointPrefixScan(strategy) else {
            return nil
        }
        switch orientation {
        case .outgoing:
            if case .exact(let value) = right { return value }
            return nil
        case .incoming:
            if case .exact(let value) = left { return value }
            return nil
        }
    }

    private func supportsEndpointPrefixScan(_ strategy: GraphIndexStrategy) -> Bool {
        switch strategy {
        case .adjacency, .hexastore:
            return true
        case .tripleStore, .namedGraphStore:
            return false
        }
    }

    private func endpointMatches(
        _ identifier: String,
        resolution: NodeIdentityResolution
    ) -> Bool {
        switch resolution {
        case .any:
            return true
        case .exact(let expected):
            return identifier == expected
        case .impossible:
            return false
        }
    }

    private func endpointIDs(
        for edge: GraphEdgeWithProperties,
        orientation: TraversalOrientation
    ) -> (left: String, right: String) {
        switch orientation {
        case .outgoing:
            return (edge.source, edge.target)
        case .incoming:
            return (edge.target, edge.source)
        }
    }

    private func resolveIdentity(
        for node: NodePattern,
        bindings: [String: FieldValue]
    ) throws -> NodeIdentityResolution {
        var exactValues: [String] = []

        if let variable = node.variable,
           let bound = bindings["\(variable).id"] ?? bindings[variable],
           let value = bound.stringValue {
            exactValues.append(value)
        }

        for property in node.properties ?? [] {
            guard property.key == "id" else {
                throw GraphTableError.invalidGraphPattern(
                    "Node property '\(property.key)' is not supported; only 'id' constraints can be evaluated"
                )
            }
            exactValues.append(try extractNodeID(from: property.value))
        }

        let unique = Set(exactValues)
        if unique.count > 1 {
            return .impossible
        }
        if let value = unique.first {
            return .exact(value)
        }
        return .any
    }

    private func extractNodeID(from expression: QueryIR.Expression) throws -> String {
        switch expression {
        case .literal(let literal):
            let value = try convertLiteralToFieldValue(literal)
            guard let stringValue = value.stringValue else {
                throw GraphTableError.typeMismatch("Node id constraint must resolve to a string value")
            }
            return stringValue
        case .equal(let lhs, let rhs):
            try validateFieldReference(lhs, fieldName: "id")
            let value = try extractLiteralValue(from: rhs)
            guard let stringValue = value.stringValue else {
                throw GraphTableError.typeMismatch("Node id constraint must resolve to a string value")
            }
            return stringValue
        default:
            throw GraphTableError.complexPropertyExpression(
                "Node id constraint must be a literal or equality comparison"
            )
        }
    }

    private func bind(
        step: Step,
        leftID: String,
        rightID: String,
        edge: GraphEdgeWithProperties,
        onto existing: [String: FieldValue]
    ) -> [String: FieldValue]? {
        var bindings = existing

        if let leftVariable = step.left.variable {
            guard bind("\(leftVariable).id", value: .string(leftID), into: &bindings) else {
                return nil
            }
        }
        if let rightVariable = step.right.variable {
            guard bind("\(rightVariable).id", value: .string(rightID), into: &bindings) else {
                return nil
            }
        }
        if let edgeVariable = step.edge.variable {
            guard bind("\(edgeVariable).label", value: .string(edge.edgeLabel), into: &bindings) else {
                return nil
            }
            for (propertyName, propertyValue) in edge.properties {
                let fieldValue = FieldValue(propertyValue) ?? .string(String(describing: propertyValue))
                guard bind("\(edgeVariable).\(propertyName)", value: fieldValue, into: &bindings) else {
                    return nil
                }
            }
        }

        return bindings
    }

    private func bind(
        _ key: String,
        value: FieldValue,
        into bindings: inout [String: FieldValue]
    ) -> Bool {
        if let existing = bindings[key] {
            return existing == value
        }
        bindings[key] = value
        return true
    }

    private func deduplicated(states: [MatchState]) -> [MatchState] {
        var seen = Set<[String: FieldValue]>()
        var unique: [MatchState] = []
        for state in states where seen.insert(state.bindings).inserted {
            unique.append(state)
        }
        return unique
    }

    private func makeRow(from state: MatchState) throws -> GraphTableRow {
        guard let first = state.matchedSteps.first,
              let last = state.matchedSteps.last else {
            throw GraphTableError.invalidGraphPattern("GRAPH_TABLE produced an empty match")
        }

        let mergedProperties = first.edge.properties
        var fields: [String: FieldValue] = [
            "source": .string(first.leftID),
            "target": .string(last.rightID),
            "edgeLabel": .string(first.edge.edgeLabel)
        ]
        for (key, value) in mergedProperties {
            fields[key] = FieldValue(value) ?? .string(String(describing: value))
        }
        for (key, value) in state.bindings {
            fields[key] = value
        }

        return GraphTableRow(
            source: first.leftID,
            target: last.rightID,
            edgeLabel: first.edge.edgeLabel,
            properties: mergedProperties,
            fields: fields
        )
    }

    private func evaluateBoolean(
        _ expression: QueryIR.Expression,
        fields: [String: FieldValue]
    ) throws -> Bool {
        switch expression {
        case .column:
            let value = try evaluateExpression(expression, fields: fields)
            guard let boolValue = value.boolValue else {
                throw GraphTableError.invalidColumnExpression("Boolean expression must resolve to Bool")
            }
            return boolValue
        case .literal(let literal):
            guard let boolValue = literal.toFieldValue()?.boolValue else {
                throw GraphTableError.typeMismatch("Boolean expression must resolve to Bool")
            }
            return boolValue
        case .equal(let lhs, let rhs):
            return try evaluateExpression(lhs, fields: fields) == evaluateExpression(rhs, fields: fields)
        case .notEqual(let lhs, let rhs):
            return try evaluateExpression(lhs, fields: fields) != evaluateExpression(rhs, fields: fields)
        case .lessThan(let lhs, let rhs):
            return try evaluateExpression(lhs, fields: fields).isLessThan(evaluateExpression(rhs, fields: fields))
        case .lessThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, fields: fields)
            let right = try evaluateExpression(rhs, fields: fields)
            return left == right || left.isLessThan(right)
        case .greaterThan(let lhs, let rhs):
            return try evaluateExpression(rhs, fields: fields).isLessThan(evaluateExpression(lhs, fields: fields))
        case .greaterThanOrEqual(let lhs, let rhs):
            let left = try evaluateExpression(lhs, fields: fields)
            let right = try evaluateExpression(rhs, fields: fields)
            return left == right || right.isLessThan(left)
        case .and(let lhs, let rhs):
            return try evaluateBoolean(lhs, fields: fields) && evaluateBoolean(rhs, fields: fields)
        case .or(let lhs, let rhs):
            return try evaluateBoolean(lhs, fields: fields) || evaluateBoolean(rhs, fields: fields)
        case .not(let inner):
            return try !evaluateBoolean(inner, fields: fields)
        case .isNull(let inner):
            return try evaluateExpression(inner, fields: fields) == .null
        case .isNotNull(let inner):
            return try evaluateExpression(inner, fields: fields) != .null
        default:
            throw GraphTableError.invalidColumnExpression("Unsupported GRAPH_TABLE WHERE expression")
        }
    }

    private func evaluateExpression(
        _ expression: QueryIR.Expression,
        fields: [String: FieldValue]
    ) throws -> FieldValue {
        switch expression {
        case .column(let column):
            if let table = column.table {
                let qualified = "\(table).\(column.column)"
                guard let value = fields[qualified] ?? fields[column.column] else {
                    throw GraphTableError.invalidColumnExpression("Unknown column '\(qualified)'")
                }
                return value
            }
            guard let value = fields[column.column] else {
                throw GraphTableError.invalidColumnExpression("Unknown column '\(column.column)'")
            }
            return value
        case .literal(let literal):
            return try convertLiteralToFieldValue(literal)
        default:
            // GRAPH_TABLE filtering on the canonical path is intentionally scoped
            // to column and literal expressions for now.
            throw GraphTableError.invalidColumnExpression("Unsupported GRAPH_TABLE expression")
        }
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
    private func findGraphIndex() throws -> IndexDescriptor {
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
            graphTableSource: source
        )
        return try await executor.execute()
    }
}
