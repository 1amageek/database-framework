/// GraphTableExecutor.swift
/// SQL/PGQ GRAPH_TABLE query executor

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

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
    public let properties: [String: FieldValue]

    /// Flattened row fields used by canonical graph-table projection.
    public let fields: [String: FieldValue]

    public init(
        source: String,
        target: String,
        edgeLabel: String,
        properties: [String: FieldValue],
        fields: [String: FieldValue]? = nil
    ) throws {
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
                "edgeLabel": .string(edgeLabel),
            ]
            for (key, value) in properties {
                fields[key] = value
            }
            self.fields = fields
        }
    }
}

// MARK: - GraphTableExecutor

/// Executor for SQL/PGQ GRAPH_TABLE queries
///
/// Converts SQL/PGQ match patterns to GraphPropertyScanner calls with property filtering.
package struct GraphTableExecutor: Sendable {
    private let graphTableSource: GraphTableSource
    private let indexDescriptor: IndexDescriptor
    private let indexSubspace: Subspace

    package init(
        indexDescriptor: IndexDescriptor,
        indexSubspace: Subspace,
        graphTableSource: GraphTableSource
    ) {
        self.indexDescriptor = indexDescriptor
        self.indexSubspace = indexSubspace
        self.graphTableSource = graphTableSource
    }

    /// Rejects graph semantics that this executor cannot preserve.
    ///
    /// Runtime adapters call this before opening an index so unsupported
    /// syntax cannot turn into an empty success when the index has no rows.
    package static func validate(
        _ graphTableSource: GraphTableSource
    ) throws {
        _ = try Self.steps(from: graphTableSource.matchPattern)
        if let filter = graphTableSource.matchPattern.where {
            try Self.validateBooleanExpression(filter)
        }
    }

    /// Executes one GRAPH_TABLE source while retaining every intermediate row
    /// under the caller's request meter and transaction.
    package func execute(
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedQueryRows {
        let steps = try Self.steps(from: graphTableSource.matchPattern)
        if let filter = graphTableSource.matchPattern.where {
            try Self.validateBooleanExpression(filter)
        }

        guard
            let configuration = PropertyGraphIndexConfiguration(
                descriptor: indexDescriptor
            )
        else {
            throw GraphTableError.indexNotFound(
                "Index '\(indexDescriptor.name)' is not a property-graph index"
            )
        }
        let scanner = GraphPropertyScanner(
            indexSubspace: indexSubspace,
            strategy: configuration.strategy,
            includedFieldNames: indexDescriptor.includedFieldNames
        )

        var states = try await match(
            step: steps[0],
            scanner: scanner,
            strategy: configuration.strategy,
            transaction: transaction,
            workMeter: workMeter
        )
        for step in steps.dropFirst() {
            states = try await extend(
                states: consume states,
                with: step,
                scanner: scanner,
                strategy: configuration.strategy,
                transaction: transaction,
                workMeter: workMeter
            )
            guard !states.isEmpty else { break }
        }

        guard let filter = graphTableSource.matchPattern.where,
              !states.isEmpty else {
            return DatabaseRetainedQueryRows(storage: consume states)
        }

        var released = DatabaseIntermediateFootprint()
        let filtered = try states.stableCompacting { row in
            try workMeter.consume(at: .filterEvaluation)
            let isIncluded = try evaluateBoolean(
                filter,
                fields: row.fields
            )
            if !isIncluded {
                released = try released.adding(
                    CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter,
                        stage: .filterEvaluation
                    )
                )
            }
            return isIncluded
        }
        let accounted = try filtered.releasingRetainedFootprint(released)
        return DatabaseRetainedQueryRows(storage: consume accounted)
    }

    // MARK: - Pattern Evaluation

    private struct Step: Sendable {
        let left: NodePattern
        let edge: EdgePattern
        let right: NodePattern
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

    private static func steps(from matchPattern: MatchPattern) throws -> [Step] {
        var result: [Step] = []
        for path in matchPattern.paths {
            result.append(contentsOf: try steps(from: path))
        }
        guard !result.isEmpty else {
            throw GraphTableError.invalidGraphPattern(
                "No edge patterns found in MATCH clause"
            )
        }
        return result
    }

    private static func steps(from path: PathPattern) throws -> [Step] {
        guard path.pathVariable == nil else {
            throw GraphTableError.invalidGraphPattern(
                "GRAPH_TABLE path variables are not supported"
            )
        }
        switch path.mode {
        case nil, .walk:
            break
        case .trail:
            throw GraphTableError.invalidGraphPattern(
                "TRAIL GRAPH_TABLE path mode is not supported"
            )
        case .acyclic:
            throw GraphTableError.invalidGraphPattern(
                "ACYCLIC GRAPH_TABLE path mode is not supported"
            )
        case .simple:
            throw GraphTableError.invalidGraphPattern(
                "SIMPLE GRAPH_TABLE path mode is not supported"
            )
        case .anyShortest, .allShortest, .shortestK:
            throw GraphTableError.invalidGraphPattern(
                "Shortest-path GRAPH_TABLE execution is not supported on the canonical read path"
            )
        }

        guard !path.elements.isEmpty else { return [] }
        for element in path.elements {
            switch element {
            case .node(let node):
                try validate(node)
            case .edge(let edge):
                _ = try propertyFilters(from: edge.properties)
            case .quantified:
                throw GraphTableError.invalidGraphPattern(
                    "Quantified GRAPH_TABLE paths are not supported"
                )
            case .alternation:
                throw GraphTableError.invalidGraphPattern(
                    "Alternating GRAPH_TABLE paths are not supported"
                )
            }
        }
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

    private static func validate(_ node: NodePattern) throws {
        guard node.labels?.isEmpty != false else {
            throw GraphTableError.invalidGraphPattern(
                "GRAPH_TABLE node labels are not supported"
            )
        }
        for property in node.properties ?? [] {
            guard property.key == "id" else {
                throw GraphTableError.invalidGraphPattern(
                    "Node property '\(property.key)' is not supported; only 'id' constraints can be evaluated"
                )
            }
            _ = try nodeIdentifier(from: property.value)
        }
    }

    private func extend(
        states: consuming DatabaseRetainedBuffer<QueryRow>,
        with step: Step,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedBuffer<QueryRow> {
        var nextStates = try makeStateBuilder(workMeter: workMeter)
        for index in 0..<states.count {
            try await states.withElement(at: index) { state in
                try await appendMatches(
                    step: step,
                    state: state,
                    isInitial: false,
                    scanner: scanner,
                    strategy: strategy,
                    transaction: transaction,
                    workMeter: workMeter,
                    into: &nextStates
                )
            }
        }
        return nextStates.finish()
    }

    private func match(
        step: Step,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedBuffer<QueryRow> {
        var matches = try makeStateBuilder(workMeter: workMeter)
        let initialState = QueryRow(fields: [:])
        try await appendMatches(
            step: step,
            state: initialState,
            isInitial: true,
            scanner: scanner,
            strategy: strategy,
            transaction: transaction,
            workMeter: workMeter,
            into: &matches
        )
        return matches.finish()
    }

    private func appendMatches(
        step: Step,
        state: borrowing QueryRow,
        isInitial: Bool,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        into matches: inout DatabaseRetainedArrayBuilder<QueryRow>
    ) async throws {
        let stateCopy = copy state
        let fields = stateCopy.fields
        let leftResolution = try resolveIdentity(
            for: step.left,
            bindings: fields
        )
        let rightResolution = try resolveIdentity(
            for: step.right,
            bindings: fields
        )
        if leftResolution == .impossible || rightResolution == .impossible {
            return
        }

        let propertyFilters = try Self.propertyFilters(
            from: step.edge.properties
        )
        let labels: [String?] = step.edge.labels?.isEmpty == false
            ? step.edge.labels!.map(Optional.some)
            : [nil]

        for orientation in traversals(for: step.edge.direction) {
            for label in labels {
                try workMeter.checkpoint(at: .pathExpansion)
                let stream = scanner.scanEdges(
                    from: scanFrom(
                        left: leftResolution,
                        right: rightResolution,
                        orientation: orientation,
                        strategy: strategy
                    ).map(GraphIdentity.identifier),
                    edge: label.map(GraphIdentity.identifier),
                    to: scanTo(
                        left: leftResolution,
                        right: rightResolution,
                        orientation: orientation,
                        strategy: strategy
                    ).map(GraphIdentity.identifier),
                    graphTarget: .all,
                    propertyFilters: propertyFilters.isEmpty ? nil : propertyFilters,
                    transaction: transaction
                )

                var edgeCursor = stream.makeCursor()
                while true {
                    try workMeter.checkpoint(at: .pathExpansion)
                    guard let edge = try await edgeCursor.next() else {
                        break
                    }
                    try workMeter.consume(at: .pathExpansion)
                    let (leftID, rightID) = try endpointIDs(
                        for: edge,
                        orientation: orientation
                    )
                    guard endpointMatches(leftID, resolution: leftResolution),
                          endpointMatches(rightID, resolution: rightResolution) else {
                        continue
                    }
                    let footprint = try prospectiveFootprint(
                        retaining: state,
                        step: step,
                        leftID: leftID,
                        rightID: rightID,
                        edge: edge,
                        isInitial: isInitial,
                        workMeter: workMeter
                    )
                    let admission = try matches.prepareAppend(
                        footprint: footprint,
                        at: .pathExpansion
                    )
                    guard let row = try makeMatchRow(
                        step: step,
                        leftID: leftID,
                        rightID: rightID,
                        edge: edge,
                        extending: state,
                        isInitial: isInitial
                    ) else {
                        continue
                    }
                    matches.append(row, using: consume admission)
                }
            }
        }
    }

    /// Computes a conservative final-row claim without materializing the
    /// row's independently mutable Dictionary. Replaced bindings may be
    /// counted twice, so admission can reject conservatively but can never
    /// retain more memory than it owns.
    private func prospectiveFootprint(
        retaining state: borrowing QueryRow,
        step: borrowing Step,
        leftID: borrowing String,
        rightID: borrowing String,
        edge: borrowing GraphEdgeWithProperties,
        isInitial: Bool,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: state,
            workMeter: workMeter,
            stage: .pathExpansion
        )

        func addingField(
            nameUTF8Count: Int,
            value: borrowing FieldValue
        ) throws -> DatabaseIntermediateFootprint {
            try footprint.adding(
                CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                    nameUTF8Count: nameUTF8Count,
                    value: value,
                    workMeter: workMeter,
                    stage: .pathExpansion
                )
            )
        }

        if isInitial {
            footprint = try addingField(
                nameUTF8Count: "source".utf8.count,
                value: .string(copy leftID)
            )
            footprint = try addingField(
                nameUTF8Count: "target".utf8.count,
                value: .string(copy rightID)
            )
            footprint = try addingField(
                nameUTF8Count: "edgeLabel".utf8.count,
                value: .string(
                    try edge.edgeLabel.requirePropertyGraphIdentifier()
                )
            )
            for (name, value) in edge.properties {
                footprint = try addingField(
                    nameUTF8Count: name.utf8.count,
                    value: value
                )
            }
        } else {
            footprint = try addingField(
                nameUTF8Count: "target".utf8.count,
                value: .string(copy rightID)
            )
        }

        if let variable = step.left.variable {
            footprint = try addingField(
                nameUTF8Count: variable.utf8.count + ".id".utf8.count,
                value: .string(copy leftID)
            )
        }
        if let variable = step.right.variable {
            footprint = try addingField(
                nameUTF8Count: variable.utf8.count + ".id".utf8.count,
                value: .string(copy rightID)
            )
        }
        if let variable = step.edge.variable {
            footprint = try addingField(
                nameUTF8Count: variable.utf8.count + ".label".utf8.count,
                value: .string(
                    try edge.edgeLabel.requirePropertyGraphIdentifier()
                )
            )
            for (propertyName, propertyValue) in edge.properties {
                footprint = try addingField(
                    nameUTF8Count: variable.utf8.count
                        + ".".utf8.count
                        + propertyName.utf8.count,
                    value: propertyValue
                )
            }
        }
        return footprint
    }

    private func makeStateBuilder(
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedArrayBuilder<QueryRow> {
        try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .pathExpansion,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self)
        )
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
        case .adjacency, .hexastore, .quadStore:
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
    ) throws -> (left: String, right: String) {
        switch orientation {
        case .outgoing:
            return (
                try edge.source.requirePropertyGraphIdentifier(),
                try edge.target.requirePropertyGraphIdentifier()
            )
        case .incoming:
            return (
                try edge.target.requirePropertyGraphIdentifier(),
                try edge.source.requirePropertyGraphIdentifier()
            )
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
            exactValues.append(try Self.nodeIdentifier(from: property.value))
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

    private static func nodeIdentifier(
        from expression: Expression
    ) throws -> String {
        switch expression {
        case .literal(let literal):
            let value = try literal.toFieldValue()
            guard let stringValue = value.stringValue else {
                throw GraphTableError.typeMismatch("Node id constraint must resolve to a string value")
            }
            return stringValue
        case .equal(let lhs, let rhs):
            try validateFieldReference(lhs, fieldName: "id")
            let value = try literalValue(from: rhs)
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

    private func makeMatchRow(
        step: Step,
        leftID: String,
        rightID: String,
        edge: GraphEdgeWithProperties,
        extending existing: borrowing QueryRow,
        isInitial: Bool
    ) throws -> QueryRow? {
        // The caller holds a conservative reservation before this mutation.
        // Swift COW keeps existing FieldValue payload owners shared while the
        // match receives its independently mutable binding map.
        let existingCopy = copy existing
        var fields = existingCopy.fields
        if isInitial {
            let edgeLabel = try edge.edgeLabel
                .requirePropertyGraphIdentifier()
            fields["source"] = .string(leftID)
            fields["target"] = .string(rightID)
            fields["edgeLabel"] = .string(edgeLabel)
            for (name, value) in edge.properties {
                fields[name] = value
            }
        } else {
            fields["target"] = .string(rightID)
        }

        if let leftVariable = step.left.variable {
            guard bind(
                "\(leftVariable).id",
                value: .string(leftID),
                into: &fields
            ) else {
                return nil
            }
        }
        if let rightVariable = step.right.variable {
            guard bind(
                "\(rightVariable).id",
                value: .string(rightID),
                into: &fields
            ) else {
                return nil
            }
        }
        if let edgeVariable = step.edge.variable {
            let edgeLabel = try edge.edgeLabel.requirePropertyGraphIdentifier()
            guard bind(
                "\(edgeVariable).label",
                value: .string(edgeLabel),
                into: &fields
            ) else {
                return nil
            }
            for (propertyName, propertyValue) in edge.properties {
                guard bind(
                    "\(edgeVariable).\(propertyName)",
                    value: propertyValue,
                    into: &fields
                ) else {
                    return nil
                }
            }
        }

        return QueryRow(fields: fields)
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

    private func evaluateBoolean(
        _ expression: Expression,
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
            guard let boolValue = try literal.toFieldValue().boolValue else {
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
        _ expression: Expression,
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

    private static func validateBooleanExpression(
        _ expression: Expression
    ) throws {
        switch expression {
        case .column, .literal:
            break
        case .equal(let lhs, let rhs),
                .notEqual(let lhs, let rhs),
                .lessThan(let lhs, let rhs),
                .lessThanOrEqual(let lhs, let rhs),
                .greaterThan(let lhs, let rhs),
                .greaterThanOrEqual(let lhs, let rhs):
            try validateScalarExpression(lhs)
            try validateScalarExpression(rhs)
        case .and(let lhs, let rhs), .or(let lhs, let rhs):
            try validateBooleanExpression(lhs)
            try validateBooleanExpression(rhs)
        case .not(let inner):
            try validateBooleanExpression(inner)
        case .isNull(let inner), .isNotNull(let inner):
            try validateScalarExpression(inner)
        default:
            throw GraphTableError.invalidColumnExpression(
                "Unsupported GRAPH_TABLE WHERE expression"
            )
        }
    }

    private static func validateScalarExpression(
        _ expression: Expression
    ) throws {
        switch expression {
        case .column, .literal:
            break
        default:
            throw GraphTableError.invalidColumnExpression(
                "Unsupported GRAPH_TABLE expression"
            )
        }
    }

    // MARK: - Property Filter Conversion

    /// Convert EdgePattern properties to PropertyFilter array
    ///
    /// Simple equality and comparison expressions are supported.
    /// Complex expressions (subqueries, functions) throw an error.
    private static func propertyFilters(
        from properties: [PropertyBinding]?
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
                let fieldValue = try literal.toFieldValue()
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .equal,
                    value: fieldValue
                ))

            case .equal(let lhs, let rhs):
                // Property comparison: {since = 2020}
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .equal,
                    value: fieldValue
                ))

            case .notEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .notEqual,
                    value: fieldValue
                ))

            case .lessThan(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .lessThan,
                    value: fieldValue
                ))

            case .lessThanOrEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .lessThanOrEqual,
                    value: fieldValue
                ))

            case .greaterThan(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
                filters.append(PropertyFilter(
                    fieldName: fieldName,
                    op: .greaterThan,
                    value: fieldValue
                ))

            case .greaterThanOrEqual(let lhs, let rhs):
                try validateFieldReference(lhs, fieldName: fieldName)
                let fieldValue = try literalValue(from: rhs)
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
    private static func validateFieldReference(
        _ expression: Expression,
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
    private static func literalValue(
        from expression: Expression
    ) throws -> FieldValue {
        guard case .literal(let literal) = expression else {
            throw GraphTableError.complexPropertyExpression(
                "Property comparison value must be a literal, not an expression"
            )
        }
        return try literal.toFieldValue()
    }

    /// Convert Literal to DatabaseEngine.FieldValue
    private func convertLiteralToFieldValue(_ literal: Literal) throws -> FieldValue {
        try literal.toFieldValue()
    }

}

// MARK: - DatabaseContext Extension

extension DatabaseContext {
    /// Execute SQL/PGQ GRAPH_TABLE query
    ///
    /// Example:
    /// ```swift
    /// let source = GraphTableSource(
    ///     graphName: "social_edge_index",
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
        source: GraphTableSource,
        options: ReadExecutionOptions = .default
    ) async throws -> [GraphTableRow] {
        try GraphTableExecutor.validate(source)
        guard let resolution = try PropertyGraphReadResolver.resolve(
            graphName: source.graphName,
            schema: container.schema
        ) else {
            throw GraphTableError.indexNotFound(
                try PropertyGraphReadResolver.errorMessage(
                    graphName: source.graphName,
                    schema: container.schema
                )
            )
        }
        guard resolution.entity.name == T.persistableType else {
            throw GraphTableError.indexNotFound(
                "Property graph '\(source.graphName)' belongs to entity '\(resolution.entity.name)', not '\(T.persistableType)'"
            )
        }

        var canonicalColumnNames = ["source", "target", "edgeLabel"]
        for fieldName in resolution.indexDescriptor.includedFieldNames
            where !canonicalColumnNames.contains(fieldName) {
            canonicalColumnNames.append(fieldName)
        }
        let canonicalSource = GraphTableSource(
            graphName: source.graphName,
            matchPattern: source.matchPattern,
            columns: canonicalColumnNames.map { fieldName in
                GraphTableColumn(
                    expression: .column(
                        ColumnRef(
                            table: source.graphName,
                            column: fieldName
                        )
                    ),
                    alias: fieldName
                )
            }
        )
        let response = try await query(
            SelectQuery(
                projection: .all,
                source: .graphTable(canonicalSource)
            ),
            options: options
        )
        let propertyNames = resolution.indexDescriptor.includedFieldNames
        var rows: [GraphTableRow] = []
        rows.reserveCapacity(response.rows.count)
        for row in response.rows {
            guard let sourceID = row.fields["source"]?.stringValue,
                  let targetID = row.fields["target"]?.stringValue,
                  let edgeLabel = row.fields["edgeLabel"]?.stringValue else {
                throw GraphTableError.invalidGraphPattern(
                    "Canonical GRAPH_TABLE result is missing an endpoint or edge label"
                )
            }
            var properties: [String: FieldValue] = [:]
            properties.reserveCapacity(propertyNames.count)
            for propertyName in propertyNames {
                if let value = row.fields[propertyName] {
                    properties[propertyName] = value
                }
            }
            // The public API changes the element type at its output boundary.
            // FieldValue payload owners remain shared through Swift COW.
            rows.append(
                try GraphTableRow(
                    source: sourceID,
                    target: targetID,
                    edgeLabel: edgeLabel,
                    properties: properties,
                    fields: row.fields
                )
            )
        }
        return rows
    }
}
