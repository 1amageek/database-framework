/// GraphTableExecutor.swift
/// SQL/PGQ GRAPH_TABLE query executor

@_spi(DatabaseExecution) import DatabaseEngine
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
struct GraphTableExecutor: Sendable {
    private let graphTableSource: GraphTableSource
    private let indexDescriptor: IndexDescriptor
    private let indexSubspace: Subspace

    init(
        indexDescriptor: IndexDescriptor,
        indexSubspace: Subspace,
        graphTableSource: GraphTableSource
    ) {
        self.indexDescriptor = indexDescriptor
        self.indexSubspace = indexSubspace
        self.graphTableSource = graphTableSource
    }

    /// Executes the canonical GRAPH_TABLE source while retaining the request's
    /// memory reservation and caller-supplied transaction through the adapter
    /// boundary.
    func executeRetainedCanonicalRows(
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedQueryRows {
        let states = try await matchingStates(
            transaction: transaction,
            workMeter: workMeter
        )
        var output = try DatabaseRetainedArrayBuilder<QueryRow>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: QueryRow.self),
            expectedCount: states.count
        )
        for index in states.startIndex..<states.endIndex {
            try states.withElement(at: index) { state in
                try workMeter.consume(at: .bindingCandidate)
                let graphRow = try makeRow(from: state)
                if let filter = graphTableSource.matchPattern.where,
                   try !evaluateBoolean(filter, fields: graphRow.fields) {
                    return
                }
                let row = QueryRow(fields: graphRow.fields)
                try output.append(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: row,
                        workMeter: workMeter
                    ),
                    make: { row }
                )
            }
        }
        let owner = try output.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
        return DatabaseRetainedQueryRows(
            owner: owner,
            visibleRange: owner.startIndex..<owner.endIndex
        )
    }

    private func matchingStates(
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<MatchState> {
        let steps = try extractSteps(from: graphTableSource.matchPattern)
        guard !steps.isEmpty else {
            throw GraphTableError.invalidGraphPattern("No edge patterns found in MATCH clause")
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
            includedFieldNames: indexDescriptor.includedFieldNames,
            workMeter: workMeter
        )

        var initial = try DatabaseRetainedArrayBuilder<MatchState>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: MatchState.self),
            expectedCount: 1
        )
        try initial.append(
            footprint: DatabaseIntermediateFootprint(rows: 1, bytes: 64),
            make: { MatchState() }
        )
        var states = try initial.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )

        for step in steps {
            states = try await extend(
                states: states,
                with: step,
                scanner: scanner,
                strategy: configuration.strategy,
                transaction: transaction,
                workMeter: workMeter
            )
            if states.isEmpty {
                return states
            }
        }
        return states
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

    /// Computes a conservative retained footprint before MATCH expansion
    /// allocates a new binding Dictionary or matched-step Array. The charge is
    /// intentionally conservative where a prospective binding may already be
    /// present; over-admission is preferable to an unaccounted fan-out path.
    private final class GraphTableMatchStateFootprintMeter {
        private static let stateHeaderByteCount: UInt64 = 64
        private static let matchedStepSlotByteCount: UInt64 = 256
        private static let prospectiveBindingSlotByteCount: UInt64 = 128
        private static let edgeHeaderByteCount: UInt64 = 256
        private static let propertySlotByteCount: UInt64 = 96

        private let fieldMeter: SPARQLBindingFootprintMeter

        private init(fieldMeter: SPARQLBindingFootprintMeter) {
            self.fieldMeter = fieldMeter
        }

        static func make(
            workMeter: DatabaseWorkMeter
        ) throws -> GraphTableMatchStateFootprintMeter {
            GraphTableMatchStateFootprintMeter(
                fieldMeter: try SPARQLBindingFootprintMeter.make(
                    workMeter: workMeter,
                    stage: .bindingCandidate
                )
            )
        }

        func shutdown() {
            fieldMeter.shutdown()
        }

        func footprint(
            of state: borrowing MatchState
        ) throws -> DatabaseIntermediateFootprint {
            var footprint = try fieldMeter.footprint(
                of: VariableBinding(state.bindings)
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.stateHeaderByteCount
                )
            ).adding(
                try DatabaseIntermediateFootprint(
                    bytes: Self.matchedStepSlotByteCount
                ).multiplied(by: UInt64(state.matchedSteps.count))
            )
            for matchedStep in state.matchedSteps {
                footprint = try footprint.adding(
                    edgeFootprint(
                        edge: matchedStep.edge,
                        leftID: matchedStep.leftID,
                        rightID: matchedStep.rightID
                    )
                )
            }
            return footprint
        }

        func prospectiveFootprint(
            retaining state: borrowing MatchState,
            step: borrowing Step,
            edge: borrowing GraphEdgeWithProperties,
            leftID: borrowing String,
            rightID: borrowing String
        ) throws -> DatabaseIntermediateFootprint {
            var footprint = try footprint(of: state).adding(
                DatabaseIntermediateFootprint(
                    bytes: Self.matchedStepSlotByteCount
                )
            ).adding(
                try edgeFootprint(
                    edge: edge,
                    leftID: leftID,
                    rightID: rightID
                )
            )

            if let variable = step.left.variable {
                footprint = try footprint.adding(
                    prospectiveBindingFootprint(
                        keyUTF8Count: variable.utf8.count + 3,
                        value: .string(copy leftID)
                    )
                )
            }
            if let variable = step.right.variable {
                footprint = try footprint.adding(
                    prospectiveBindingFootprint(
                        keyUTF8Count: variable.utf8.count + 3,
                        value: .string(copy rightID)
                    )
                )
            }
            if let variable = step.edge.variable {
                let label = try edge.edgeLabel
                    .requirePropertyGraphIdentifier()
                footprint = try footprint.adding(
                    prospectiveBindingFootprint(
                        keyUTF8Count: variable.utf8.count + 6,
                        value: .string(label)
                    )
                )
                for (propertyName, propertyValue) in edge.properties {
                    footprint = try footprint.adding(
                        prospectiveBindingFootprint(
                            keyUTF8Count: variable.utf8.count
                                + propertyName.utf8.count + 1,
                            value: propertyValue
                        )
                    )
                }
            }
            return footprint
        }

        private func edgeFootprint(
            edge: borrowing GraphEdgeWithProperties,
            leftID: borrowing String,
            rightID: borrowing String
        ) throws -> DatabaseIntermediateFootprint {
            let label = try edge.edgeLabel.requirePropertyGraphIdentifier()
            var footprint = DatabaseIntermediateFootprint(
                bytes: Self.edgeHeaderByteCount
                    + UInt64(leftID.utf8.count)
                    + UInt64(rightID.utf8.count)
                    + UInt64(label.utf8.count)
            )
            for (propertyName, propertyValue) in edge.properties {
                footprint = try footprint.adding(
                    DatabaseIntermediateFootprint(
                        bytes: Self.propertySlotByteCount
                            + UInt64(propertyName.utf8.count)
                    )
                ).adding(
                    try fieldMeter.footprint(of: propertyValue)
                )
            }
            return footprint
        }

        private func prospectiveBindingFootprint(
            keyUTF8Count: Int,
            value: borrowing FieldValue
        ) throws -> DatabaseIntermediateFootprint {
            try DatabaseIntermediateFootprint(
                bytes: Self.prospectiveBindingSlotByteCount
                    + UInt64(keyUTF8Count)
            ).adding(
                try fieldMeter.footprint(of: value)
            )
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
        states: DatabaseSharedRetainedArray<MatchState>,
        with step: Step,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<MatchState> {
        var nextStates = try DatabaseRetainedArrayBuilder<MatchState>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: MatchState.self)
        )
        var seen = try SPARQLRetainedBindingSet.make(
            workMeter: workMeter,
            stage: .deduplication
        )
        let footprintMeter = try GraphTableMatchStateFootprintMeter.make(
            workMeter: workMeter
        )
        defer { footprintMeter.shutdown() }

        for stateIndex in states.startIndex..<states.endIndex {
            let matches = try await states.withElement(at: stateIndex) {
                state in
                try await match(
                    step: step,
                    state: state,
                    scanner: scanner,
                    strategy: strategy,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            for matchIndex in matches.startIndex..<matches.endIndex {
                try matches.withElement(at: matchIndex) { state in
                    let binding = VariableBinding(state.bindings)
                    guard try seen.insert(binding) else { return }
                    let footprint = try footprintMeter.footprint(of: state)
                    let admission = try nextStates.prepareAppend(
                        footprint: footprint,
                        at: .bindingCandidate
                    )
                    nextStates.append(copy state, using: admission)
                }
            }
        }
        return try nextStates.finish().moveToSharedOwnership(
            at: .bindingCandidate
        )
    }

    private func match(
        step: Step,
        state: MatchState,
        scanner: GraphPropertyScanner,
        strategy: GraphIndexStrategy,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<MatchState> {
        let leftResolution = try resolveIdentity(for: step.left, bindings: state.bindings)
        let rightResolution = try resolveIdentity(for: step.right, bindings: state.bindings)
        if leftResolution == .impossible || rightResolution == .impossible {
            return try DatabaseSharedRetainedArray.empty(
                workMeter: workMeter,
                stage: .bindingCandidate
            )
        }

        let propertyFilters = try convertToPropertyFilters(step.edge.properties)
        let labels: [String?] = step.edge.labels?.isEmpty == false
            ? step.edge.labels!.map(Optional.some)
            : [nil]
        var matches = try DatabaseRetainedArrayBuilder<MatchState>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: MatchState.self)
        )
        var seen = try SPARQLRetainedBindingSet.make(
            workMeter: workMeter,
            stage: .deduplication
        )
        let footprintMeter = try GraphTableMatchStateFootprintMeter.make(
            workMeter: workMeter
        )
        defer { footprintMeter.shutdown() }

        for orientation in traversals(for: step.edge.direction) {
            for label in labels {
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
                while let edge = try await edgeCursor.next() {
                    let (leftID, rightID) = try endpointIDs(
                        for: edge,
                        orientation: orientation
                    )
                    guard endpointMatches(leftID, resolution: leftResolution),
                          endpointMatches(rightID, resolution: rightResolution) else {
                        continue
                    }
                    let footprint = try footprintMeter.prospectiveFootprint(
                        retaining: state,
                        step: step,
                        edge: edge,
                        leftID: leftID,
                        rightID: rightID
                    )
                    let admission = try matches.prepareAppend(
                        footprint: footprint,
                        at: .bindingCandidate
                    )
                    guard let bindings = try bind(
                        step: step,
                        leftID: leftID,
                        rightID: rightID,
                        edge: edge,
                        onto: state.bindings
                    ) else {
                        continue
                    }
                    let candidate = MatchState(
                        bindings: bindings,
                        matchedSteps: state.matchedSteps + [
                            MatchedStep(
                                step: step,
                                edge: edge,
                                leftID: leftID,
                                rightID: rightID
                            )
                        ]
                    )
                    let binding = VariableBinding(candidate.bindings)
                    guard try seen.insert(binding) else { continue }
                    matches.append(candidate, using: admission)
                }
            }
        }

        return try matches.finish().moveToSharedOwnership(
            at: .bindingCandidate
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

    private func extractNodeID(from expression: Expression) throws -> String {
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
    ) throws -> [String: FieldValue]? {
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
            let edgeLabel = try edge.edgeLabel.requirePropertyGraphIdentifier()
            guard bind("\(edgeVariable).label", value: .string(edgeLabel), into: &bindings) else {
                return nil
            }
            for (propertyName, propertyValue) in edge.properties {
                guard bind("\(edgeVariable).\(propertyName)", value: propertyValue, into: &bindings) else {
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

    private func makeRow(from state: MatchState) throws -> GraphTableRow {
        guard let first = state.matchedSteps.first,
              let last = state.matchedSteps.last else {
            throw GraphTableError.invalidGraphPattern("GRAPH_TABLE produced an empty match")
        }

        let mergedProperties = first.edge.properties
        let edgeLabel = try first.edge.edgeLabel.requirePropertyGraphIdentifier()
        var fields: [String: FieldValue] = [
            "source": .string(first.leftID),
            "target": .string(last.rightID),
            "edgeLabel": .string(edgeLabel),
        ]
        for (key, value) in mergedProperties {
            fields[key] = value
        }
        for (key, value) in state.bindings {
            fields[key] = value
        }

        return try GraphTableRow(
            source: first.leftID,
            target: last.rightID,
            edgeLabel: edgeLabel,
            properties: mergedProperties,
            fields: fields
        )
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
    private func extractLiteralValue(from expression: Expression) throws -> FieldValue {
        guard case .literal(let literal) = expression else {
            throw GraphTableError.complexPropertyExpression(
                "Property comparison value must be a literal, not an expression"
            )
        }
        return try convertLiteralToFieldValue(literal)
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
        guard let descriptor = indexQueryContext
            .indexDescriptors(for: T.self)
            .first(
            where: { $0.type == .graph(.property) }
                ) else {
            throw GraphTableError.indexNotFound(
                "No property-graph index found for entity \(T.persistableType)"
            )
        }
        var canonicalColumnNames = ["source", "target", "edgeLabel"]
        for fieldName in descriptor.includedFieldNames
            where !canonicalColumnNames.contains(fieldName) {
            canonicalColumnNames.append(fieldName)
        }
        let canonicalSource: GraphTableSource
        if source.columns?.isEmpty != false {
            canonicalSource = GraphTableSource(
                graphName: source.graphName,
                matchPattern: source.matchPattern,
                columns: canonicalColumnNames.map { fieldName in
                    GraphTableColumn(
                        expression: .column(
                            ColumnRef(column: fieldName)
                        ),
                        alias: fieldName
                    )
                },
                alias: source.alias
            )
        } else {
            canonicalSource = source
        }
        let response = try await query(
            SelectQuery(
                projection: .all,
                source: .graphTable(canonicalSource)
            )
        )
        let propertyNames = Set(descriptor.includedFieldNames)
        return try response.rows.map { row in
            guard let sourceID = row.fields["source"]?.stringValue,
                  let targetID = row.fields["target"]?.stringValue,
                  let edgeLabel = row.fields["edgeLabel"]?.stringValue else {
                throw GraphTableError.invalidGraphPattern(
                    "Canonical GRAPH_TABLE result is missing an endpoint or edge label"
                )
            }
            let properties = row.fields.filter { propertyNames.contains($0.key) }
            return try GraphTableRow(
                source: sourceID,
                target: targetID,
                edgeLabel: edgeLabel,
                properties: properties,
                fields: row.fields
            )
        }
    }
}
