import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

/// A graph edge and its covering-index properties.
public struct GraphEdgeWithProperties: Sendable, Equatable {
    public let source: GraphIdentity
    public let target: GraphIdentity
    public let edgeLabel: GraphIdentity
    public let graph: GraphIdentity?
    public let properties: [String: FieldValue]

    public init(
        source: GraphIdentity,
        target: GraphIdentity,
        edgeLabel: GraphIdentity,
        graph: GraphIdentity?,
        properties: [String: FieldValue]
    ) {
        self.source = source
        self.target = target
        self.edgeLabel = edgeLabel
        self.graph = graph
        self.properties = properties
    }

    public static func == (
        lhs: GraphEdgeWithProperties,
        rhs: GraphEdgeWithProperties
    ) -> Bool {
        lhs.source == rhs.source
            && lhs.target == rhs.target
            && lhs.edgeLabel == rhs.edgeLabel
            && lhs.graph == rhs.graph
            && lhs.properties == rhs.properties
    }
}

/// A covering-index property predicate evaluated after structural key pruning.
public struct PropertyFilter: Sendable {
    public let fieldName: String
    public let op: ComparisonOperator
    public let value: FieldValue

    public init(
        fieldName: String,
        op: ComparisonOperator,
        value: FieldValue
    ) {
        self.fieldName = fieldName
        self.op = op
        self.value = value
    }

    public func evaluate(on rawValue: FieldValue?) throws -> Bool {
        guard let rawValue else {
            throw GraphPropertyFilterError.fieldNotStored(fieldName)
        }
        switch op {
        case .isNil:
            return rawValue == .null
        case .isNotNil:
            return rawValue != .null
        default:
            break
        }
        let fieldValue = rawValue
        if fieldValue.isNull { return false }

        switch op {
        case .equal:
            return fieldValue.isEqual(to: value)
        case .notEqual:
            return !fieldValue.isEqual(to: value)
        case .lessThan:
            return fieldValue.isLessThan(value)
        case .lessThanOrEqual:
            return fieldValue.isLessThan(value)
                || fieldValue.isEqual(to: value)
        case .greaterThan:
            return value.isLessThan(fieldValue)
        case .greaterThanOrEqual:
            return value.isLessThan(fieldValue)
                || fieldValue.isEqual(to: value)
        case .contains:
            guard let string = fieldValue.stringValue,
                  let substring = value.stringValue else {
                throw GraphPropertyFilterError.operatorTypeMismatch(
                    field: fieldName,
                    operation: op
                )
            }
            return TextSearch.contains(substring, in: string)
        case .in, .notIn:
            guard case .array(let values) = value else {
                throw GraphPropertyFilterError.operatorTypeMismatch(
                    field: fieldName,
                    operation: op
                )
            }
            let contains = values.contains(fieldValue)
            return op == .in ? contains : !contains
        case .hasPrefix:
            guard let string = fieldValue.stringValue,
                  let prefix = value.stringValue else {
                throw GraphPropertyFilterError.operatorTypeMismatch(
                    field: fieldName,
                    operation: op
                )
            }
            return string.hasPrefix(prefix)
        case .hasSuffix:
            guard let string = fieldValue.stringValue,
                  let suffix = value.stringValue else {
                throw GraphPropertyFilterError.operatorTypeMismatch(
                    field: fieldName,
                    operation: op
                )
            }
            return string.hasSuffix(suffix)
        case .isNil, .isNotNil:
            return false
        }
    }
}

/// Adds covering-value decoding and property predicates to GraphEdgeScanner.
///
/// Structural key planning and decoding remain owned by GraphEdgeScanner, so
/// graph layout semantics cannot diverge between algorithm and property reads.
public struct GraphPropertyScanner: Sendable {
    private let indexSubspace: Subspace
    private let strategy: GraphIndexStrategy
    private let storedFieldNames: [String]
    private let snapshot: GraphReadSnapshot?

    public init(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        storedFieldNames: [String]
    ) {
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.storedFieldNames = storedFieldNames
        self.snapshot = nil
    }

    package init(
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        storedFieldNames: [String],
        snapshot: GraphReadSnapshot
    ) {
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.storedFieldNames = storedFieldNames
        self.snapshot = snapshot
    }

    /// Scans a structural pattern inside an explicit graph scope.
    public func scanEdges(
        from source: GraphIdentity?,
        edge edgeLabel: GraphIdentity?,
        to target: GraphIdentity?,
        scope: GraphScanScope = .all,
        propertyFilters: [PropertyFilter]?,
        transaction: any TransactionAccess
    ) -> GraphPropertySequence {
        let scanner: GraphEdgeScanner
        if let snapshot {
            scanner = GraphEdgeScanner(
                indexSubspace: indexSubspace,
                strategy: strategy,
                scope: scope,
                snapshot: snapshot
            )
        } else {
            scanner = GraphEdgeScanner(
                indexSubspace: indexSubspace,
                strategy: strategy,
                scope: scope
            )
        }
        return GraphPropertySequence(
            scanner: self,
            entries: scanner.scanEntries(
                source: source,
                target: target,
                edgeLabel: edgeLabel,
                transaction: transaction
            ),
            filters: propertyFilters
        )
    }

    package func decodeProperties(
        _ value: Bytes
    ) throws -> [String: FieldValue] {
        guard !storedFieldNames.isEmpty else { return [:] }
        return try CoveringValueBuilder.decode(
            value,
            storedFieldNames: storedFieldNames
        )
    }

    package func matches(
        _ properties: [String: FieldValue],
        filters: [PropertyFilter]?
    ) throws -> Bool {
        guard let filters, !filters.isEmpty else { return true }
        for filter in filters {
            if try !filter.evaluate(on: properties[filter.fieldName]) {
                return false
            }
        }
        return true
    }
}

public enum GraphPropertyFilterError: Error, Sendable, Equatable {
    case fieldNotStored(String)
    case unsupportedPropertyValue(field: String, value: FieldValue)
    case operatorTypeMismatch(field: String, operation: ComparisonOperator)
}

public struct GraphPropertySequence: AsyncSequence, Sendable {
    public typealias Element = GraphEdgeWithProperties

    private let scanner: GraphPropertyScanner
    private let entries: GraphEdgeEntrySequence
    private let filters: [PropertyFilter]?

    package init(
        scanner: GraphPropertyScanner,
        entries: GraphEdgeEntrySequence,
        filters: [PropertyFilter]?
    ) {
        self.scanner = scanner
        self.entries = entries
        self.filters = filters
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            scanner: scanner,
            iterator: entries.makeAsyncIterator(),
            filters: filters
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private let scanner: GraphPropertyScanner
        private var iterator: GraphEdgeEntrySequence.AsyncIterator
        private let filters: [PropertyFilter]?

        package init(
            scanner: GraphPropertyScanner,
            iterator: GraphEdgeEntrySequence.AsyncIterator,
            filters: [PropertyFilter]?
        ) {
            self.scanner = scanner
            self.iterator = iterator
            self.filters = filters
        }

        public mutating func next() async throws -> GraphEdgeWithProperties? {
            while let entry = try await iterator.next() {
                let properties = try scanner.decodeProperties(entry.value)
                guard try scanner.matches(properties, filters: filters) else {
                    continue
                }
                return GraphEdgeWithProperties(
                    source: entry.edge.source,
                    target: entry.edge.target,
                    edgeLabel: entry.edge.edgeLabel,
                    graph: entry.edge.graph,
                    properties: properties
                )
            }
            return nil
        }
    }
}
