import DatabaseKit
import DatabaseTypes

/// Immutable cross-entity property-graph input for a canonical Fusion plan.
public struct Connected<
    Item: Persistable,
    Edge: Persistable
>: FusionQueryInput, Sendable {
    public typealias Direction = FusionConnectedDirection

    private let resultField: FieldIdentity
    private let edgeEntity: String
    private let edgePartitions: FieldObject
    private let indexName: String
    private let origin: String
    private var edgeLabel: String?
    private var traversalDirection: Direction
    private var maximumHops: UInt64 = 1
    private var resultLimit: UInt64?

    public init(
        _ resultField: Field<Item, String>,
        from origin: String,
        through edge: Edge.Type,
        indexNamed indexName: String,
        partitions: FieldObject = FieldObject()
    ) {
        self.resultField = resultField.identity
        self.edgeEntity = Edge.persistableType
        self.edgePartitions = partitions
        self.indexName = indexName
        self.origin = origin
        self.traversalDirection = .outgoing
    }

    public init(
        _ resultField: Field<Item, String?>,
        from origin: String,
        through edge: Edge.Type,
        indexNamed indexName: String,
        partitions: FieldObject = FieldObject()
    ) {
        self.resultField = resultField.identity
        self.edgeEntity = Edge.persistableType
        self.edgePartitions = partitions
        self.indexName = indexName
        self.origin = origin
        self.traversalDirection = .outgoing
    }

    public init(
        _ resultField: Field<Item, String>,
        to origin: String,
        through edge: Edge.Type,
        indexNamed indexName: String,
        partitions: FieldObject = FieldObject()
    ) {
        self.resultField = resultField.identity
        self.edgeEntity = Edge.persistableType
        self.edgePartitions = partitions
        self.indexName = indexName
        self.origin = origin
        self.traversalDirection = .incoming
    }

    public init(
        _ resultField: Field<Item, String?>,
        to origin: String,
        through edge: Edge.Type,
        indexNamed indexName: String,
        partitions: FieldObject = FieldObject()
    ) {
        self.resultField = resultField.identity
        self.edgeEntity = Edge.persistableType
        self.edgePartitions = partitions
        self.indexName = indexName
        self.origin = origin
        self.traversalDirection = .incoming
    }

    public func via(_ edgeLabel: String) -> Self {
        var copy = self
        copy.edgeLabel = edgeLabel
        return copy
    }

    public func hops(_ count: UInt64) -> Self {
        var copy = self
        copy.maximumHops = count
        return copy
    }

    public func direction(_ direction: Direction) -> Self {
        var copy = self
        copy.traversalDirection = direction
        return copy
    }

    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.resultLimit = count
        return copy
    }

    public var fusionInput: FusionInput {
        FusionInput(
            operation: .connected(
                FusionConnectedSource(
                    edgeEntity: edgeEntity,
                    edgePartitions: edgePartitions,
                    selection: .named(
                        name: indexName,
                        type: .graph(.property)
                    ),
                    resultField: resultField,
                    origin: origin,
                    edgeLabel: edgeLabel,
                    direction: traversalDirection,
                    maximumHops: maximumHops
                )
            ),
            scoring: .annotation(name: "hops", order: .lowerIsBetter),
            limit: resultLimit
        )
    }
}
