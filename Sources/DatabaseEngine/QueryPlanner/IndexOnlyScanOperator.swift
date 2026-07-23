import Core

/// Plan operator that reads complete records from canonical DBIX projections.
public struct IndexOnlyScanOperator<T: Persistable>: Sendable {
    public let index: IndexDescriptor
    public let metadata: CoveringIndexMetadata
    public let bounds: IndexScanBounds
    public let reverse: Bool
    public let projectedFields: Set<String>
    public let satisfiedConditions: [any FieldConditionProtocol<T>]
    public let estimatedEntries: Int
    public let limit: Int?

    public init(
        index: IndexDescriptor,
        metadata: CoveringIndexMetadata,
        bounds: IndexScanBounds,
        reverse: Bool = false,
        projectedFields: Set<String>,
        satisfiedConditions: [any FieldConditionProtocol<T>] = [],
        estimatedEntries: Int,
        limit: Int? = nil
    ) {
        self.index = index
        self.metadata = metadata
        self.bounds = bounds
        self.reverse = reverse
        self.projectedFields = projectedFields
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
        self.limit = limit
    }
}
