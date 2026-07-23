/// The storage access and residual work that query execution will perform.
///
/// This plan deliberately contains no estimated cost. Cost and row estimates
/// belong only in statistics-backed administrative output.
public struct QueryAccessPlan: Sendable, Equatable, Hashable {
    public let accessPath: QueryAccessPath
    public let indexedConditions: [QueryAccessCondition]
    public let residualFilterRequired: Bool
    public let sortRequired: Bool
    public let fetchLimit: Int?
    public let fetchOffset: Int?

    public init(
        accessPath: QueryAccessPath,
        indexedConditions: [QueryAccessCondition],
        residualFilterRequired: Bool,
        sortRequired: Bool,
        fetchLimit: Int?,
        fetchOffset: Int?
    ) {
        self.accessPath = accessPath
        self.indexedConditions = indexedConditions
        self.residualFilterRequired = residualFilterRequired
        self.sortRequired = sortRequired
        self.fetchLimit = fetchLimit
        self.fetchOffset = fetchOffset
    }
}
