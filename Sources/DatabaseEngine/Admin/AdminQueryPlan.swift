public struct AdminQueryPlan: Sendable, Equatable {
    public let kind: AdminQueryPlanKind
    public let selectedIndexName: String?
    public let indexConditions: [String]
    public let filterConditions: [String]
    public let requiresSort: Bool

    public init(
        kind: AdminQueryPlanKind,
        selectedIndexName: String?,
        indexConditions: [String],
        filterConditions: [String],
        requiresSort: Bool
    ) {
        self.kind = kind
        self.selectedIndexName = selectedIndexName
        self.indexConditions = indexConditions
        self.filterConditions = filterConditions
        self.requiresSort = requiresSort
    }
}
