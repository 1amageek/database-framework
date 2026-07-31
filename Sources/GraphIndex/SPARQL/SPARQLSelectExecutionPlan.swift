public struct SPARQLSelectExecutionPlan: Sendable {
    public let ordered: SPARQLOrderedSolutionPlan
    public let projectionVariables: [String]
    public let projectionIsIdentity: Bool
    public let duplicatePolicy: SPARQLDuplicatePolicy
    public let slice: SPARQLSlice

    package init(
        ordered: consuming SPARQLOrderedSolutionPlan,
        projectionVariables: consuming [String],
        projectionIsIdentity: Bool,
        duplicatePolicy: SPARQLDuplicatePolicy,
        slice: SPARQLSlice
    ) {
        self.ordered = consume ordered
        self.projectionVariables = consume projectionVariables
        self.projectionIsIdentity = projectionIsIdentity
        self.duplicatePolicy = duplicatePolicy
        self.slice = slice
    }
}
