/// Query-form-independent SPARQL solution pipeline through ORDER BY.
///
/// The executable dataset is retained with the algebra so a compiled plan
/// cannot be evaluated against a silently different dataset scope.
public struct SPARQLOrderedSolutionPlan: Sendable, Equatable {
    public let datasetScope: SPARQLDatasetExecutionScope
    public let algebra: ExecutionPattern
    public let orderKeys: [SPARQLOrderKeyPlan]
    public let visibleVariables: [String]

    package init(
        datasetScope: SPARQLDatasetExecutionScope,
        algebra: consuming ExecutionPattern,
        orderKeys: consuming [SPARQLOrderKeyPlan],
        visibleVariables: consuming [String]
    ) {
        self.datasetScope = datasetScope
        self.algebra = consume algebra
        self.orderKeys = consume orderKeys
        self.visibleVariables = consume visibleVariables
    }
}
