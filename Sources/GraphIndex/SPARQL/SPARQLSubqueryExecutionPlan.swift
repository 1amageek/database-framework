public enum SPARQLSubqueryInputPolicy: Sendable, Hashable {
    case isolated
    case lateral
}

public struct SPARQLSubqueryExecutionPlan: Sendable {
    public let occurrenceIdentifier: UInt64
    public let select: SPARQLSelectExecutionPlan
    public let inputPolicy: SPARQLSubqueryInputPolicy

    package init(
        occurrenceIdentifier: UInt64,
        select: consuming SPARQLSelectExecutionPlan,
        inputPolicy: SPARQLSubqueryInputPolicy
    ) {
        self.occurrenceIdentifier = occurrenceIdentifier
        self.select = consume select
        self.inputPolicy = inputPolicy
    }
}
