import DatabaseKit

/// Immutable inspection of decisions made by the ordinary execution owner.
struct SQLExplainPlanNode: Sendable {
    let operation: String
    let details: [SQLExplainPlanDetail]
    let outputs: [QueryColumn]
    let children: [SQLExplainPlanNode]
    let retainedPlan: CanonicalBoundSelect?
    let storageReservation: DatabaseIntermediateReservation?

    init(operation: String, details: [SQLExplainPlanDetail] = [],
         outputs: [QueryColumn] = [], children: [SQLExplainPlanNode] = [],
         retainedPlan: CanonicalBoundSelect? = nil, storageReservation: DatabaseIntermediateReservation? = nil) {
        self.operation = operation
        self.details = details
        self.outputs = outputs
        self.children = children
        self.retainedPlan = retainedPlan
        self.storageReservation = storageReservation
    }
}
