import DatabaseKit

/// Checked operator expressions retained across a SELECT's row iteration.
final class CanonicalBoundSelect: Sendable {
    let columns: [QueryColumn]
    let projection: [CanonicalBoundExpression]
    let filter: CanonicalBoundExpression?
    let groupBy: [CanonicalBoundExpression]
    let having: CanonicalBoundExpression?
    let orderBy: [CanonicalBoundExpression]
    let sourcePlan: CanonicalSourceBindingPlan?

    init(columns: [QueryColumn], projection: [CanonicalBoundExpression],
         filter: CanonicalBoundExpression?, groupBy: [CanonicalBoundExpression],
         having: CanonicalBoundExpression?, orderBy: [CanonicalBoundExpression],
         sourcePlan: CanonicalSourceBindingPlan? = nil) {
        self.columns = columns
        self.projection = projection
        self.filter = filter
        self.groupBy = groupBy
        self.having = having
        self.orderBy = orderBy
        self.sourcePlan = sourcePlan
    }

    func withSourcePlan(_ sourcePlan: CanonicalSourceBindingPlan) -> CanonicalBoundSelect {
        CanonicalBoundSelect(columns: columns, projection: projection, filter: filter,
                             groupBy: groupBy, having: having, orderBy: orderBy,
                             sourcePlan: sourcePlan)
    }
}
