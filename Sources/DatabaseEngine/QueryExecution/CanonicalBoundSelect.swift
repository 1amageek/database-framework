import DatabaseKit

/// Checked operator expressions retained across a SELECT's row iteration.
final class CanonicalBoundSelect: Sendable {
    let columns: [QueryColumn]
    let projection: [CanonicalBoundExpression]
    let filter: CanonicalBoundExpression?
    let groupBy: [CanonicalBoundExpression]
    let grouping: CanonicalBoundGrouping?
    let windows: [CanonicalBoundWindowDefinition]
    let having: CanonicalBoundExpression?
    let orderBy: [CanonicalBoundKey]
    let distinctOn: [CanonicalBoundKey]
    let pagination: CanonicalBoundPagination
    let ctes: [CanonicalBoundCTE]
    let bindingMetadata: CanonicalBindingMetadata?
    let metadataLease: DatabaseQueryPayloadLease?
    let sourcePlan: CanonicalSourceBindingPlan?

    init(columns: [QueryColumn], projection: [CanonicalBoundExpression],
         filter: CanonicalBoundExpression?, groupBy: [CanonicalBoundExpression],
         having: CanonicalBoundExpression?, orderBy: [CanonicalBoundKey],
         sourcePlan: CanonicalSourceBindingPlan? = nil,
         distinctOn: [CanonicalBoundKey] = [],
         pagination: CanonicalBoundPagination = CanonicalBoundPagination(),
         ctes: [CanonicalBoundCTE] = [], metadataLease: DatabaseQueryPayloadLease? = nil,
         bindingMetadata: CanonicalBindingMetadata? = nil,
         grouping: CanonicalBoundGrouping? = nil, windows: [CanonicalBoundWindowDefinition] = []) {
        self.metadataLease = metadataLease
        self.bindingMetadata = bindingMetadata
        self.columns = columns
        self.projection = projection
        self.filter = filter
        self.groupBy = groupBy
        self.grouping = grouping
        self.windows = windows
        self.having = having
        self.orderBy = orderBy
        self.distinctOn = distinctOn
        self.pagination = pagination
        self.ctes = ctes
        self.sourcePlan = sourcePlan
    }

    func withSourcePlan(_ sourcePlan: CanonicalSourceBindingPlan) -> CanonicalBoundSelect {
        CanonicalBoundSelect(columns: columns, projection: projection, filter: filter,
                             groupBy: groupBy, having: having, orderBy: orderBy,
                             sourcePlan: sourcePlan, distinctOn: distinctOn,
                             pagination: pagination, ctes: ctes, metadataLease: metadataLease, bindingMetadata: bindingMetadata,
                             grouping: grouping, windows: windows)
    }
}
