/// Shared transition budget for one regex match or global replacement call.
struct SPARQLRegexWorkBudget {
    let limit: Int
    private(set) var consumed = 0

    mutating func consume(_ amount: Int) throws {
        let (actual, overflow) = consumed.addingReportingOverflow(amount)
        guard amount >= 0, !overflow, actual <= limit else {
            throw SPARQLRegularExpression.Error.resourceLimit(
                name: "activeTransitionWork",
                limit: limit,
                actual: overflow || amount < 0 ? Int.max : actual
            )
        }
        consumed = actual
    }
}
