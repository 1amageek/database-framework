/// Selected count expressions are evaluated offset then limit once per invocation.
struct CanonicalBoundPagination: Sendable {
    let offset: CanonicalBoundCount?
    let limit: CanonicalBoundCount?
    let withTies: Bool
    let rejectsUnknownNullLimit: Bool

    init(offset: CanonicalBoundCount? = nil, limit: CanonicalBoundCount? = nil,
         withTies: Bool = false, rejectsUnknownNullLimit: Bool = false) {
        self.offset = offset
        self.limit = limit
        self.withTies = withTies
        self.rejectsUnknownNullLimit = rejectsUnknownNullLimit
    }
}
