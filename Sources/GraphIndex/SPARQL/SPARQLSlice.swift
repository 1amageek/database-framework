public struct SPARQLSlice: Sendable, Hashable {
    public let offset: Int
    public let limit: Int?

    package init(
        offset: UInt64 = 0,
        limit: UInt64? = nil
    ) throws {
        guard let executableOffset = Int(exactly: offset) else {
            throw SPARQLSelectPlanCompilationError
                .solutionModifierExceedsExecutionRange(
                    name: "OFFSET",
                    value: offset
                )
        }
        let executableLimit: Int?
        if let limit {
            guard let converted = Int(exactly: limit) else {
                throw SPARQLSelectPlanCompilationError
                    .solutionModifierExceedsExecutionRange(
                        name: "LIMIT",
                        value: limit
                    )
            }
            executableLimit = converted
        } else {
            executableLimit = nil
        }
        self.offset = executableOffset
        self.limit = executableLimit
    }
}
