import DatabaseServer
import DatabaseWire
import DatabaseKit
import Testing

@Suite("Database runtime execution limits")
struct DatabaseRuntimeLimitsTests {
    @Test("Intermediate budgets accept their exact server caps")
    func acceptsIntermediateCaps() throws {
        let limits = try makeLimits()
        try limits.validate(
            ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 20,
                maximumIntermediateBytes: 2_048,
                timeoutMilliseconds: 500
            )
        )
    }

    @Test("Intermediate row budget rejects zero and cap plus one")
    func rejectsInvalidIntermediateRows() throws {
        let limits = try makeLimits()
        for requested: UInt32 in [0, 21] {
            #expect(
                throws: DatabaseRuntimeLimitError
                    .invalidMaximumIntermediateRows(
                        requested: requested,
                        maximum: 20
                    )
            ) {
                try limits.validate(budget(intermediateRows: requested))
            }
        }
    }

    @Test("Intermediate byte budget rejects zero and cap plus one")
    func rejectsInvalidIntermediateBytes() throws {
        let limits = try makeLimits()
        for requested: UInt64 in [0, 2_049] {
            #expect(
                throws: DatabaseRuntimeLimitError
                    .invalidMaximumIntermediateBytes(
                        requested: requested,
                        maximum: 2_048
                    )
            ) {
                try limits.validate(budget(intermediateBytes: requested))
            }
        }
    }

    @Test("Default and minimum positive configurations are valid")
    func acceptsValidConfigurations() throws {
        try DatabaseRuntimeLimits.default.validateConfiguration()
        let structuralLimits = QueryStructuralLimits(
            maximumNestingDepth: 1,
            maximumInputTokens: 1,
            maximumTotalNodes: 1,
            maximumCollectionElements: 1,
            maximumBasicGraphPatterns: 1,
            maximumTriplePatterns: 1,
            maximumValuesRows: 1,
            maximumValuesVariables: 1,
            maximumValuesCells: 1,
            maximumReifiedTripleExpansions: 1
        )
        let limits = try makeLimits(
            maximumRows: 1,
            maximumWorkUnits: 1,
            maximumIntermediateRows: 1,
            maximumIntermediateBytes: 1,
            maximumTimeoutMilliseconds: 1,
            maximumMutations: 1,
            maximumPreconditions: 1,
            maximumIdempotencyKeyBytes: 1,
            maximumMutationAggregateBytes: 1,
            queryStructuralLimits: structuralLimits
        )
        try limits.validateConfiguration()
    }

    @Test("Every top-level configuration limit rejects zero")
    func rejectsZeroTopLevelLimits() {
        #expect(throws: configurationError(.maximumRows)) {
            _ = try makeLimits(maximumRows: 0)
        }
        #expect(throws: configurationError(.maximumWorkUnits)) {
            _ = try makeLimits(maximumWorkUnits: 0)
        }
        #expect(throws: configurationError(.maximumIntermediateRows)) {
            _ = try makeLimits(maximumIntermediateRows: 0)
        }
        #expect(throws: configurationError(.maximumIntermediateBytes)) {
            _ = try makeLimits(maximumIntermediateBytes: 0)
        }
        #expect(throws: configurationError(.maximumTimeoutMilliseconds)) {
            _ = try makeLimits(maximumTimeoutMilliseconds: 0)
        }
        #expect(throws: configurationError(.maximumMutations)) {
            _ = try makeLimits(maximumMutations: 0)
        }
        #expect(throws: configurationError(.maximumPreconditions)) {
            _ = try makeLimits(maximumPreconditions: 0)
        }
        #expect(throws: configurationError(.maximumIdempotencyKeyBytes)) {
            _ = try makeLimits(maximumIdempotencyKeyBytes: 0)
        }
        #expect(throws: configurationError(.maximumMutationAggregateBytes)) {
            _ = try makeLimits(maximumMutationAggregateBytes: 0)
        }
    }

    @Test("Signed configuration limits reject negative values")
    func rejectsNegativeSignedLimits() {
        #expect(throws: configurationError(.maximumMutations)) {
            _ = try makeLimits(maximumMutations: -1)
        }
        #expect(throws: configurationError(.maximumPreconditions)) {
            _ = try makeLimits(maximumPreconditions: -1)
        }
        #expect(throws: configurationError(.maximumIdempotencyKeyBytes)) {
            _ = try makeLimits(maximumIdempotencyKeyBytes: -1)
        }
        #expect(throws: configurationError(.maximumMutationAggregateBytes)) {
            _ = try makeLimits(maximumMutationAggregateBytes: -1)
        }
    }

    @Test("Every structural configuration limit rejects zero")
    func rejectsZeroStructuralLimits() {
        let invalidLimits: [
            (DatabaseRuntimeConfigurationError.Limit, QueryStructuralLimits)
        ] = [
            (
                .maximumNestingDepth,
                QueryStructuralLimits(maximumNestingDepth: 0)
            ),
            (
                .maximumInputTokens,
                QueryStructuralLimits(maximumInputTokens: 0)
            ),
            (
                .maximumTotalNodes,
                QueryStructuralLimits(maximumTotalNodes: 0)
            ),
            (
                .maximumCollectionElements,
                QueryStructuralLimits(maximumCollectionElements: 0)
            ),
            (
                .maximumBasicGraphPatterns,
                QueryStructuralLimits(maximumBasicGraphPatterns: 0)
            ),
            (
                .maximumTriplePatterns,
                QueryStructuralLimits(maximumTriplePatterns: 0)
            ),
            (
                .maximumValuesRows,
                QueryStructuralLimits(maximumValuesRows: 0)
            ),
            (
                .maximumValuesVariables,
                QueryStructuralLimits(maximumValuesVariables: 0)
            ),
            (
                .maximumValuesCells,
                QueryStructuralLimits(maximumValuesCells: 0)
            ),
            (
                .maximumReifiedTripleExpansions,
                QueryStructuralLimits(maximumReifiedTripleExpansions: 0)
            ),
        ]

        for (limit, structuralLimits) in invalidLimits {
            #expect(throws: configurationError(limit)) {
                _ = try makeLimits(queryStructuralLimits: structuralLimits)
            }
        }
    }

    @Test("Row limits reserve a platform-representable overflow sentinel")
    func rejectsPlatformUnsupportedRows() {
        guard let platformMaximumRows = UInt32(exactly: Int.max) else {
            return
        }
        #expect(
            throws: DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumRows,
                    actual: UInt64(platformMaximumRows),
                    maximum: UInt64(Int.max - 1)
                )
        ) {
            _ = try makeLimits(maximumRows: platformMaximumRows)
        }
    }

    @Test("Timeout limits must fit the backend transaction option")
    func rejectsPlatformUnsupportedTimeout() {
        guard let unsupportedTimeout = UInt32(exactly: UInt64(Int.max) + 1) else {
            return
        }
        #expect(
            throws: DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumTimeoutMilliseconds,
                    actual: UInt64(unsupportedTimeout),
                    maximum: UInt64(Int.max)
                )
        ) {
            _ = try makeLimits(
                maximumTimeoutMilliseconds: unsupportedTimeout
            )
        }
    }

    @Test("Request budgets validate rows, work, and timeout")
    func rejectsInvalidPrimaryRequestBudgets() throws {
        let limits = try makeLimits()
        for requested: UInt32 in [0, 101] {
            #expect(
                throws: DatabaseRuntimeLimitError.invalidMaximumRows(
                    requested: requested,
                    maximum: 100
                )
            ) {
                try limits.validate(budget(maximumRows: requested))
            }
        }
        for requested: UInt64 in [0, 1_001] {
            #expect(
                throws: DatabaseRuntimeLimitError.invalidMaximumWorkUnits(
                    requested: requested,
                    maximum: 1_000
                )
            ) {
                try limits.validate(budget(maximumWorkUnits: requested))
            }
        }
        for requested: UInt32 in [0, 501] {
            #expect(
                throws: DatabaseRuntimeLimitError.invalidTimeout(
                    requested: requested,
                    maximum: 500
                )
            ) {
                try limits.validate(budget(timeoutMilliseconds: requested))
            }
        }
    }

    private func makeLimits(
        maximumRows: UInt32 = 100,
        maximumWorkUnits: UInt64 = 1_000,
        maximumIntermediateRows: UInt32 = 20,
        maximumIntermediateBytes: UInt64 = 2_048,
        maximumTimeoutMilliseconds: UInt32 = 500,
        maximumMutations: Int = 1_000,
        maximumPreconditions: Int = 1_000,
        maximumIdempotencyKeyBytes: Int = 512,
        maximumMutationAggregateBytes: Int = 8 * 1_024 * 1_024,
        queryStructuralLimits: QueryStructuralLimits = .default
    ) throws -> DatabaseRuntimeLimits {
        try DatabaseRuntimeLimits(
            maximumRows: maximumRows,
            maximumWorkUnits: maximumWorkUnits,
            maximumIntermediateRows: maximumIntermediateRows,
            maximumIntermediateBytes: maximumIntermediateBytes,
            maximumTimeoutMilliseconds: maximumTimeoutMilliseconds,
            maximumMutations: maximumMutations,
            maximumPreconditions: maximumPreconditions,
            maximumIdempotencyKeyBytes: maximumIdempotencyKeyBytes,
            maximumMutationAggregateBytes: maximumMutationAggregateBytes,
            queryStructuralLimits: queryStructuralLimits
        )
    }

    private func configurationError(
        _ limit: DatabaseRuntimeConfigurationError.Limit
    ) -> DatabaseRuntimeConfigurationError {
        .nonPositiveLimit(limit)
    }

    private func budget(
        maximumRows: UInt32 = 100,
        maximumWorkUnits: UInt64 = 1_000,
        intermediateRows: UInt32 = 20,
        intermediateBytes: UInt64 = 2_048,
        timeoutMilliseconds: UInt32 = 500
    ) -> ExecutionBudget {
        ExecutionBudget(
            maximumRows: maximumRows,
            maximumWorkUnits: maximumWorkUnits,
            maximumIntermediateRows: intermediateRows,
            maximumIntermediateBytes: intermediateBytes,
            timeoutMilliseconds: timeoutMilliseconds
        )
    }
}
