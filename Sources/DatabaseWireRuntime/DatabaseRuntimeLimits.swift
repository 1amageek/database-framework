@_spi(DatabaseWireRuntime) import DatabaseWire
import DatabaseKit

public struct DatabaseRuntimeLimits: Sendable, Hashable {
    public let maximumRows: UInt32
    public let maximumWorkUnits: UInt64
    public let maximumIntermediateRows: UInt32
    public let maximumIntermediateBytes: UInt64
    public let maximumTimeoutMilliseconds: UInt32
    public let maximumMutations: Int
    public let maximumPreconditions: Int
    public let maximumIdempotencyKeyBytes: Int
    public let maximumMutationAggregateBytes: Int
    public let queryStructuralLimits: QueryStructuralLimits

    public init(
        maximumRows: UInt32,
        maximumWorkUnits: UInt64,
        maximumIntermediateRows: UInt32 = 10_000,
        maximumIntermediateBytes: UInt64 = 16 * 1_024 * 1_024,
        maximumTimeoutMilliseconds: UInt32,
        maximumMutations: Int = 1_000,
        maximumPreconditions: Int = 1_000,
        maximumIdempotencyKeyBytes: Int = 512,
        maximumMutationAggregateBytes: Int = 8 * 1_024 * 1_024,
        queryStructuralLimits: QueryStructuralLimits = .default
    ) throws {
        try Self.validatePositive(
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
        self.maximumRows = maximumRows
        self.maximumWorkUnits = maximumWorkUnits
        self.maximumIntermediateRows = maximumIntermediateRows
        self.maximumIntermediateBytes = maximumIntermediateBytes
        self.maximumTimeoutMilliseconds = maximumTimeoutMilliseconds
        self.maximumMutations = maximumMutations
        self.maximumPreconditions = maximumPreconditions
        self.maximumIdempotencyKeyBytes = maximumIdempotencyKeyBytes
        self.maximumMutationAggregateBytes = maximumMutationAggregateBytes
        self.queryStructuralLimits = queryStructuralLimits
    }

    public static let `default` = DatabaseRuntimeLimits(
        validatedMaximumRows: 10_000,
        maximumWorkUnits: 1_000_000,
        maximumIntermediateRows: 10_000,
        maximumIntermediateBytes: 16 * 1_024 * 1_024,
        maximumTimeoutMilliseconds: 30_000,
        maximumMutations: 1_000,
        maximumPreconditions: 1_000,
        maximumIdempotencyKeyBytes: 512,
        maximumMutationAggregateBytes: 8 * 1_024 * 1_024,
        queryStructuralLimits: .default
    )

    public func validateConfiguration() throws(DatabaseRuntimeConfigurationError) {
        try Self.validatePositive(
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

    public func validate(_ budget: ExecutionBudget) throws {
        guard budget.maximumRows > 0, budget.maximumRows <= maximumRows else {
            throw DatabaseRuntimeLimitError.invalidMaximumRows(
                requested: budget.maximumRows,
                maximum: maximumRows
            )
        }
        guard budget.maximumWorkUnits > 0,
              budget.maximumWorkUnits <= maximumWorkUnits else {
            throw DatabaseRuntimeLimitError.invalidMaximumWorkUnits(
                requested: budget.maximumWorkUnits,
                maximum: maximumWorkUnits
            )
        }
        guard budget.maximumIntermediateRows > 0,
              budget.maximumIntermediateRows <= maximumIntermediateRows else {
            throw DatabaseRuntimeLimitError.invalidMaximumIntermediateRows(
                requested: budget.maximumIntermediateRows,
                maximum: maximumIntermediateRows
            )
        }
        guard budget.maximumIntermediateBytes > 0,
              budget.maximumIntermediateBytes <= maximumIntermediateBytes else {
            throw DatabaseRuntimeLimitError.invalidMaximumIntermediateBytes(
                requested: budget.maximumIntermediateBytes,
                maximum: maximumIntermediateBytes
            )
        }
        guard budget.timeoutMilliseconds > 0,
              budget.timeoutMilliseconds <= maximumTimeoutMilliseconds else {
            throw DatabaseRuntimeLimitError.invalidTimeout(
                requested: budget.timeoutMilliseconds,
                maximum: maximumTimeoutMilliseconds
            )
        }
    }

    private init(
        validatedMaximumRows maximumRows: UInt32,
        maximumWorkUnits: UInt64,
        maximumIntermediateRows: UInt32,
        maximumIntermediateBytes: UInt64,
        maximumTimeoutMilliseconds: UInt32,
        maximumMutations: Int,
        maximumPreconditions: Int,
        maximumIdempotencyKeyBytes: Int,
        maximumMutationAggregateBytes: Int,
        queryStructuralLimits: QueryStructuralLimits
    ) {
        self.maximumRows = maximumRows
        self.maximumWorkUnits = maximumWorkUnits
        self.maximumIntermediateRows = maximumIntermediateRows
        self.maximumIntermediateBytes = maximumIntermediateBytes
        self.maximumTimeoutMilliseconds = maximumTimeoutMilliseconds
        self.maximumMutations = maximumMutations
        self.maximumPreconditions = maximumPreconditions
        self.maximumIdempotencyKeyBytes = maximumIdempotencyKeyBytes
        self.maximumMutationAggregateBytes = maximumMutationAggregateBytes
        self.queryStructuralLimits = queryStructuralLimits
    }

    private static func validatePositive(
        maximumRows: UInt32,
        maximumWorkUnits: UInt64,
        maximumIntermediateRows: UInt32,
        maximumIntermediateBytes: UInt64,
        maximumTimeoutMilliseconds: UInt32,
        maximumMutations: Int,
        maximumPreconditions: Int,
        maximumIdempotencyKeyBytes: Int,
        maximumMutationAggregateBytes: Int,
        queryStructuralLimits: QueryStructuralLimits
    ) throws(DatabaseRuntimeConfigurationError) {
        let unsignedLimits: [
            (DatabaseRuntimeConfigurationError.Limit, UInt64)
        ] = [
            (.maximumRows, UInt64(maximumRows)),
            (.maximumWorkUnits, maximumWorkUnits),
            (.maximumIntermediateRows, UInt64(maximumIntermediateRows)),
            (.maximumIntermediateBytes, maximumIntermediateBytes),
            (.maximumTimeoutMilliseconds, UInt64(maximumTimeoutMilliseconds)),
            (.maximumNestingDepth, queryStructuralLimits.maximumNestingDepth),
            (.maximumInputTokens, queryStructuralLimits.maximumInputTokens),
            (.maximumTotalNodes, queryStructuralLimits.maximumTotalNodes),
            (
                .maximumCollectionElements,
                queryStructuralLimits.maximumCollectionElements
            ),
            (
                .maximumBasicGraphPatterns,
                queryStructuralLimits.maximumBasicGraphPatterns
            ),
            (.maximumTriplePatterns, queryStructuralLimits.maximumTriplePatterns),
            (.maximumValuesRows, queryStructuralLimits.maximumValuesRows),
            (
                .maximumValuesVariables,
                queryStructuralLimits.maximumValuesVariables
            ),
            (.maximumValuesCells, queryStructuralLimits.maximumValuesCells),
            (
                .maximumReifiedTripleExpansions,
                queryStructuralLimits.maximumReifiedTripleExpansions
            ),
        ]
        for (limit, value) in unsignedLimits where value == 0 {
            throw DatabaseRuntimeConfigurationError.nonPositiveLimit(limit)
        }

        let integerLimits: [
            (DatabaseRuntimeConfigurationError.Limit, Int)
        ] = [
            (.maximumMutations, maximumMutations),
            (.maximumPreconditions, maximumPreconditions),
            (.maximumIdempotencyKeyBytes, maximumIdempotencyKeyBytes),
            (.maximumMutationAggregateBytes, maximumMutationAggregateBytes),
        ]
        for (limit, value) in integerLimits where value <= 0 {
            throw DatabaseRuntimeConfigurationError.nonPositiveLimit(limit)
        }

        let maximumPlatformRows = Int.max - 1
        guard let platformRows = Int(exactly: maximumRows),
              platformRows <= maximumPlatformRows else {
            throw DatabaseRuntimeConfigurationError.unsupportedOnCurrentPlatform(
                limit: .maximumRows,
                actual: UInt64(maximumRows),
                maximum: UInt64(maximumPlatformRows)
            )
        }

        guard Int(exactly: maximumTimeoutMilliseconds) != nil else {
            throw DatabaseRuntimeConfigurationError.unsupportedOnCurrentPlatform(
                limit: .maximumTimeoutMilliseconds,
                actual: UInt64(maximumTimeoutMilliseconds),
                maximum: UInt64(Int.max)
            )
        }
    }
}
