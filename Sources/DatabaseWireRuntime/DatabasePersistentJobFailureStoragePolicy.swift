import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabasePersistentJobFailureStoragePolicy: Sendable {
    private static let storageBudgetFailureCode =
        "JOB_FAILURE_STORAGE_BUDGET_EXCEEDED"
    private static let storageBudgetFailureMessage =
        "The original job failure exceeded the persistent unsuccessful-outcome storage budget"
    private static let encodingFailureCode = "JOB_FAILURE_ENCODING_FAILED"
    private static let encodingFailureMessage =
        "The original job failure could not be encoded canonically"

    private let limits: DatabaseWireLimits
    private let maximumOriginalCodeBytes: Int

    init(
        storageLimits: DatabasePersistentJobStorageLimits,
        wireLimits: DatabaseWireLimits
    ) throws {
        let limits = try storageLimits.unsuccessfulOutcomeWireLimits(
            basedOn: wireLimits
        )
        let maximumOriginalCodeBytes = try Self.maximumStorableOriginalCodeBytes(
            limits: limits
        )
        self.limits = limits
        self.maximumOriginalCodeBytes = maximumOriginalCodeBytes
    }

    func storableFailure(
        for remoteError: RemoteOperationError
    ) throws(DatabaseWireError) -> RemoteOperationError {
        do {
            try Self.validateEncodedSize(
                of: .failed(remoteError),
                limits: limits
            )
            return remoteError
        } catch let wireError {
            let replacement: RemoteOperationError
            if wireError.exceedsPersistentOutcomeResourceLimits {
                replacement = try Self.storageBudgetFailure(
                    replacing: remoteError,
                    maximumOriginalCodeBytes: maximumOriginalCodeBytes
                )
            } else {
                replacement = try Self.encodingFailure(
                    replacing: remoteError,
                    maximumOriginalCodeBytes: maximumOriginalCodeBytes
                )
            }
            try Self.validateEncodedSize(
                of: .failed(replacement),
                limits: limits
            )
            return replacement
        }
    }

    private static func validateEncodedSize(
        of outcome: DatabaseJobUnsuccessfulOutcome,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) {
        do {
            _ = try PersistentJobPayloadStorage.encodedByteCount(
                outcome,
                limits: limits
            )
        } catch let error as DatabaseWireError {
            throw error
        } catch {
            throw .invalidResultPayload(0)
        }
    }

    private static func storageBudgetFailure(
        replacing error: RemoteOperationError,
        maximumOriginalCodeBytes: Int
    ) throws(DatabaseWireError) -> RemoteOperationError {
        RemoteOperationError(
            category: error.category,
            code: storageBudgetFailureCode,
            message: storageBudgetFailureMessage,
            retryability: error.retryability,
            details: try originalCodeDetails(
                for: error,
                maximumBytes: maximumOriginalCodeBytes
            )
        )
    }

    private static func encodingFailure(
        replacing error: RemoteOperationError,
        maximumOriginalCodeBytes: Int
    ) throws(DatabaseWireError) -> RemoteOperationError {
        RemoteOperationError(
            category: .internalFailure,
            code: encodingFailureCode,
            message: encodingFailureMessage,
            retryability: .never,
            details: try originalCodeDetails(
                for: error,
                maximumBytes: maximumOriginalCodeBytes
            )
        )
    }

    private static func originalCodeDetails(
        for error: RemoteOperationError,
        maximumBytes: Int
    ) throws(DatabaseWireError) -> FieldObject {
        do {
            return try FieldObject([
                (
                    key: "originalCode",
                    value: .string(stringPrefix(
                        error.code,
                        maximumBytes: maximumBytes
                    ))
                ),
            ])
        } catch {
            throw .invalidFieldObject(error)
        }
    }

    private static func maximumStorableOriginalCodeBytes(
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) -> Int {
        var lowerBound = 0
        var upperBound = min(1_024, limits.maximumStringBytes)
        while lowerBound < upperBound {
            let candidate = lowerBound + ((upperBound - lowerBound + 1) / 2)
            if fallbackOutcomesFit(
                originalCodeBytes: candidate,
                limits: limits
            ) {
                lowerBound = candidate
            } else {
                upperBound = candidate - 1
            }
        }
        try validateFallbackOutcomes(
            originalCodeBytes: lowerBound,
            limits: limits
        )
        return lowerBound
    }

    private static func fallbackOutcomesFit(
        originalCodeBytes: Int,
        limits: DatabaseWireLimits
    ) -> Bool {
        do {
            try validateFallbackOutcomes(
                originalCodeBytes: originalCodeBytes,
                limits: limits
            )
            return true
        } catch {
            return false
        }
    }

    private static func validateFallbackOutcomes(
        originalCodeBytes: Int,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) {
        let validationFailure = RemoteOperationError(
            category: .internalFailure,
            code: String(repeating: "X", count: originalCodeBytes),
            message: "Server failure",
            retryability: .never
        )
        try validateEncodedSize(
            of: .failed(try storageBudgetFailure(
                replacing: validationFailure,
                maximumOriginalCodeBytes: originalCodeBytes
            )),
            limits: limits
        )
        try validateEncodedSize(
            of: .failed(try encodingFailure(
                replacing: validationFailure,
                maximumOriginalCodeBytes: originalCodeBytes
            )),
            limits: limits
        )
    }

    private static func stringPrefix(
        _ string: String,
        maximumBytes: Int
    ) -> String {
        guard maximumBytes > 0 else { return "" }
        guard string.utf8.count > maximumBytes else { return string }

        // Persistent state owns this reduced diagnostic. Iterating scalars
        // avoids an intermediate UTF-8 array and never splits a scalar.
        var result = ""
        result.reserveCapacity(maximumBytes)
        var byteCount = 0
        for scalar in string.unicodeScalars {
            let addition = byteCount.addingReportingOverflow(scalar.utf8.count)
            guard !addition.overflow,
                  addition.partialValue <= maximumBytes else {
                break
            }
            result.unicodeScalars.append(scalar)
            byteCount = addition.partialValue
        }
        return result
    }
}

private extension DatabaseWireError {
    var exceedsPersistentOutcomeResourceLimits: Bool {
        switch self {
        case .byteCountOverflow,
             .frameTooLarge,
             .stringTooLarge,
             .byteStringTooLarge,
             .collectionTooLarge,
             .nestingTooDeep,
             .objectBudgetExceeded:
            return true
        default:
            return false
        }
    }
}
