import DatabaseEngine
import DatabaseOperations
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

/// Adapts one canonical DatabaseWire frame to host-independent operation
/// execution. This type owns framing only; target binding, authorization,
/// transactions, and operation semantics remain in `DatabaseOperations`.
public final class DatabaseWireEndpoint: Sendable {
    private let dispatcher: DatabaseOperationDispatcher
    private let instance: DatabaseOperationInstance?
    private let requestLimits: DatabaseWireLimits
    private let responseLimits: DatabaseWireLimits
    private let errorMapper: AnyDatabaseErrorMapper

    public init(
        instance: DatabaseOperationInstance,
        requestLimits: DatabaseWireLimits = .default,
        responseLimits: DatabaseWireLimits = .default,
        errorMapper: AnyDatabaseErrorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    ) {
        self.dispatcher = instance.dispatcher
        self.instance = instance
        self.requestLimits = requestLimits
        self.responseLimits = responseLimits
        self.errorMapper = errorMapper
    }

    package init(
        dispatcher: DatabaseOperationDispatcher,
        requestLimits: DatabaseWireLimits = .default,
        responseLimits: DatabaseWireLimits = .default
    ) {
        self.dispatcher = dispatcher
        self.instance = nil
        self.requestLimits = requestLimits
        self.responseLimits = responseLimits
        self.errorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    }

    package init(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        requestLimits: DatabaseWireLimits = .default,
        responseLimits: DatabaseWireLimits = .default
    ) {
        self.dispatcher = DatabaseOperationDispatcher(
            container: container,
            registry: registry,
            admissionPolicy: admissionPolicy,
            middlewares: middlewares
        )
        self.instance = nil
        self.requestLimits = requestLimits
        self.responseLimits = responseLimits
        self.errorMapper = AnyDatabaseErrorMapper(
            CanonicalDatabaseErrorMapper()
        )
    }

    package init<Mapper: DatabaseErrorMapper>(
        container: DBContainer,
        registry: DatabaseOperationRegistry,
        admissionPolicy: AnyDatabaseOperationAdmissionPolicy,
        middlewares: [AnyDatabaseRequestMiddleware] = [],
        requestLimits: DatabaseWireLimits = .default,
        responseLimits: DatabaseWireLimits = .default,
        errorMapper: Mapper
    ) {
        self.dispatcher = DatabaseOperationDispatcher(
            container: container,
            registry: registry,
            admissionPolicy: admissionPolicy,
            middlewares: middlewares
        )
        self.instance = nil
        self.requestLimits = requestLimits
        self.responseLimits = responseLimits
        self.errorMapper = AnyDatabaseErrorMapper(errorMapper)
    }

    public func execute(
        _ bytes: ByteString,
        context executionContext: DatabaseRequestExecutionContext
    ) async throws -> ByteString {
        let request: DatabaseWireRequestEnvelope
        do {
            request = try DatabaseWireDecoder(limits: requestLimits)
                .decodeRequestEnvelope(bytes)
        } catch {
            throw DatabaseWireAdapterError.invalidRequestFrame(error)
        }

        let outcome: DatabaseOperationDispatchOutcome
        if let instance {
            outcome = try await instance.dispatch(
                request,
                context: executionContext,
                requestLimits: requestLimits,
                responseLimits: responseLimits
            )
        } else {
            outcome = try await dispatcher.execute(
                request,
                context: executionContext,
                requestLimits: requestLimits,
                responseLimits: responseLimits
            )
        }
        switch outcome {
        case .success(let result, let context):
            do {
                return try result.encodeResponse(
                    requestID: request.requestID,
                    limits: responseLimits
                )
            } catch let wireError as DatabaseWireError {
                return try encodeFailureResponse(
                    for: request,
                    error: errorMapper.remoteError(
                        for: DatabaseResponsePreparationError(
                            wireError: wireError
                        ),
                        context: context,
                        limits: responseLimits
                    )
                )
            } catch {
                return try encodeFailureResponse(
                    for: request,
                    error: errorMapper.remoteError(
                        for: error,
                        context: context,
                        limits: responseLimits
                    )
                )
            }
        case .failure(let failure):
            return try encodeFailureResponse(
                for: request,
                error: errorMapper.remoteError(
                    for: failure.error,
                    context: failure.context,
                    limits: responseLimits
                )
            )
        }
    }

    private func encodeFailureResponse(
        for request: DatabaseWireRequestEnvelope,
        error remoteError: RemoteOperationError
    ) throws -> ByteString {
        do {
            return try encodeFailureEnvelope(for: request, error: remoteError)
        } catch {
            let reduced = RemoteOperationError(
                category: remoteError.category,
                code: Self.stringPrefix(
                    remoteError.code,
                    maximumBytes: responseLimits.maximumStringBytes
                ),
                message: Self.stringPrefix(
                    remoteError.message,
                    maximumBytes: responseLimits.maximumStringBytes
                ),
                retryability: remoteError.retryability
            )
            do {
                return try encodeFailureEnvelope(for: request, error: reduced)
            } catch {
                let fallback = RemoteOperationError(
                    category: .internalFailure,
                    code: Self.stringPrefix(
                        "FAILURE_RESPONSE_ENCODING_FAILED",
                        maximumBytes: responseLimits.maximumStringBytes
                    ),
                    message: Self.stringPrefix(
                        "Failure response exceeded configured wire limits",
                        maximumBytes: responseLimits.maximumStringBytes
                    ),
                    retryability: .never
                )
                do {
                    return try encodeFailureEnvelope(
                        for: request,
                        error: fallback
                    )
                } catch let wireError {
                    throw DatabaseWireAdapterError.responseEncodingFailed(
                        wireError
                    )
                }
            }
        }
    }

    private func encodeFailureEnvelope(
        for request: DatabaseWireRequestEnvelope,
        error: RemoteOperationError
    ) throws(DatabaseWireError) -> ByteString {
        try DatabaseWireEncoder(limits: responseLimits).encodeFailure(
            requestID: request.requestID,
            operation: request.operation,
            error: error
        )
    }

    private static func stringPrefix(
        _ string: String,
        maximumBytes: Int
    ) -> String {
        guard maximumBytes > 0 else { return "" }
        guard string.utf8.count > maximumBytes else { return string }

        // A String allocation is required at the external Wire boundary.
        // Scalar iteration avoids an intermediate UTF-8 array and never
        // truncates a scalar.
        var result = ""
        result.reserveCapacity(maximumBytes)
        var byteCount = 0
        for scalar in string.unicodeScalars {
            let addition = byteCount.addingReportingOverflow(
                scalar.utf8.count
            )
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
