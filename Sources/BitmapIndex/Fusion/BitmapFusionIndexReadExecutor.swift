import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

package struct BitmapFusionIndexReadExecutor: FusionIndexReadExecutor {
    package let indexType: IndexType = .bitmap

    package init() {}

    package func validate(
        _ request: FusionIndexValidationRequest
    ) throws {
        try validateParameterNames(request.source.parameters)
        let fieldName = try string(
            BitmapReadParameter.fieldName,
            from: request.source.parameters
        )
        let operation = try string(
            BitmapReadParameter.operation,
            from: request.source.parameters
        )
        let values = try canonicalValues(from: request.source.parameters)
        do {
            for value in values {
                _ = try value.toTupleElement()
            }
        } catch {
            throw invalid(BitmapReadParameter.values)
        }
        guard request.descriptor.type == indexType,
              request.descriptor.fieldNames == [fieldName],
              request.descriptor.fieldIdentities
                == request.source.referencedFields else {
            throw invalid(BitmapReadParameter.fieldName)
        }
        guard request.scoring == nil else {
            throw invalid("scoring")
        }
        switch operation {
        case BitmapReadParameter.equalsOperation:
            guard values.count == 1 else {
                throw invalid(BitmapReadParameter.values)
            }
        case BitmapReadParameter.inOperation:
            break
        default:
            throw invalid(BitmapReadParameter.operation)
        }
    }

    package func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            return try await execute(
                request,
                candidates: nil,
                output: output
            )
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    package func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            return try await execute(
                request,
                candidates: candidates,
                output: output
            )
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    private func execute(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain?,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        guard request.limit > 0 else { return .satisfiedLimit }
        let values = try canonicalValues(from: request.source.parameters)
        guard !values.isEmpty else { return .exhausted }

        var bitmap = try await readBitmap(
            for: values[0],
            request: request
        )
        for value in values.dropFirst() {
            let next = try await readBitmap(for: value, request: request)
            bitmap = try union(bitmap, next, workMeter: request.workMeter)
        }

        let iteratorReservation = try request.workMeter.reserveIntermediate(
            bytes: try bitmap.value.iteratorRetainedStorageByteCount(),
            at: .indexScan
        )
        defer { iteratorReservation.release() }
        let ids = request.access.index.subspace.subspace("ids")
        var emitted = 0
        for identifier in bitmap.value {
            try request.workMeter.consume(at: .indexScan)
            let keyReservation = try request.workMeter.reserveIntermediate(
                bytes: 64,
                at: .indexScan
            )
            let value: FusionIndexReadValue
            do {
                let identifierTuple = Tuple(Int(identifier))
                let keyByteCount = try DatabaseIntermediateFootprint(
                    bytes: UInt64(identifierTuple.packedByteCount)
                ).adding(
                    DatabaseIntermediateFootprint(
                        bytes: UInt64(ids.prefix.count)
                    )
                ).bytes
                try keyReservation.reserveAdditional(
                    bytes: keyByteCount,
                    at: .indexScan
                )
                let key = ids.pack(identifierTuple)
                guard let resolved = try await request.access.getValue(
                    key: key
                ) else {
                    throw BitmapFusionStorageError
                        .missingPrimaryKeyMapping(identifier)
                }
                value = resolved
                keyReservation.release()
            } catch {
                keyReservation.release()
                throw error
            }
            if let candidates,
               try !candidates.contains(
                   primaryKey: value.bytes,
                   workMeter: request.workMeter
               ) {
                continue
            }
            try output.submit(primaryKey: value.bytes, numericSignal: nil)
            emitted += 1
            if emitted == request.limit {
                return .satisfiedLimit
            }
        }
        withExtendedLifetime(bitmap) {}
        return .exhausted
    }

    private func readBitmap(
        for value: FieldValue,
        request: FusionIndexReadRequest
    ) async throws -> BitmapFusionRetainedBitmap {
        let keyReservation = try request.workMeter.reserveIntermediate(
            bytes: 64,
            at: .indexScan
        )
        let stored: FusionIndexReadValue?
        do {
            let element = try value.toTupleElement()
            let tuple = Tuple(element)
            let data = request.access.index.subspace.subspace("data")
            let keyByteCount = try DatabaseIntermediateFootprint(
                bytes: UInt64(tuple.packedByteCount)
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(data.prefix.count)
                )
            ).bytes
            try keyReservation.reserveAdditional(
                bytes: keyByteCount,
                at: .indexScan
            )
            let key = data.pack(tuple)
            stored = try await request.access.getValue(key: key)
            keyReservation.release()
        } catch {
            keyReservation.release()
            throw error
        }
        guard let stored else {
            let empty = RoaringBitmap()
            return BitmapFusionRetainedBitmap(
                value: empty,
                reservation: try request.workMeter.reserveIntermediate(
                    bytes: try empty.retainedStorageByteCount(),
                    at: .indexScan
                )
            )
        }

        let reservation = try request.workMeter.reserveIntermediate(
            at: .indexScan
        )
        do {
            let decoded = try RoaringBitmap(
                serializedBytes: stored.bytes
            ) { bytes in
                try reservation.reserveAdditional(
                    bytes: bytes,
                    at: .indexScan
                )
            }
            return BitmapFusionRetainedBitmap(
                value: decoded,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func union(
        _ left: BitmapFusionRetainedBitmap,
        _ right: BitmapFusionRetainedBitmap,
        workMeter: DatabaseWorkMeter
    ) throws -> BitmapFusionRetainedBitmap {
        let maximumOutputBytes = try left.value
            .unionRetainedStorageUpperBound(with: right.value)
        let reservation = try workMeter.reserveIntermediate(
            bytes: maximumOutputBytes,
            at: .indexScan
        )
        let scratch = try workMeter.reserveIntermediate(
            bytes: try left.value.unionScratchByteCount(with: right.value),
            at: .indexScan
        )
        defer { scratch.release() }
        let value = left.value || right.value
        let retainedBytes = try value.retainedStorageByteCount()
        guard retainedBytes <= maximumOutputBytes else {
            throw FusionExecutionError.executionContractViolation
        }
        if retainedBytes < maximumOutputBytes {
            try reservation.releasePartial(
                bytes: maximumOutputBytes - retainedBytes
            )
        }
        withExtendedLifetime(left) {}
        withExtendedLifetime(right) {}
        return BitmapFusionRetainedBitmap(
            value: value,
            reservation: reservation
        )
    }

    private func canonicalValues(
        from parameters: [String: FieldValue]
    ) throws -> [FieldValue] {
        guard let values = parameters[BitmapReadParameter.values]?.arrayValue
        else {
            throw invalid(BitmapReadParameter.values)
        }
        for value in values {
            switch value {
            case .null, .bool,
                    .int8, .int16, .int32, .int64,
                    .uint8, .uint16, .uint32, .uint64,
                    .float32, .float64, .string, .bytes, .uuid:
                continue
            default:
                throw invalid(BitmapReadParameter.values)
            }
        }
        return values
    }

    private func string(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[name]?.stringValue else {
            throw invalid(name)
        }
        return value
    }

    private func validateParameterNames(
        _ parameters: [String: FieldValue]
    ) throws {
        for name in parameters.keys {
            switch name {
            case BitmapReadParameter.fieldName,
                 BitmapReadParameter.operation,
                 BitmapReadParameter.values:
                continue
            default:
                throw invalid(name)
            }
        }
    }

    private func invalid(_ parameter: String) -> FusionExecutionError {
        .invalidIndexInput(indexType: indexType, parameter: parameter)
    }

    private func sanitizedExecutionError(_ error: any Error) -> any Error {
        if error is RoaringBitmapFormatError
            || error is BitmapFusionStorageError
            || error is ByteConversionError
            || error is TupleError {
            return FusionExecutionError.corruptedIndex(indexType)
        }
        if error is FieldValueTupleCodecError {
            return FusionExecutionError.executionContractViolation
        }
        if let cleanup = error as? StorageRangeTerminalCleanupError {
            return StorageRangeTerminalCleanupError(
                cleanupError: sanitizedExecutionError(cleanup.cleanupError)
            )
        }
        if let cleanup = error as? StorageRangeCleanupError {
            return StorageRangeCleanupError(
                iterationError: sanitizedExecutionError(
                    cleanup.iterationError
                ),
                cleanupError: sanitizedExecutionError(cleanup.cleanupError)
            )
        }
        return error
    }
}

private enum BitmapFusionStorageError: Error, Sendable, Equatable {
    case missingPrimaryKeyMapping(UInt32)
}

private struct BitmapFusionRetainedBitmap: Sendable {
    let value: RoaringBitmap
    let reservation: DatabaseIntermediateReservation
}
