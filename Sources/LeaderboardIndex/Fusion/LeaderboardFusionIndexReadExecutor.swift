import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

package struct LeaderboardFusionIndexReadExecutor: FusionIndexReadExecutor {
    package let indexType: IndexType = .leaderboard

    package init() {}

    package func validate(
        _ request: FusionIndexValidationRequest
    ) throws {
        try validateParameterNames(request.source.parameters)
        let configuration: TimeWindowLeaderboardConfiguration
        do {
            configuration = try TimeWindowLeaderboardConfiguration(
                definition: request.descriptor.declaration.definition
            )
        } catch {
            throw FusionExecutionError.executionContractViolation
        }
        let scoreField = try string(
            LeaderboardFusionReadParameter.scoreField,
            from: request.source.parameters
        )
        guard request.descriptor.type == indexType,
              configuration.scoreFieldName == scoreField,
              request.descriptor.fieldIdentities.last
                == request.source.referencedFields.first,
              request.source.referencedFields.count == 1 else {
            throw invalid(LeaderboardFusionReadParameter.scoreField)
        }
        guard request.scoring == .position else {
            throw invalid("scoring")
        }
        if let grouping = try groupingValues(
            from: request.source.parameters
        ) {
            guard grouping.count == configuration.groupingFieldNames.count
            else {
                throw invalid(LeaderboardFusionReadParameter.grouping)
            }
            do {
                _ = try FieldValue.toTupleElements(grouping)
            } catch {
                throw invalid(LeaderboardFusionReadParameter.grouping)
            }
        }
        _ = try windowID(from: request.source.parameters)
    }

    package func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            let prepared = try prepare(request)
            if prepared.isStorageOrdered {
                return try await executeOrdered(
                    prepared,
                    request: request,
                    output: output
                )
            }
            return try await executeGlobal(
                prepared,
                request: request,
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
            guard request.limit > 0 else { return .satisfiedLimit }
            let prepared = try prepare(request)
            var topK = try LeaderboardFusionTopK(
                limit: request.limit,
                workMeter: request.workMeter
            )
            let positions = request.access.index.subspace.subspace("pos")
            for index in 0..<candidates.count {
                let primaryKey = candidates.primaryKey(at: index)
                guard let position = try await readPosition(
                    primaryKey: primaryKey,
                    positions: positions,
                    prepared: prepared,
                    request: request
                ) else {
                    continue
                }
                try topK.consider(
                    primaryKey: primaryKey,
                    score: position
                )
            }
            try topK.emit(to: output)
            withExtendedLifetime(prepared) {}
            return .exhausted
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    private func executeOrdered(
        _ prepared: LeaderboardFusionPreparedRead,
        request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        guard request.limit > 0 else { return .satisfiedLimit }
        let scanSubspace = try retainedScanSubspace(
            prepared: prepared,
            request: request
        )
        defer { withExtendedLifetime(scanSubspace) {} }
        let cursor = try openFusionSubspaceCursor(
            using: request.access,
            in: scanSubspace.subspace,
            reverse: false
        )
        var emitted = 0
        while let row = try await cursor.next() {
            let entry = try decodeWindowEntry(
                row.key,
                prepared: prepared,
                request: request
            )
            try output.submit(
                primaryKey: entry.primaryKey,
                numericSignal: nil
            )
            emitted += 1
            if emitted == request.limit {
                return .satisfiedLimit
            }
        }
        return .exhausted
    }

    private func executeGlobal(
        _ prepared: LeaderboardFusionPreparedRead,
        request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        guard request.limit > 0 else { return .satisfiedLimit }
        let scanSubspace = try retainedScanSubspace(
            prepared: prepared,
            request: request
        )
        defer { withExtendedLifetime(scanSubspace) {} }
        let cursor = try openFusionSubspaceCursor(
            using: request.access,
            in: scanSubspace.subspace,
            reverse: false
        )
        var topK = try LeaderboardFusionTopK(
            limit: request.limit,
            workMeter: request.workMeter
        )
        while let row = try await cursor.next() {
            let entry = try decodeWindowEntry(
                row.key,
                prepared: prepared,
                request: request
            )
            try topK.consider(
                primaryKey: entry.primaryKey,
                score: entry.score
            )
        }
        try topK.emit(to: output)
        return .exhausted
    }

    private func prepare(
        _ request: FusionIndexReadRequest
    ) throws -> LeaderboardFusionPreparedRead {
        let configuration = try TimeWindowLeaderboardConfiguration(
            definition: request.access.index.descriptor.declaration.definition
        )
        let grouping = try groupingValues(from: request.source.parameters)
        let retainedGrouping = try grouping.map {
            try retainGrouping($0, request: request)
        }
        let resolvedWindowID = try windowID(from: request.source.parameters)
            ?? request.timestamp.secondsSinceUnixEpoch
                / Int64(configuration.window.durationSeconds)
        return LeaderboardFusionPreparedRead(
            configuration: configuration,
            windowID: resolvedWindowID,
            grouping: retainedGrouping
        )
    }

    private func retainGrouping(
        _ values: [FieldValue],
        request: FusionIndexReadRequest
    ) throws -> ByteString {
        var encodedByteCount: UInt64 = 0
        for value in values {
            let prepared = try FieldValueTupleCodec.prepareComposite(
                value,
                limits: .default
            )
            encodedByteCount = try DatabaseIntermediateFootprint(
                bytes: encodedByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(prepared.encodedByteCount)
                )
            ).bytes
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(
            (any TupleElement).self
        )
        let growth = try layout.growth(from: 0, toFit: values.count)
        let transientBytes = try DatabaseIntermediateFootprint(
            bytes: layout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: growth.additionalByteCount
            )
        ).bytes
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(bytes: encodedByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: transientBytes))
                .adding(DatabaseIntermediateFootprint(bytes: 64)).bytes,
            at: .indexScan
        )
        do {
            let packed: ByteString = try {
                let elements = try FieldValue.toTupleElements(values)
                return Tuple(elements).pack()
            }()
            guard UInt64(packed.count) == encodedByteCount else {
                throw FusionExecutionError.executionContractViolation
            }
            try reservation.releasePartial(bytes: transientBytes)
            return try DatabaseRetainedByteString.make(
                packed,
                reservation: reservation,
                at: .indexScan
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func retainedScanSubspace(
        prepared: LeaderboardFusionPreparedRead,
        request: FusionIndexReadRequest
    ) throws -> LeaderboardFusionRetainedSubspace {
        let windows = request.access.index.subspace.subspace("window")
        let windowByteCount = Tuple(prepared.windowID).packedByteCount
        let groupingByteCount = prepared.grouping?.count ?? 0
        let tupleByteCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(windowByteCount)
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(groupingByteCount))
        ).bytes
        let prefixByteCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(windows.prefix.count)
        ).adding(
            DatabaseIntermediateFootprint(bytes: tupleByteCount)
        ).bytes
        guard let encodedTupleByteCount = Int(exactly: tupleByteCount) else {
            throw FusionExecutionError.executionContractViolation
        }
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(bytes: prefixByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: 64)).bytes,
            at: .indexScan
        )
        do {
            let prefix = windows.pack(
                encodedTupleByteCount: encodedTupleByteCount
            ) { sink in
                prepared.windowID.encodeTuple(to: &sink)
                if let grouping = prepared.grouping {
                    grouping.withUnsafeBytes { source in
                        sink.writeBytes(source)
                    }
                }
            }
            return LeaderboardFusionRetainedSubspace(
                subspace: Subspace(
                    prefix: try DatabaseRetainedByteString.make(
                        prefix,
                        reservation: reservation,
                        at: .indexScan
                    )
                )
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func decodeWindowEntry(
        _ key: ByteString,
        prepared: LeaderboardFusionPreparedRead,
        request: FusionIndexReadRequest
    ) throws -> (primaryKey: ByteString, score: Int64) {
        let windows = request.access.index.subspace.subspace("window")
        guard windows.contains(key) else {
            throw LeaderboardFusionStorageError.malformedWindowEntry
        }
        let tupleBytes = key[
            (key.startIndex + windows.prefix.count)..<key.endIndex
        ]
        var cursor = TupleCursor(bytes: tupleBytes)
        guard try cursor.requireInt64() == prepared.windowID else {
            throw LeaderboardFusionStorageError.malformedWindowEntry
        }
        for _ in 0..<prepared.configuration.groupingFieldNames.count {
            _ = try cursor.requireNext()
        }
        let invertedScore = try cursor.requireInt64()
        let primaryKeyStart = tupleBytes.startIndex + cursor.consumedByteCount
        guard try cursor.next() != nil else {
            throw LeaderboardFusionStorageError.malformedWindowEntry
        }
        while try cursor.next() != nil {}
        let primaryKey = tupleBytes[primaryKeyStart..<tupleBytes.endIndex]
        let score = Int64(
            bitPattern: UInt64.max - UInt64(bitPattern: invertedScore)
        )
        return (primaryKey, score)
    }

    private func readPosition(
        primaryKey: ByteString,
        positions: Subspace,
        prepared: LeaderboardFusionPreparedRead,
        request: FusionIndexReadRequest
    ) async throws -> Int64? {
        let keyByteCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(positions.prefix.count)
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(primaryKey.count))
        ).bytes
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: keyByteCount,
            at: .indexScan
        )
        let stored: FusionIndexReadValue?
        do {
            let key = positions.prefix.appending(contentsOf: primaryKey)
            stored = try await request.access.getValue(key: key)
            reservation.release()
        } catch {
            reservation.release()
            throw error
        }
        guard let stored else { return nil }
        var cursor = TupleCursor(bytes: stored.bytes)
        let windowID = try cursor.requireInt64()
        let score = try cursor.requireInt64()
        let groupingStart = stored.bytes.startIndex + cursor.consumedByteCount
        for _ in 0..<prepared.configuration.groupingFieldNames.count {
            _ = try cursor.requireNext()
        }
        guard cursor.isAtEnd else {
            throw LeaderboardFusionStorageError.malformedPosition
        }
        guard windowID == prepared.windowID else { return nil }
        if let expectedGrouping = prepared.grouping {
            let storedGrouping = stored.bytes[
                groupingStart..<stored.bytes.endIndex
            ]
            try DatabaseByteProcessingMeter.consume(
                byteCount: max(storedGrouping.count, expectedGrouping.count),
                workMeter: request.workMeter,
                stage: .indexScan
            )
            guard storedGrouping == expectedGrouping else { return nil }
        }
        return score
    }

    private func groupingValues(
        from parameters: [String: FieldValue]
    ) throws -> [FieldValue]? {
        guard let value = parameters[LeaderboardFusionReadParameter.grouping]
        else {
            return nil
        }
        guard case .array(let values) = value else {
            throw invalid(LeaderboardFusionReadParameter.grouping)
        }
        return values
    }

    private func windowID(
        from parameters: [String: FieldValue]
    ) throws -> Int64? {
        guard let value = parameters[LeaderboardFusionReadParameter.windowID]
        else {
            return nil
        }
        guard case .int64(let windowID) = value else {
            throw invalid(LeaderboardFusionReadParameter.windowID)
        }
        return windowID
    }

    private func string(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard case .string(let value)? = parameters[name] else {
            throw invalid(name)
        }
        return value
    }

    private func validateParameterNames(
        _ parameters: [String: FieldValue]
    ) throws {
        for name in parameters.keys {
            switch name {
            case LeaderboardFusionReadParameter.scoreField,
                 LeaderboardFusionReadParameter.grouping,
                 LeaderboardFusionReadParameter.windowID:
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
        if error is LeaderboardFusionStorageError
            || error is TimeWindowLeaderboardIndexError
            || error is TupleError {
            return FusionExecutionError.corruptedIndex(indexType)
        }
        if error is FieldValueTupleCodecError
            || error is TimeWindowLeaderboardConfigurationError {
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

private struct LeaderboardFusionPreparedRead: Sendable {
    let configuration: TimeWindowLeaderboardConfiguration
    let windowID: Int64
    let grouping: ByteString?

    var isStorageOrdered: Bool {
        grouping != nil || configuration.groupingFieldNames.isEmpty
    }
}

private struct LeaderboardFusionRetainedSubspace: Sendable {
    let subspace: Subspace
}

private enum LeaderboardFusionStorageError: Error, Sendable {
    case malformedWindowEntry
    case malformedPosition
}
