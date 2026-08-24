import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

package struct SpatialFusionIndexReadExecutor: FusionIndexReadExecutor {
    package let indexType: IndexType = .spatial

    package init() {}

    package func validate(
        _ request: FusionIndexValidationRequest
    ) throws {
        let prepared = try parameters(from: request.source.parameters)
        guard request.descriptor.type == indexType,
              request.descriptor.fieldIdentities
                == request.source.referencedFields,
              request.descriptor.fieldNames == [prepared.fieldName],
              case .spatial(let location, _, _) = request.descriptor
                .declaration.definition,
              location == request.source.referencedFields.first else {
            throw invalid(SpatialFusionReadParameter.fieldName)
        }
        guard request.scoring == .annotation(
            name: "distance",
            order: .lowerIsBetter
        ) else {
            throw invalid("scoring")
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
        let prepared = try parameters(from: request.source.parameters)
        let physicalLayout = request.access.index.physicalLayout
        guard physicalLayout.name == "spatial.exact-coordinate",
              physicalLayout.revision == 1,
              physicalLayout.parameters.isEmpty else {
            throw FusionExecutionError.executionContractViolation
        }
        let configuration = try configuration(
            from: request.access.index.descriptor.declaration.definition
        )
        let cursor = try request.access.subspaceCursor(
            request.access.index.subspace,
            reverse: false
        )
        var topK = try SpatialFusionTopK(
            limit: request.limit,
            workMeter: request.workMeter
        )
        while let row = try await cursor.next() {
            let entry = try entryIdentity(
                from: row.key,
                indexSubspace: request.access.index.subspace
            )
            let coordinate = try SpatialIndexValueCodec.decode(row.value)
            guard SpatialCodeEncoder.encode(
                coordinate,
                encoding: configuration.encoding,
                level: configuration.level
            ) == entry.spatialCode else {
                throw SpatialFusionStorageError.malformedKey
            }
            if let candidates,
               try !candidates.contains(
                   primaryKey: entry.primaryKey,
                   workMeter: request.workMeter
               ) {
                continue
            }
            let distance = CellDistanceCalculator.haversineDistance(
                from: prepared.referencePoint,
                to: coordinate.point
            )
            guard distance.isFinite else {
                throw SpatialFusionStorageError.nonFiniteDistance
            }
            guard prepared.constraint.contains(
                coordinate.point,
                distanceFromReference: distance
            ) else {
                continue
            }
            try topK.consider(
                primaryKey: entry.primaryKey,
                distance: distance
            )
        }
        try topK.emit(to: output)
        return .exhausted
    }

    private func entryIdentity(
        from key: ByteString,
        indexSubspace: Subspace
    ) throws -> (spatialCode: UInt64, primaryKey: ByteString) {
        var cursor = try indexSubspace.tupleCursor(for: key)
        let spatialCode: UInt64
        switch try cursor.requireNext().tupleValue {
        case .signedInteger(let value) where value >= 0:
            spatialCode = UInt64(value)
        case .unsignedInteger(let value):
            spatialCode = value
        default:
            throw SpatialFusionStorageError.malformedKey
        }
        let primaryKeyStart = key.startIndex
            + indexSubspace.prefix.count
            + cursor.consumedByteCount
        guard try cursor.next() != nil else {
            throw SpatialFusionStorageError.malformedKey
        }
        while try cursor.next() != nil {}
        return (
            spatialCode: spatialCode,
            primaryKey: key[primaryKeyStart..<key.endIndex]
        )
    }

    private func configuration(
        from definition: IndexDefinition<FieldIdentity>
    ) throws -> SpatialFusionIndexConfiguration {
        guard case .spatial(_, let encoding, let level) = definition else {
            throw FusionExecutionError.executionContractViolation
        }
        return SpatialFusionIndexConfiguration(
            encoding: encoding,
            level: level
        )
    }

    private func parameters(
        from values: [String: FieldValue]
    ) throws -> SpatialFusionPreparedRead {
        let fieldName = try string(
            SpatialFusionReadParameter.fieldName,
            from: values
        )
        let operation = try string(
            SpatialFusionReadParameter.operation,
            from: values
        )
        let referencePoint = try point(
            SpatialFusionReadParameter.referencePoint,
            from: values
        )
        let constraint: SpatialFusionConstraint
        let expectedParameters: Set<String>
        switch operation {
        case SpatialFusionReadParameter.radiusOperation:
            let center = try point(
                SpatialFusionReadParameter.center,
                from: values
            )
            let radiusMeters = try finiteDouble(
                SpatialFusionReadParameter.radiusMeters,
                from: values
            )
            guard radiusMeters >= 0 else {
                throw invalid(SpatialFusionReadParameter.radiusMeters)
            }
            guard center == referencePoint else {
                throw invalid(SpatialFusionReadParameter.referencePoint)
            }
            constraint = .radius(meters: radiusMeters)
            expectedParameters = [
                SpatialFusionReadParameter.fieldName,
                SpatialFusionReadParameter.operation,
                SpatialFusionReadParameter.center,
                SpatialFusionReadParameter.radiusMeters,
                SpatialFusionReadParameter.referencePoint,
            ]
        case SpatialFusionReadParameter.boundsOperation:
            let bounds: BoundingBox
            do {
                bounds = try BoundingBox(
                    minLatitude: try finiteDouble(
                        SpatialFusionReadParameter.minimumLatitude,
                        from: values
                    ),
                    minLongitude: try finiteDouble(
                        SpatialFusionReadParameter.minimumLongitude,
                        from: values
                    ),
                    maxLatitude: try finiteDouble(
                        SpatialFusionReadParameter.maximumLatitude,
                        from: values
                    ),
                    maxLongitude: try finiteDouble(
                        SpatialFusionReadParameter.maximumLongitude,
                        from: values
                    )
                )
            } catch is BoundingBoxError {
                throw invalid(SpatialFusionReadParameter.operation)
            }
            let center: GeographicPoint
            do {
                center = try bounds.center()
            } catch {
                throw invalid(SpatialFusionReadParameter.referencePoint)
            }
            guard center == referencePoint else {
                throw invalid(SpatialFusionReadParameter.referencePoint)
            }
            constraint = .bounds(bounds)
            expectedParameters = [
                SpatialFusionReadParameter.fieldName,
                SpatialFusionReadParameter.operation,
                SpatialFusionReadParameter.minimumLatitude,
                SpatialFusionReadParameter.minimumLongitude,
                SpatialFusionReadParameter.maximumLatitude,
                SpatialFusionReadParameter.maximumLongitude,
                SpatialFusionReadParameter.referencePoint,
            ]
        default:
            throw invalid(SpatialFusionReadParameter.operation)
        }
        guard Set(values.keys) == expectedParameters else {
            throw invalid(SpatialFusionReadParameter.operation)
        }
        return SpatialFusionPreparedRead(
            fieldName: fieldName,
            constraint: constraint,
            referencePoint: referencePoint
        )
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

    private func point(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> GeographicPoint {
        guard case .geographicPoint(let value)? = parameters[name] else {
            throw invalid(name)
        }
        return value
    }

    private func finiteDouble(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> Double {
        guard case .float64(let value)? = parameters[name],
              value.isFinite else {
            throw invalid(name)
        }
        return value
    }

    private func invalid(_ parameter: String) -> FusionExecutionError {
        .invalidIndexInput(indexType: indexType, parameter: parameter)
    }

    private func sanitizedExecutionError(_ error: any Error) -> any Error {
        if error is SpatialIndexValueCodecError
            || error is TupleError {
            return FusionExecutionError.corruptedIndex(indexType)
        }
        if let storageError = error as? SpatialFusionStorageError {
            switch storageError {
            case .malformedKey:
                return FusionExecutionError.corruptedIndex(indexType)
            case .nonFiniteDistance:
                return FusionExecutionError.executionContractViolation
            }
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

private struct SpatialFusionPreparedRead: Sendable {
    let fieldName: String
    let constraint: SpatialFusionConstraint
    let referencePoint: GeographicPoint
}

private struct SpatialFusionIndexConfiguration: Sendable {
    let encoding: SpatialEncoding
    let level: Int
}

private enum SpatialFusionConstraint: Sendable {
    case radius(meters: Double)
    case bounds(BoundingBox)

    func contains(
        _ point: GeographicPoint,
        distanceFromReference: Double
    ) -> Bool {
        switch self {
        case .radius(let meters):
            return distanceFromReference <= meters
        case .bounds(let bounds):
            return bounds.contains(point)
        }
    }
}

private enum SpatialFusionStorageError: Error, Sendable, Equatable {
    case malformedKey
    case nonFiniteDistance
}
