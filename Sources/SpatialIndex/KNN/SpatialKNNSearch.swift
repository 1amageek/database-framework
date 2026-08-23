import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Exact K-nearest-neighbor execution over one spatial index snapshot.
///
/// The reference implementation scans the complete spatial index up to an
/// explicit candidate cap. It does not prune from approximate cell bounds, so
/// reaching the end of the index is sufficient proof that the returned top-k
/// is exact for both S2 and Morton encodings. A sentinel entry distinguishes
/// exact exhaustion from resource-limited partial output.
package struct SpatialKNNSearch<T: Persistable>: Sendable {
    package struct SearchResult: ~Copyable, Sendable {
        private let retainedItems:
            DatabaseRetainedBuffer<(item: T, distance: Double)>
        package let limitReason: LimitReason?

        fileprivate init(
            retainedItems: consuming DatabaseRetainedBuffer<(
                item: T,
                distance: Double
            )>,
            limitReason: LimitReason?
        ) {
            self.retainedItems = retainedItems
            self.limitReason = limitReason
        }

        package var count: Int { retainedItems.count }

        package consuming func promoteToOutput() -> [(
            item: T,
            distance: Double
        )] {
            retainedItems.promoteToOutput()
        }
    }

    private let entity: Schema.Entity
    private let indexName: String
    private let indexSubspace: Subspace
    private let encoding: SpatialEncoding
    private let level: Int
    private let fieldName: String
    private let maximumCandidatesToScan: Int

    package init(
        entity: Schema.Entity,
        indexName: String,
        indexSubspace: Subspace,
        encoding: SpatialEncoding,
        level: Int,
        fieldName: String,
        maximumCandidatesToScan: Int = 50_000
    ) {
        self.entity = entity
        self.indexName = indexName
        self.indexSubspace = indexSubspace
        self.encoding = encoding
        self.level = level
        self.fieldName = fieldName
        self.maximumCandidatesToScan = maximumCandidatesToScan
    }

    package func findKNearest(
        k: Int,
        from queryPoint: GeographicPoint,
        transaction: any IndexQueryReadAccess,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> SearchResult {
        precondition(k > 0)
        precondition(maximumCandidatesToScan > 0)

        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: encoding,
            level: level
        )
        let scan = try await scanner.scanAllRetained(
            maximumEntries: maximumCandidatesToScan,
            transaction: transaction,
            workMeter: workMeter
        )
        let models = try await transaction.fetchPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: scan.keys,
            partitions: partitions,
            workMeter: workMeter
        )
        var candidates = try DatabaseRetainedArrayBuilder<(
            item: T,
            distance: Double
        )>(
            workMeter: workMeter,
            stage: .projection,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: (item: T, distance: Double).self
            ),
            expectedCount: models.count
        )

        for index in models.indices {
            guard let model = models[index] else {
                throw SpatialQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: scan.keys[index].pack()
                )
            }
            let admission = try candidates.prepareAppend(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: model,
                    workMeter: workMeter
                ).adding(DatabaseIntermediateFootprint(bytes: 8)),
                at: .projection
            )
            let item = try model.decode(as: T.self)
            guard let location = try extractGeographicPoint(from: item) else {
                continue
            }
            let distance = CellDistanceCalculator.haversineDistance(
                from: queryPoint,
                to: location
            )
            candidates.append(
                (item: item, distance: distance),
                using: admission
            )
        }

        let sorted = candidates.finish().sortingElements {
            lhs,
            rhs in
            if lhs.distance != rhs.distance {
                return lhs.distance < rhs.distance
            }
            return lhs.item.id.persistableIdentifierValue
                < rhs.item.id.persistableIdentifierValue
        }
        let topKCount = min(k, sorted.count)
        let retainedTopK = sorted.retainingSubrange(0..<topKCount)
        return SearchResult(
            retainedItems: retainedTopK,
            limitReason: scan.limitReason
        )
    }

    private func extractGeographicPoint(from item: T) throws -> GeographicPoint? {
        guard let fieldNumber = T.fieldNumber(for: fieldName) else {
            throw SpatialIndexMaintenanceError.invalidFieldExpression(
                indexName: fieldName
            )
        }
        guard let value = try item.persistedFieldValue(
            for: FieldIdentity(name: fieldName, number: fieldNumber)
        ) else {
            throw SpatialIndexMaintenanceError.missingCoordinate(
                fieldName: fieldName
            )
        }
        switch value {
        case .null:
            return nil
        case .geographicPoint(let point):
            return point
        case .geographicPosition(let position):
            return position.point
        default:
            throw SpatialIndexMaintenanceError.unsupportedCoordinateValue(
                fieldName: fieldName
            )
        }
    }
}
