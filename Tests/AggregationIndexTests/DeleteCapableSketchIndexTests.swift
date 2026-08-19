import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing

@testable import AggregationIndex

@Suite("Delete-capable sketch indexes")
struct DeleteCapableSketchIndexTests {
    @Test("Membership metadata tracks unique keys and exact scan bytes")
    func membershipMetadataTracksRebuildCost() async throws {
        let engine = InMemoryEngine()
        let memberA = ByteString([0x01, 0x02, 0x03])
        let memberB = ByteString([0x04, 0x05, 0x06, 0x07])
        let metadataKey = ByteString([0xFF])
        let maximumBytes = 1_024

        try await engine.withTransaction { transaction in
            let insertedA = try await incrementAggregationMembership(
                key: memberA,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
            #expect(insertedA)
            let retainedA = try await incrementAggregationMembership(
                key: memberA,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
            #expect(!retainedA)
            let insertedB = try await incrementAggregationMembership(
                key: memberB,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
            #expect(insertedB)
        }

        let expectedBytes = try aggregationMemberScanByteCount(for: memberA)
            + aggregationMemberScanByteCount(for: memberB)
        let metadata = try await engine.withTransaction { transaction in
            let bytes = try #require(
                try await transaction.getValue(for: metadataKey)
            )
            #expect(bytes.count == 16)
            return try decodeAggregationMembershipMetadata(
                bytes,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes
            )
        }
        #expect(metadata.uniqueMemberCount == 2)
        #expect(metadata.memberScanBytes == expectedBytes)

        try await engine.withTransaction { transaction in
            let retainedA = try await decrementAggregationMembership(
                key: memberA,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
            #expect(!retainedA)
            let removedA = try await decrementAggregationMembership(
                key: memberA,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
            #expect(removedA)
        }

        let remaining = try await engine.withTransaction { transaction in
            let bytes = try #require(
                try await transaction.getValue(for: metadataKey)
            )
            return try decodeAggregationMembershipMetadata(
                bytes,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes
            )
        }
        #expect(remaining.uniqueMemberCount == 1)
        let remainingBytes = try aggregationMemberScanByteCount(for: memberB)
        #expect(remaining.memberScanBytes == remainingBytes)
    }

    @Test("Membership byte limit rejects an unrebuildable ingestion")
    func membershipByteLimitRejectsBeforeCommit() async throws {
        let engine = InMemoryEngine()
        let firstMember = ByteString(Array(repeating: 0x01, count: 32))
        let rejectedMember = ByteString(Array(repeating: 0x02, count: 32))
        let metadataKey = ByteString([0xFE])
        let firstBytes = try aggregationMemberScanByteCount(for: firstMember)
        let maximumBytes = Int(firstBytes * 2 - 1)

        try await engine.withTransaction { transaction in
            _ = try await incrementAggregationMembership(
                key: firstMember,
                metadataKey: metadataKey,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes,
                transaction: transaction
            )
        }

        await #expect(
            throws: AggregationStorageError.membershipByteLimitExceeded(
                maximumBytes
            )
        ) {
            try await engine.withTransaction { transaction in
                _ = try await incrementAggregationMembership(
                    key: rejectedMember,
                    metadataKey: metadataKey,
                    maximumMembers: 10,
                    maximumScanBytes: maximumBytes,
                    transaction: transaction
                )
            }
        }

        try await engine.withTransaction { transaction in
            #expect(
                try await transaction.getValue(for: rejectedMember) == nil
            )
            let bytes = try #require(
                try await transaction.getValue(for: metadataKey)
            )
            let metadata = try decodeAggregationMembershipMetadata(
                bytes,
                maximumMembers: 10,
                maximumScanBytes: maximumBytes
            )
            #expect(metadata.uniqueMemberCount == 1)
            #expect(metadata.memberScanBytes == firstBytes)
        }
    }

    @Test("Membership metadata rejects an impossible fixed frame")
    func membershipMetadataRejectsImpossibleShape() throws {
        let metadata = AggregationMembershipMetadata(
            uniqueMemberCount: 2,
            memberScanBytes: 15
        )
        #expect(
            throws: AggregationStorageError.membershipMetadataTooSmall(
                minimumScanBytes: 16,
                actualScanBytes: 15
            )
        ) {
            _ = try encodeAggregationMembershipMetadata(metadata)
        }

        let bytes = ByteString.copying(count: 16) { destination in
            guard let baseAddress = destination.baseAddress else {
                preconditionFailure("Fixed metadata frame requires storage")
            }
            baseAddress.storeBytes(
                of: Int64(2).littleEndian,
                toByteOffset: 0,
                as: Int64.self
            )
            baseAddress.storeBytes(
                of: Int64(15).littleEndian,
                toByteOffset: MemoryLayout<Int64>.size,
                as: Int64.self
            )
        }
        #expect(
            throws: AggregationStorageError.membershipMetadataTooSmall(
                minimumScanBytes: 16,
                actualScanBytes: 15
            )
        ) {
            _ = try decodeAggregationMembershipMetadata(
                bytes,
                maximumMembers: 10,
                maximumScanBytes: 1_024
            )
        }
    }

    @Test("DISTINCT removes only the final value reference")
    func distinctReferenceCountsAndDeleteRebuild() async throws {
        let engine = InMemoryEngine()
        let maintainer = try makeDistinctMaintainer(
            subspace: Subspace(prefix: Tuple("distinct-delete").pack())
        )
        let first = SketchIndexEntity(
            id: "first",
            group: "calendar",
            distinctValue: "shared",
            numericValue: 10
        )
        let second = SketchIndexEntity(
            id: "second",
            group: "calendar",
            distinctValue: "shared",
            numericValue: 20
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: first,
                transaction: transaction
            )
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: second,
                transaction: transaction
            )
        }
        var result = try await distinctCount(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(result.estimated == 1)

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: first,
                newItem: nil,
                transaction: transaction
            )
        }
        result = try await distinctCount(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(result.estimated == 1)

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: second,
                newItem: nil,
                transaction: transaction
            )
        }
        result = try await distinctCount(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(result.estimated == 0)
    }

    @Test("DISTINCT update removes the old value and old group")
    func distinctUpdateAndGroupMove() async throws {
        let engine = InMemoryEngine()
        let maintainer = try makeDistinctMaintainer(
            subspace: Subspace(prefix: Tuple("distinct-update").pack())
        )
        let old = SketchIndexEntity(
            id: "entity",
            group: "old",
            distinctValue: "first",
            numericValue: 10
        )
        let updated = SketchIndexEntity(
            id: "entity",
            group: "new",
            distinctValue: "second",
            numericValue: 10
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: old,
                transaction: transaction
            )
        }
        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: old,
                newItem: updated,
                transaction: transaction
            )
        }

        let oldResult = try await distinctCount(
            maintainer,
            group: "old",
            engine: engine
        )
        let newResult = try await distinctCount(
            maintainer,
            group: "new",
            engine: engine
        )
        #expect(oldResult.estimated == 0)
        #expect(newResult.estimated == 1)
    }

    @Test("DISTINCT rejects unsupported precision at declaration")
    func distinctRejectsOversizedPrecision() async throws {
        #expect(
            throws: IndexDeclarationError(
                indexName: "distinct",
                validationError: .invalidConfiguration(
                    index: "distinct",
                    reason: "Approximate-distinct precision must be in 4...17"
                )
            )
        ) {
            _ = try makeDistinctMaintainer(
                subspace: Subspace(prefix: Tuple("distinct-precision").pack()),
                precision: 18
            )
        }
    }

    @Test("DISTINCT rejects a malformed binary summary")
    func distinctRejectsMalformedSummary() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("distinct-corrupt").pack()
        )
        let maintainer = try makeDistinctMaintainer(subspace: subspace)
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(try canonicalGroupingTuple(["calendar"]))
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: "value",
            numericValue: 10
        )

        try await engine.withTransaction { transaction in
            try await maintainer.scanItem(
                entity,
                id: Tuple(entity.id),
                transaction: transaction
            )
            try transaction.setValue([0x00, 0x01], for: summaryKey)
        }

        await #expect(throws: DistinctIndexError.corruptedSummary) {
            try await distinctCount(
                maintainer,
                group: "calendar",
                engine: engine
            )
        }
    }

    @Test("DISTINCT never treats a missing summary as an empty group")
    func distinctRejectsMissingSummary() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("distinct-missing-summary").pack()
        )
        let maintainer = try makeDistinctMaintainer(subspace: subspace)
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: "value",
            numericValue: 10
        )
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(try canonicalGroupingTuple(["calendar"]))

        try await engine.withTransaction { transaction in
            try await maintainer.scanItem(
                entity,
                id: Tuple(entity.id),
                transaction: transaction
            )
            try transaction.clear(key: summaryKey)
        }

        await #expect(throws: DistinctIndexError.corruptedSummary) {
            try await distinctCount(
                maintainer,
                group: "calendar",
                engine: engine
            )
        }
        await #expect(throws: DistinctIndexError.corruptedSummary) {
            try await engine.withTransaction { transaction in
                try await maintainer.getAllDistinctCounts(
                    transaction: transaction
                )
            }
        }

        let replacement = SketchIndexEntity(
            id: "replacement",
            group: "calendar",
            distinctValue: "replacement",
            numericValue: 20
        )
        let replacementKeys = try await maintainer.computeIndexKeys(
            for: replacement,
            id: Tuple(replacement.id)
        )
        let replacementKey = try #require(replacementKeys.first)
        try await engine.withTransaction { transaction in
            do {
                try await maintainer.scanItem(
                    replacement,
                    id: Tuple(replacement.id),
                    transaction: transaction
                )
                Issue.record("Missing DISTINCT summary must reject mutation")
            } catch let error as DistinctIndexError {
                #expect(error == .corruptedSummary)
            }
            let stagedValue = try await transaction.getValue(
                for: replacementKey
            )
            #expect(stagedValue == nil)
        }
    }

    @Test("PERCENTILE delete and update rebuild current membership")
    func percentileDeleteAndUpdateRebuild() async throws {
        let engine = InMemoryEngine()
        let maintainer = try makePercentileMaintainer(
            subspace: Subspace(prefix: Tuple("percentile-delete").pack())
        )
        let first = SketchIndexEntity(
            id: "first",
            group: "calendar",
            distinctValue: "first",
            numericValue: 10
        )
        let duplicate = SketchIndexEntity(
            id: "duplicate",
            group: "calendar",
            distinctValue: "duplicate",
            numericValue: 10
        )
        let high = SketchIndexEntity(
            id: "high",
            group: "calendar",
            distinctValue: "high",
            numericValue: 100
        )

        try await engine.withTransaction { transaction in
            try await maintainer.scanItems(
                [
                    (item: first, id: Tuple(first.id)),
                    (item: duplicate, id: Tuple(duplicate.id)),
                    (item: high, id: Tuple(high.id)),
                ],
                transaction: transaction
            )
        }
        var statistics = try await percentileStatistics(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(statistics?.count == 3)
        #expect(statistics?.min == 10)
        #expect(statistics?.max == 100)

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: first,
                newItem: nil,
                transaction: transaction
            )
        }
        statistics = try await percentileStatistics(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(statistics?.count == 2)
        #expect(statistics?.min == 10)

        let updated = SketchIndexEntity(
            id: duplicate.id,
            group: duplicate.group,
            distinctValue: duplicate.distinctValue,
            numericValue: 50
        )
        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: duplicate,
                newItem: updated,
                transaction: transaction
            )
        }
        statistics = try await percentileStatistics(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(statistics?.count == 2)
        #expect(statistics?.min == 50)
        #expect(statistics?.max == 100)

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: updated,
                newItem: nil,
                transaction: transaction
            )
            try await maintainer.updateIndex(
                oldItem: high,
                newItem: nil,
                transaction: transaction
            )
        }
        statistics = try await percentileStatistics(
            maintainer,
            group: "calendar",
            engine: engine
        )
        #expect(statistics == nil)
    }

    @Test("PERCENTILE never treats a missing summary as an empty group")
    func percentileRejectsMissingSummary() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("percentile-missing-summary").pack()
        )
        let maintainer = try makePercentileMaintainer(subspace: subspace)
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: "value",
            numericValue: 10
        )
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(try canonicalGroupingTuple(["calendar"]))

        try await engine.withTransaction { transaction in
            try await maintainer.scanItem(
                entity,
                id: Tuple(entity.id),
                transaction: transaction
            )
            try transaction.clear(key: summaryKey)
        }

        await #expect(throws: PercentileIndexError.corruptedSummary) {
            try await percentileStatistics(
                maintainer,
                group: "calendar",
                engine: engine
            )
        }
        await #expect(throws: PercentileIndexError.corruptedSummary) {
            try await engine.withTransaction { transaction in
                try await maintainer.getAllPercentiles(
                    percentiles: [0.5],
                    transaction: transaction
                )
            }
        }

        let replacement = SketchIndexEntity(
            id: "replacement",
            group: "calendar",
            distinctValue: "replacement",
            numericValue: 20
        )
        let replacementKeys = try await maintainer.computeIndexKeys(
            for: replacement,
            id: Tuple(replacement.id)
        )
        let replacementKey = try #require(replacementKeys.first)
        try await engine.withTransaction { transaction in
            do {
                try await maintainer.scanItem(
                    replacement,
                    id: Tuple(replacement.id),
                    transaction: transaction
                )
                Issue.record("Missing percentile summary must reject mutation")
            } catch let error as PercentileIndexError {
                #expect(error == .corruptedSummary)
            }
            let stagedValue = try await transaction.getValue(
                for: replacementKey
            )
            #expect(stagedValue == nil)
        }
    }

    @Test("PERCENTILE rejects invalid range and non-finite mutation")
    func percentileRejectsInvalidInputsBeforeMutation() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("percentile-invalid").pack()
        )
        let maintainer = try makePercentileMaintainer(subspace: subspace)

        await #expect(
            throws: PercentileIndexError.invalidPercentile(-0.1)
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.getPercentile(
                    percentile: -0.1,
                    groupingValues: ["calendar"],
                    transaction: transaction
                )
            }
        }
        await #expect(
            throws: PercentileIndexError.invalidPercentile(1.1)
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.getPercentile(
                    percentile: 1.1,
                    groupingValues: ["calendar"],
                    transaction: transaction
                )
            }
        }
        await #expect(throws: PercentileIndexError.self) {
            try await engine.withTransaction { transaction in
                try await maintainer.getPercentile(
                    percentile: .nan,
                    groupingValues: ["calendar"],
                    transaction: transaction
                )
            }
        }

        let invalid = SketchIndexEntity(
            id: "invalid",
            group: "calendar",
            distinctValue: "invalid",
            numericValue: .infinity
        )
        await #expect(
            throws: PercentileIndexError.invalidNumericValue(
                fieldName: "percentile"
            )
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: invalid,
                    transaction: transaction
                )
            }
        }

        let transaction = try engine.createTransaction()
        let range = subspace.range()
        let rows = try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end)
        )
        #expect(rows.isEmpty)
        try await transaction.cancel()
    }
}

private func makeDistinctMaintainer(
    subspace: Subspace,
    precision: Int = 14
) throws -> DistinctIndexMaintainer<SketchIndexEntity> {
    let index = try ResolvedIndex(
        for: SketchIndexEntity.self,
        name: "distinct",
        definition: distinctIndexDefinition(
            groupingFields: [
                FieldIdentity(name: "group", number: 2)
            ],
            valueField: FieldIdentity(name: "distinctValue", number: 3),
            precision: precision
        ),
        rootExpression: ConcatenateKeyExpression(children: [
            FieldKeyExpression(fieldName: "group"),
            FieldKeyExpression(fieldName: "distinctValue"),
        ]),
        itemTypes: [SketchIndexEntity.persistableType]
    )
    return DistinctIndexMaintainer(
        index: index,
        subspace: subspace,
        idExpression: FieldKeyExpression(fieldName: "id"),
        precision: precision
    )
}

private func makePercentileMaintainer(
    subspace: Subspace
) throws -> PercentileIndexMaintainer<SketchIndexEntity> {
    let index = try ResolvedIndex(
        for: SketchIndexEntity.self,
        name: "percentile",
        definition: percentileIndexDefinition(
            groupingFields: [
                FieldIdentity(name: "group", number: 2)
            ],
            valueField: FieldIdentity(name: "numericValue", number: 4),
            compression: 100
        ),
        rootExpression: ConcatenateKeyExpression(children: [
            FieldKeyExpression(fieldName: "group"),
            FieldKeyExpression(fieldName: "numericValue"),
        ]),
        itemTypes: [SketchIndexEntity.persistableType]
    )
    return PercentileIndexMaintainer(
        index: index,
        subspace: subspace,
        idExpression: FieldKeyExpression(fieldName: "id"),
        compression: 100
    )
}

private func distinctCount(
    _ maintainer: DistinctIndexMaintainer<SketchIndexEntity>,
    group: String,
    engine: InMemoryEngine
) async throws -> (estimated: Int64, errorRate: Double) {
    try await engine.withTransaction { transaction in
        try await maintainer.getDistinctCount(
            groupingValues: [.string(group)],
            transaction: transaction
        )
    }
}

private func percentileStatistics(
    _ maintainer: PercentileIndexMaintainer<SketchIndexEntity>,
    group: String,
    engine: InMemoryEngine
) async throws -> (
    count: Int64,
    min: Double,
    max: Double,
    median: Double
)? {
    try await engine.withTransaction { transaction in
        try await maintainer.getStatistics(
            groupingValues: [.string(group)],
            transaction: transaction
        )
    }
}

private func canonicalGroupingTuple(
    _ values: [FieldValue]
) throws -> Tuple {
    try Tuple(FieldValue.toTupleElements(values))
}

@Persistable
private struct SketchIndexEntity {
    let id: String
    let group: String
    let distinctValue: String
    let numericValue: Double
}
