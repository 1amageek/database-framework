@testable import AggregationIndex
import Core
import DatabaseValue
import DatabaseEngine
import StorageKit
import Testing

@Suite("Delete-capable sketch indexes")
struct DeleteCapableSketchIndexTests {
    @Test("Membership metadata tracks unique keys and exact scan bytes")
    func membershipMetadataTracksRebuildCost() async throws {
        let engine = InMemoryEngine()
        let memberA = Bytes([0x01, 0x02, 0x03])
        let memberB = Bytes([0x04, 0x05, 0x06, 0x07])
        let metadataKey = Bytes([0xFF])
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
        let firstMember = Bytes(Array(repeating: 0x01, count: 32))
        let rejectedMember = Bytes(Array(repeating: 0x02, count: 32))
        let metadataKey = Bytes([0xFE])
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

        let bytes = Bytes.copying(count: 16) { destination in
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

    @Test("DISTINCT removes only the final canonical value reference")
    func distinctReferenceCountsAndDeleteRebuild() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeDistinctMaintainer(
            subspace: Subspace(prefix: Tuple("distinct-delete").pack())
        )
        let signed = SketchIndexEntity(
            id: "signed",
            group: "calendar",
            distinctValue: .int64(1),
            numericValue: 10
        )
        let floating = SketchIndexEntity(
            id: "floating",
            group: "calendar",
            distinctValue: .double(1),
            numericValue: 20
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: signed,
                transaction: transaction
            )
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: floating,
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
                oldItem: signed,
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
                oldItem: floating,
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
        let maintainer = makeDistinctMaintainer(
            subspace: Subspace(prefix: Tuple("distinct-update").pack())
        )
        let old = SketchIndexEntity(
            id: "entity",
            group: "old",
            distinctValue: .string("first"),
            numericValue: 10
        )
        let updated = SketchIndexEntity(
            id: "entity",
            group: "new",
            distinctValue: .string("second"),
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

    @Test("DISTINCT rejects unsupported persisted precision")
    func distinctRejectsOversizedPrecision() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeDistinctMaintainer(
            subspace: Subspace(prefix: Tuple("distinct-precision").pack()),
            precision: 18
        )
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: .string("value"),
            numericValue: 10
        )

        await #expect(throws: DistinctIndexError.invalidPrecision(18)) {
            try await engine.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }
    }

    @Test("DISTINCT rejects a malformed binary summary")
    func distinctRejectsMalformedSummary() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("distinct-corrupt").pack()
        )
        let maintainer = makeDistinctMaintainer(subspace: subspace)
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(Tuple("calendar"))
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: .string("value"),
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
        let maintainer = makeDistinctMaintainer(subspace: subspace)
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: .string("value"),
            numericValue: 10
        )
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(Tuple("calendar"))

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
            distinctValue: .string("replacement"),
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
        let maintainer = makePercentileMaintainer(
            subspace: Subspace(prefix: Tuple("percentile-delete").pack())
        )
        let first = SketchIndexEntity(
            id: "first",
            group: "calendar",
            distinctValue: .string("first"),
            numericValue: 10
        )
        let duplicate = SketchIndexEntity(
            id: "duplicate",
            group: "calendar",
            distinctValue: .string("duplicate"),
            numericValue: 10
        )
        let high = SketchIndexEntity(
            id: "high",
            group: "calendar",
            distinctValue: .string("high"),
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
        let maintainer = makePercentileMaintainer(subspace: subspace)
        let entity = SketchIndexEntity(
            id: "entity",
            group: "calendar",
            distinctValue: .string("value"),
            numericValue: 10
        )
        let summaryKey = subspace
            .subspace(Int64(1))
            .pack(Tuple("calendar"))

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
            distinctValue: .string("replacement"),
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
        let maintainer = makePercentileMaintainer(subspace: subspace)

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
            distinctValue: .string("invalid"),
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
) -> DistinctIndexMaintainer<SketchIndexEntity> {
    let index = Index(
        name: "distinct",
        kind: DistinctIndexKind<SketchIndexEntity>(
            groupBy: [\.group],
            value: \.distinctValue,
            precision: precision
        ),
        rootExpression: ConcatenateKeyExpression(children: [
            FieldKeyExpression(fieldName: "group"),
            FieldKeyExpression(fieldName: "distinctValue"),
        ]),
        subspaceKey: "distinct",
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
) -> PercentileIndexMaintainer<SketchIndexEntity> {
    let index = Index(
        name: "percentile",
        kind: PercentileIndexKind<SketchIndexEntity, Double>(
            groupBy: [\.group],
            value: \.numericValue
        ),
        rootExpression: ConcatenateKeyExpression(children: [
            FieldKeyExpression(fieldName: "group"),
            FieldKeyExpression(fieldName: "numericValue"),
        ]),
        subspaceKey: "percentile",
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
            groupingValues: [group],
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
            groupingValues: [group],
            transaction: transaction
        )
    }
}

private struct SketchIndexEntity: Persistable {
    typealias ID = String

    let id: String
    let group: String
    let distinctValue: FieldValue
    let numericValue: Double

    static let persistableType = "SketchIndexEntity"
    static let allFields = [
        "id",
        "group",
        "distinctValue",
        "numericValue",
    ]
    static let indexDescriptors: [IndexDescriptor] = []

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": id
        case "group": group
        case "distinctValue": distinctValue
        case "numericValue": numericValue
        default: nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<SketchIndexEntity, Value>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<SketchIndexEntity>)
    }

    static func fieldName(
        for keyPath: PartialKeyPath<SketchIndexEntity>
    ) -> String {
        switch keyPath {
        case \SketchIndexEntity.id: "id"
        case \SketchIndexEntity.group: "group"
        case \SketchIndexEntity.distinctValue: "distinctValue"
        case \SketchIndexEntity.numericValue: "numericValue"
        default: String(describing: keyPath)
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<SketchIndexEntity> else {
            return String(describing: keyPath)
        }
        return fieldName(for: keyPath)
    }
}
