import Core
import DatabaseValue
import DatabaseEngine
import DatabaseRuntime
import StorageKit
import Testing
@testable import AggregationIndex

@Suite("Canonical aggregation reduction semantics")
struct CanonicalAggregationReducerTests {
    @Test("empty global aggregate returns one canonical reduced row")
    func emptyGlobalAggregateReturnsOneReducedRow() async throws {
        let context = try await makeEmptyQueryContext()

        let results = try await context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "entityCount")
            .sum(\.value, as: "sum")
            .avg(\.value, as: "average")
            .min(\.value, as: "minimum")
            .max(\.value, as: "maximum")
            .percentile(\.value, p: 0.5, as: "median")
            .execute()

        assertCanonicalEmptyGlobalResult(results)
    }

    @Test("empty global aggregate returns one canonical row from indexes")
    func emptyGlobalAggregateReturnsOneIndexedRow() async throws {
        let context = try await makeEmptyQueryContext()

        let query = context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "entityCount")
            .sum(\.value, as: "sum")
            .avg(\.value, as: "average")
            .min(\.value, as: "minimum")
            .max(\.value, as: "maximum")
        Self.assertAllIndexBacked(
            try query.determineExecutionStrategies(),
            names: ["entityCount", "sum", "average", "minimum", "maximum"]
        )

        let results = try await query.execute()

        assertCanonicalEmptyGlobalResult(results)
    }

    @Test("non-empty global aggregate indexes accept zero grouping fields")
    func nonEmptyGlobalIndexesAcceptZeroGroupingFields() async throws {
        let schema = Schema([EmptyGlobalAggregationEntity.self])
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [EmptyGlobalAggregationEntity.self, IndexedGlobalSketchEntity.self]),
            security: .disabled
        )
        let context = container.newContext()
        try context.insert(EmptyGlobalAggregationEntity(value: 4))
        try await context.save()

        let query = context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "count")
            .sum(\.value, as: "sum")
            .avg(\.value, as: "average")
            .min(\.value, as: "minimum")
            .max(\.value, as: "maximum")
        Self.assertAllIndexBacked(
            try query.determineExecutionStrategies(),
            names: ["count", "sum", "average", "minimum", "maximum"]
        )

        let results = try await query.execute()

        let result = try #require(results.first)
        #expect(results.count == 1)
        #expect(result.groupKey.isEmpty)
        #expect(result.aggregateInt64("count") == 1)
        #expect(result.aggregateInt64("sum") == 4)
        #expect(result.aggregateInt64("average") == 4)
        #expect(result.aggregateInt64("minimum") == 4)
        #expect(result.aggregateInt64("maximum") == 4)
    }

    @Test("empty global sketch indexes preserve canonical results")
    func emptyGlobalSketchIndexesPreserveCanonicalResults() async throws {
        let context = try await makeGlobalSketchQueryContext()
        let query = context
            .aggregate(IndexedGlobalSketchEntity.self)
            .distinct(\.value, as: "distinct")
            .percentile(\.value, p: 0.5, as: "median")
        Self.assertAllIndexBacked(
            try query.determineExecutionStrategies(),
            names: ["distinct", "median"]
        )

        let results = try await query.execute()
        let result = try #require(results.first)
        #expect(results.count == 1)
        #expect(result.groupKey.isEmpty)
        #expect(result.aggregateInt64("distinct") == 0)
        assertStoredNull(result.aggregates, name: "median")
    }

    @Test("non-empty global sketch indexes are read outside subspace ranges")
    func nonEmptyGlobalSketchIndexesAreReadDirectly() async throws {
        let context = try await makeGlobalSketchQueryContext()
        try context.insert(IndexedGlobalSketchEntity(value: 10))
        try context.insert(IndexedGlobalSketchEntity(value: 20))
        try context.insert(IndexedGlobalSketchEntity(value: 20))
        try await context.save()

        let query = context
            .aggregate(IndexedGlobalSketchEntity.self)
            .distinct(\.value, as: "distinct")
            .percentile(\.value, p: 0.5, as: "median")
        Self.assertAllIndexBacked(
            try query.determineExecutionStrategies(),
            names: ["distinct", "median"]
        )

        let results = try await query.execute()
        let result = try #require(results.first)
        #expect(results.count == 1)
        #expect(result.groupKey.isEmpty)
        #expect(result.aggregateInt64("distinct") == 2)
        #expect(try result.aggregateDouble("median") == 20)
    }

    @Test("duplicate aggregate result names fail before execution")
    func duplicateAggregateNamesFail() async throws {
        let context = try await makeEmptyQueryContext()

        await #expect(
            throws: AggregationQueryError.duplicateAggregationName("value")
        ) {
            try await context
                .aggregate(EmptyGlobalAggregationEntity.self)
                .count(as: "value")
                .sum(\.value, as: "value")
                .execute()
        }
    }

    @Test("unsigned sum preserves integers above Double's exact range")
    func unsignedSumPreservesPrecision() throws {
        let result = try CanonicalAggregationReducer.sum(
            values: [
                .uint64(9_007_199_254_740_993),
                .uint64(2),
            ],
            field: "value"
        )

        #expect(result == .uint64(9_007_199_254_740_995))
    }

    @Test("aggregate Double access rejects lossy integer conversion")
    func aggregateDoubleRejectsLossyIntegerConversion() {
        let value = FieldValue.uint64(9_007_199_254_740_993)
        let result = AggregateResult<EmptyGlobalAggregationEntity>(
            groupKey: [:],
            aggregates: ["sum": value]
        )

        #expect(
            throws: AggregateResultError.notExactlyRepresentableAsDouble(
                name: "sum",
                value: value
            )
        ) {
            try result.aggregateDouble("sum")
        }
    }

    @Test("signed sum overflow is a typed failure")
    func signedSumOverflowFails() {
        #expect(
            throws: AggregationQueryError.numericOverflow(
                operation: "sum",
                field: "value"
            )
        ) {
            try CanonicalAggregationReducer.sum(
                values: [.int64(.max), .int64(1)],
                field: "value"
            )
        }
    }

    @Test("integer average keeps an exact UInt64 result")
    func unsignedAveragePreservesPrecision() throws {
        let result = try CanonicalAggregationReducer.average(
            values: [
                .uint64(UInt64.max),
                .uint64(UInt64.max - 2),
            ],
            field: "value"
        )

        #expect(result == .uint64(UInt64.max - 1))
    }

    @Test("unrepresentable integer average is not rounded")
    func unrepresentableAverageFails() {
        #expect(
            throws: AggregationQueryError.resultNotRepresentable(
                operation: "average",
                field: "value"
            )
        ) {
            try CanonicalAggregationReducer.average(
                values: [
                    .uint64(UInt64.max),
                    .uint64(UInt64.max - 1),
                ],
                field: "value"
            )
        }
    }

    @Test("empty and null-only aggregates use SQL null semantics")
    func emptyAggregatesReturnNil() throws {
        #expect(try CanonicalAggregationReducer.sum(values: [], field: "value") == nil)
        #expect(
            try CanonicalAggregationReducer.average(
                values: [.null, .null],
                field: "value"
            ) == nil
        )
        #expect(try CanonicalAggregationReducer.minimum(values: [], field: "value") == nil)
        #expect(
            try CanonicalAggregationReducer.maximum(
                values: [.null],
                field: "value"
            ) == nil
        )
    }

    @Test("non-numeric and non-finite inputs fail explicitly")
    func invalidNumericInputsFail() {
        #expect(
            throws: AggregationQueryError.nonNumericValue(
                field: "value",
                value: .string("invalid")
            )
        ) {
            try CanonicalAggregationReducer.sum(
                values: [.string("invalid")],
                field: "value"
            )
        }
        #expect(
            throws: AggregationQueryError.nonFiniteNumericValue(field: "value")
        ) {
            try CanonicalAggregationReducer.average(
                values: [.double(.infinity)],
                field: "value"
            )
        }
    }

    @Test("minimum compares large integers without Double conversion")
    func minimumPreservesIntegerOrdering() throws {
        let result = try CanonicalAggregationReducer.minimum(
            values: [
                .uint64(9_007_199_254_740_993),
                .uint64(9_007_199_254_740_992),
            ],
            field: "value"
        )

        #expect(result == .uint64(9_007_199_254_740_992))
    }

    @Test("incomparable values are not ordered by enum case")
    func incomparableValuesFail() {
        #expect(
            throws: AggregationQueryError.incomparableValues(
                field: "value",
                lhs: .bool(false),
                rhs: .string("value")
            )
        ) {
            try CanonicalAggregationReducer.minimum(
                values: [.string("value"), .bool(false)],
                field: "value"
            )
        }
    }

    @Test("percentile endpoints retain exact integer values")
    func percentileEndpointPreservesInteger() throws {
        var values: [FieldValue] = [
            .uint64(UInt64.max),
            .uint64(UInt64.max - 1),
        ]

        let result = try CanonicalAggregationReducer.percentile(
            values: &values,
            percentile: 1,
            field: "value"
        )

        #expect(result == .uint64(UInt64.max))
    }

    @Test("percentile rejects invalid configuration and lossy interpolation")
    func invalidPercentileFails() {
        var outOfRangeValues: [FieldValue] = [.int64(1)]
        #expect(throws: AggregationQueryError.invalidPercentile(1.1)) {
            try CanonicalAggregationReducer.percentile(
                values: &outOfRangeValues,
                percentile: 1.1,
                field: "value"
            )
        }

        var impreciseValues: [FieldValue] = [
            .uint64(UInt64.max - 1),
            .uint64(UInt64.max),
        ]
        #expect(
            throws: AggregationQueryError.resultNotRepresentable(
                operation: "percentile",
                field: "value"
            )
        ) {
            try CanonicalAggregationReducer.percentile(
                values: &impreciseValues,
                percentile: 0.5,
                field: "value"
            )
        }
    }

    @Test("unsupported dynamic values produce a typed conversion error")
    func unsupportedValueFailsConversion() {
        #expect(throws: AggregationQueryError.self) {
            try CanonicalAggregationReducer.fieldValue(
                UnsupportedAggregationValue(),
                field: "value"
            )
        }
    }

    @Test("distinct ignores null and uses canonical numeric identity")
    func distinctUsesCanonicalIdentity() throws {
        let entities = [
            AggregationReducerEntity(id: "1", group: nil, value: .int64(1)),
            AggregationReducerEntity(id: "2", group: nil, value: .uint64(1)),
            AggregationReducerEntity(id: "3", group: nil, value: .double(1)),
            AggregationReducerEntity(id: "4", group: nil, value: nil),
        ]

        let result = try CanonicalAggregationReducer.aggregate(
            items: entities,
            aggregation: .distinct(field: "value")
        )
        let identity = try CanonicalAggregationReducer.groupIdentity(
            item: entities[0],
            fields: ["group"]
        )

        #expect(result == .int64(1))
        #expect(identity == [.null])
    }

    @Test("unknown fields are not silently treated as null")
    func unknownFieldsFailExplicitly() {
        let entity = AggregationReducerEntity(
            id: "1",
            group: nil,
            value: nil
        )

        #expect(throws: AggregationQueryError.invalidField("missing")) {
            try CanonicalAggregationReducer.groupIdentity(
                item: entity,
                fields: ["missing"]
            )
        }
    }

    @Test("tuple extraction rejects an invalid element range")
    func tupleExtractionRejectsInvalidRange() {
        let tuple = Tuple("group")

        do {
            _ = try tuple.elements(in: 0..<2)
            Issue.record("Invalid tuple element range must fail")
        } catch TupleError.invalidElementRange(
            let lowerBound,
            let upperBound,
            let count
        ) {
            #expect(lowerBound == 0)
            #expect(upperBound == 2)
            #expect(count == 1)
        } catch {
            Issue.record("Unexpected tuple extraction error: \(error)")
        }
    }

    @Test("minimum index rejects a malformed aggregate value")
    func minimumIndexRejectsMalformedAggregateValue() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("malformed-minimum").pack())
        let index = Index(
            name: "minimum_by_group",
            kind: MinIndexKind<AggregationTupleEntity, Int64>(
                groupBy: [\.group],
                value: \.number
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "number"),
            ]),
            subspaceKey: "minimum_by_group",
            itemTypes: [AggregationTupleEntity.persistableType]
        )
        let maintainer = MinIndexMaintainer<AggregationTupleEntity, Int64>(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let transaction = try engine.createTransaction()
        let malformedKey = subspace
            .subspace(Int64(1))
            .pack(Tuple("group"))
        try transaction.setValue(Tuple(Int64(42)).pack(), for: malformedKey)

        await #expect(throws: IndexError.self) {
            try await maintainer.getAllMins(transaction: transaction)
        }
    }

    @Test("integer index reads do not round through Double")
    func integerIndexReadRejectsLossyDoubleConversion() {
        let index = Index(
            name: "sum_by_group",
            kind: SumIndexKind<AggregationTupleEntity, Int64>(
                groupBy: [\.group],
                value: \.number
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "number"),
            ]),
            subspaceKey: "sum_by_group",
            itemTypes: [AggregationTupleEntity.persistableType]
        )
        let maintainer = SumIndexMaintainer<AggregationTupleEntity, Int64>(
            index: index,
            subspace: Subspace(prefix: Tuple("lossless-sum").pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        #expect(
            throws: AggregationStorageError
                .integerNotExactlyRepresentableAsDouble(.max)
        ) {
            try maintainer.readNumericValue(
                ByteConversion.int64ToBytes(.max)
            )
        }
    }

    @Test("floating sum storage preserves sub-micro values")
    func floatingSumStorageDoesNotQuantize() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeSumMaintainer(
            valueType: Double.self,
            name: "raw-double-sum"
        )
        let entity = FloatingAggregationEntity(
            id: "1",
            group: "group",
            value: 0.000_000_4
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }
        let sum = try await engine.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: ["group"],
                transaction: transaction
            )
        }

        #expect(sum == .double(entity.value))
    }

    @Test("materialized floating aggregates preserve compensated contributions")
    func materializedFloatingAggregatesUseCompensatedState() async throws {
        let engine = InMemoryEngine()
        let sumMaintainer = makeSumMaintainer(
            valueType: Double.self,
            name: "compensated-double-sum"
        )
        let averageMaintainer = makeAverageMaintainer(
            valueType: Double.self,
            name: "compensated-double-average"
        )
        let entities = [
            FloatingAggregationEntity(
                id: "large-positive",
                group: "group",
                value: 1.0e16
            ),
            FloatingAggregationEntity(
                id: "unit",
                group: "group",
                value: 1
            ),
            FloatingAggregationEntity(
                id: "large-negative",
                group: "group",
                value: -1.0e16
            ),
        ]

        try await engine.withTransaction { transaction in
            for entity in entities {
                try await sumMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
                try await averageMaintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }

        let initial = try await engine.withTransaction { transaction in
            let sum = try await sumMaintainer.getSum(
                groupingValues: ["group"],
                transaction: transaction
            )
            let average = try await averageMaintainer.getAverage(
                groupingValues: ["group"],
                transaction: transaction
            )
            return (sum, average)
        }
        #expect(initial.0 == .double(1))
        #expect(initial.1.count == 3)
        #expect(initial.1.average == .double(1.0 / 3.0))

        try await engine.withTransaction { transaction in
            try await sumMaintainer.updateIndex(
                oldItem: entities[1],
                newItem: nil,
                transaction: transaction
            )
            try await averageMaintainer.updateIndex(
                oldItem: entities[1],
                newItem: nil,
                transaction: transaction
            )
        }

        let afterDelete = try await engine.withTransaction { transaction in
            let sum = try await sumMaintainer.getSum(
                groupingValues: ["group"],
                transaction: transaction
            )
            let average = try await averageMaintainer.getAverage(
                groupingValues: ["group"],
                transaction: transaction
            )
            return (sum, average)
        }
        #expect(afterDelete.0 == .double(0))
        #expect(afterDelete.1.count == 2)
        #expect(afterDelete.1.average == .double(0))
    }

    @Test("floating aggregate storage rejects noncanonical payload width")
    func floatingAggregateStorageRejectsLegacyWidth() {
        let maintainer = makeSumMaintainer(
            valueType: Double.self,
            name: "strict-double-state"
        )
        let legacyPayload = ByteConversion.uint64ToBytes(1.0.bitPattern)

        #expect(
            throws: AggregationStorageError
                .invalidFloatingPointStateByteCount(8)
        ) {
            try maintainer.readNumericValue(legacyPayload)
        }
    }

    @Test("unsigned sum storage preserves UInt64 maximum")
    func unsignedSumStoragePreservesUInt64Maximum() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeSumMaintainer(
            valueType: UInt64.self,
            name: "uint64-sum"
        )
        let entity = UnsignedAggregationEntity(
            id: "1",
            group: "group",
            value: .max
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }
        let sum = try await engine.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: ["group"],
                transaction: transaction
            )
        }

        #expect(sum == .uint64(.max))
        await #expect(
            throws: AggregationStorageError
                .unsignedIntegerNotExactlyRepresentableAsDouble(.max)
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.getSumAsDouble(
                    groupingValues: ["group"],
                    transaction: transaction
                )
            }
        }
    }

    @Test("deleting the final sum member removes the group")
    func deletingFinalSumMemberRemovesGroup() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeSumMaintainer(
            valueType: UInt64.self,
            name: "sum-membership"
        )
        let entity = UnsignedAggregationEntity(
            id: "1",
            group: "group",
            value: 42
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }
        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: entity,
                newItem: nil,
                transaction: transaction
            )
        }
        let sums = try await engine.withTransaction { transaction in
            try await maintainer.getAllSums(transaction: transaction)
        }

        #expect(sums.isEmpty)
    }

    @Test("deleting the final count member removes the stored group")
    func deletingFinalCountMemberRemovesStoredGroup() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeCountMaintainer(name: "count-membership")
        let entity = AggregationTupleEntity(
            id: "1",
            group: "group",
            number: 42
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }
        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: entity,
                newItem: nil,
                transaction: transaction
            )
        }
        let result = try await engine.withTransaction { transaction in
            let count = try await maintainer.getCount(
                groupingValues: ["group"],
                transaction: transaction
            )
            let groups = try await maintainer.getAllCounts(
                transaction: transaction
            )
            return (count, groups)
        }

        #expect(result.0 == 0)
        #expect(result.1.isEmpty)
    }

    @Test("materialized zero count is rejected as index corruption")
    func materializedZeroCountIsRejected() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeCountMaintainer(name: "invalid-zero-count")
        let key = try maintainer.buildGroupingKey(["group"])

        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteConversion.int64ToBytes(0),
                for: key
            )
        }

        await #expect(
            throws: AggregationStorageError.nonPositiveStoredCount(0)
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.getAllCounts(transaction: transaction)
            }
        }
    }

    @Test("unsigned average storage preserves an exact integral result")
    func unsignedAverageStoragePreservesExactResult() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeAverageMaintainer(
            valueType: UInt64.self,
            name: "uint64-average"
        )
        let entities = [
            UnsignedAggregationEntity(
                id: "1",
                group: "group",
                value: .max
            ),
            UnsignedAggregationEntity(
                id: "2",
                group: "group",
                value: .max
            ),
        ]

        try await engine.withTransaction { transaction in
            for entity in entities {
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }
        let result = try await engine.withTransaction { transaction in
            try await maintainer.getAverage(
                groupingValues: ["group"],
                transaction: transaction
            )
        }

        #expect(result.count == 2)
        #expect(result.average == .uint64(.max))
        await #expect(
            throws: AggregationStorageError
                .unsignedIntegerNotExactlyRepresentableAsDouble(.max)
        ) {
            try await engine.withTransaction { transaction in
                try await maintainer.getAverageAsDouble(
                    groupingValues: ["group"],
                    transaction: transaction
                )
            }
        }
    }

    @Test("unsigned sum overflow rolls back the transaction")
    func unsignedSumOverflowRollsBack() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeSumMaintainer(
            valueType: UInt64.self,
            name: "uint64-overflow"
        )
        let maximum = UnsignedAggregationEntity(
            id: "maximum",
            group: "group",
            value: .max
        )
        let one = UnsignedAggregationEntity(
            id: "one",
            group: "group",
            value: 1
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: maximum,
                transaction: transaction
            )
        }
        await #expect(throws: AggregationStorageError.integerOverflow) {
            try await engine.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: one,
                    transaction: transaction
                )
            }
        }
        let result = try await engine.withTransaction { transaction in
            try await maintainer.getAllSums(transaction: transaction)
        }

        #expect(result.count == 1)
        #expect(result.first?.sum == .uint64(.max))
    }

    @Test("numeric aggregate mutations reject orphaned sum/count state")
    func numericAggregateRejectsOrphanedState() async throws {
        let engine = InMemoryEngine()
        let name = "orphaned-sum"
        let maintainer = makeSumMaintainer(
            valueType: Int64.self,
            name: name
        )
        let subspace = Subspace(prefix: Tuple(name).pack())
        let grouping: [any TupleElement] = ["group"]
        let sumKey = subspace.pack(
            elements: grouping,
            appending: "sum"
        )
        let entity = SignedAggregationEntity(
            id: "entity",
            group: "group",
            value: 2
        )

        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteConversion.int64ToBytes(1),
                for: sumKey
            )
        }

        await #expect(throws: IndexError.self) {
            try await engine.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }
        await #expect(throws: IndexError.self) {
            try await engine.withTransaction { transaction in
                try await maintainer.getSum(
                    groupingValues: ["group"],
                    transaction: transaction
                )
            }
        }
    }

    @Test("deleting Int64 minimum does not negate the contribution")
    func deletingSignedMinimumIsChecked() async throws {
        let engine = InMemoryEngine()
        let maintainer = makeSumMaintainer(
            valueType: Int64.self,
            name: "int64-minimum-delete"
        )
        let entity = SignedAggregationEntity(
            id: "minimum",
            group: "group",
            value: .min
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }
        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: entity,
                newItem: nil,
                transaction: transaction
            )
        }
        let result = try await engine.withTransaction { transaction in
            try await maintainer.getAllSums(transaction: transaction)
        }

        #expect(result.isEmpty)
    }

    @Test("unsigned minimum and maximum indexes preserve UInt64 range")
    func unsignedExtremaIndexesPreserveRange() async throws {
        let engine = InMemoryEngine()
        let minimum = makeMinimumMaintainer(
            valueType: UInt64.self,
            name: "uint64-minimum"
        )
        let maximum = makeMaximumMaintainer(
            valueType: UInt64.self,
            name: "uint64-maximum"
        )
        let entities = [
            UnsignedAggregationEntity(
                id: "maximum",
                group: "group",
                value: .max
            ),
            UnsignedAggregationEntity(
                id: "previous",
                group: "group",
                value: .max - 1
            ),
        ]

        try await engine.withTransaction { transaction in
            for entity in entities {
                try await minimum.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
                try await maximum.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }
        let minimums = try await engine.withTransaction { transaction in
            try await minimum.getAllMins(transaction: transaction)
        }
        let maximums = try await engine.withTransaction { transaction in
            try await maximum.getAllMaxs(transaction: transaction)
        }

        #expect(minimums.first?.min == UInt64.max - 1)
        #expect(maximums.first?.max == UInt64.max)
    }

    private func makeSumMaintainer<Value: IndexNumericValue>(
        valueType _: Value.Type,
        name: String
    ) -> SumIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = Index(
            name: name,
            kind: SumIndexKind<AggregationValueEntity<Value>, Value>(
                groupBy: [\.group],
                value: \.value
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: name,
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return SumIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeEmptyQueryContext() async throws -> DatabaseContext {
        let schema = Schema([EmptyGlobalAggregationEntity.self])
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [EmptyGlobalAggregationEntity.self, IndexedGlobalSketchEntity.self]),
            security: .disabled
        )
        return container.newContext()
    }

    private func makeGlobalSketchQueryContext() async throws -> DatabaseContext {
        let schema = Schema([IndexedGlobalSketchEntity.self])
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [EmptyGlobalAggregationEntity.self, IndexedGlobalSketchEntity.self]),
            security: .disabled
        )
        return container.newContext()
    }

    private func assertCanonicalEmptyGlobalResult(
        _ results: [AggregateResult<EmptyGlobalAggregationEntity>]
    ) {
        #expect(results.count == 1)
        guard let result = results.first else {
            Issue.record("Global aggregate must contain one result")
            return
        }
        #expect(result.groupKey.isEmpty)
        #expect(result.aggregates.keys.contains("entityCount"))
        #expect(result.aggregates.keys.contains("sum"))
        #expect(result.aggregates.keys.contains("average"))
        #expect(result.aggregates.keys.contains("minimum"))
        #expect(result.aggregates.keys.contains("maximum"))

        guard let storedCount = result.aggregates["entityCount"],
              let count = storedCount else {
            Issue.record("COUNT must be materialized in the global result")
            return
        }
        #expect(count == .int64(0))
        assertStoredNull(result.aggregates, name: "sum")
        assertStoredNull(result.aggregates, name: "average")
        assertStoredNull(result.aggregates, name: "minimum")
        assertStoredNull(result.aggregates, name: "maximum")
        if result.aggregates.keys.contains("median") {
            assertStoredNull(result.aggregates, name: "median")
        }
    }

    private func assertStoredNull(
        _ aggregates: [String: FieldValue?],
        name: String
    ) {
        guard let storedValue = aggregates[name] else {
            Issue.record("Aggregate '\(name)' must be present with a null value")
            return
        }
        #expect(storedValue == nil)
    }

    private static func assertAllIndexBacked<Entity: Persistable>(
        _ strategies: [String: AggregationQueryBuilder<Entity>.ExecutionStrategy],
        names: [String]
    ) {
        #expect(strategies.count == names.count)
        for name in names {
            guard let strategy = strategies[name] else {
                Issue.record("Aggregation '\(name)' is missing an execution strategy")
                continue
            }
            guard case .useIndex = strategy else {
                Issue.record("Aggregation '\(name)' must use its declared index")
                continue
            }
        }
    }

    private func makeCountMaintainer(
        name: String
    ) -> CountIndexMaintainer<AggregationTupleEntity> {
        let index = Index(
            name: name,
            kind: CountIndexKind<AggregationTupleEntity>(groupBy: [\.group]),
            rootExpression: FieldKeyExpression(fieldName: "group"),
            subspaceKey: name,
            itemTypes: [AggregationTupleEntity.persistableType]
        )
        return CountIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeAverageMaintainer<Value: IndexNumericValue>(
        valueType _: Value.Type,
        name: String
    ) -> AverageIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = Index(
            name: name,
            kind: AverageIndexKind<AggregationValueEntity<Value>, Value>(
                groupBy: [\.group],
                value: \.value
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: name,
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return AverageIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeMinimumMaintainer<
        Value: IndexNumericValue & IndexComparableValue
    >(
        valueType _: Value.Type,
        name: String
    ) -> MinIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = Index(
            name: name,
            kind: MinIndexKind<AggregationValueEntity<Value>, Value>(
                groupBy: [\.group],
                value: \.value
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: name,
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return MinIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeMaximumMaintainer<
        Value: IndexNumericValue & IndexComparableValue
    >(
        valueType _: Value.Type,
        name: String
    ) -> MaxIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = Index(
            name: name,
            kind: MaxIndexKind<AggregationValueEntity<Value>, Value>(
                groupBy: [\.group],
                value: \.value
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: name,
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return MaxIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }
}

private typealias FloatingAggregationEntity = AggregationValueEntity<Double>
private typealias UnsignedAggregationEntity = AggregationValueEntity<UInt64>
private typealias SignedAggregationEntity = AggregationValueEntity<Int64>

private struct AggregationValueEntity<Value: IndexNumericValue>: Persistable {
    typealias ID = String

    let id: String
    let group: String
    let value: Value

    static var persistableType: String {
        "AggregationValueEntity<\(Value.indexScalarType.rawValue)>"
    }
    static var allFields: [String] { ["id", "group", "value"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static var fieldSchemas: [FieldSchema] {
        [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "group", fieldNumber: 2, type: .string),
            FieldSchema(
                name: "value",
                fieldNumber: 3,
                type: valueFieldSchemaType
            ),
        ]
    }

    static func fieldNumber(for fieldName: String) -> Int? {
        switch fieldName {
        case "id": return 1
        case "group": return 2
        case "value": return 3
        default: return nil
        }
    }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id":
            return id
        case "group":
            return group
        case "value":
            return value
        default:
            return nil
        }
    }

    static func fieldName<FieldValue>(
        for keyPath: KeyPath<Self, FieldValue>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<Self>)
    }

    static func fieldName(for keyPath: PartialKeyPath<Self>) -> String {
        switch keyPath {
        case \Self.id:
            return "id"
        case \Self.group:
            return "group"
        case \Self.value:
            return "value"
        default:
            preconditionFailure("Unsupported aggregation value key path")
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<Self> else {
            preconditionFailure("Unsupported aggregation value key path type")
        }
        return fieldName(for: keyPath)
    }

    private static var valueFieldSchemaType: FieldSchemaType {
        switch Value.indexScalarType {
        case .int: return .int
        case .int8: return .int8
        case .int16: return .int16
        case .int32: return .int32
        case .int64: return .int64
        case .uint: return .uint
        case .uint8: return .uint8
        case .uint16: return .uint16
        case .uint32: return .uint32
        case .uint64: return .uint64
        case .float: return .float
        case .double: return .double
        case .string, .date:
            preconditionFailure("Aggregation test value must be numeric")
        }
    }
}

private struct UnsupportedAggregationValue: Sendable {}

@Persistable
private struct EmptyGlobalAggregationEntity {
    #Directory<EmptyGlobalAggregationEntity>("tests", "empty-global-aggregate")

    var id: String = ""
    var value: Int64 = 0

    #Index(CountIndexKind<EmptyGlobalAggregationEntity>(groupBy: []))
    #Index(SumIndexKind<EmptyGlobalAggregationEntity, Int64>(
        groupBy: [],
        value: \.value
    ))
    #Index(AverageIndexKind<EmptyGlobalAggregationEntity, Int64>(
        groupBy: [],
        value: \.value
    ))
    #Index(MinIndexKind<EmptyGlobalAggregationEntity, Int64>(
        groupBy: [],
        value: \.value
    ))
    #Index(MaxIndexKind<EmptyGlobalAggregationEntity, Int64>(
        groupBy: [],
        value: \.value
    ))
}

@Persistable
private struct IndexedGlobalSketchEntity {
    #Directory<IndexedGlobalSketchEntity>("tests", "indexed-global-sketch")

    var value: Int64 = 0

    #Index(DistinctIndexKind<IndexedGlobalSketchEntity>(
        groupBy: [],
        value: \.value
    ))
    #Index(PercentileIndexKind<IndexedGlobalSketchEntity, Int64>(
        groupBy: [],
        value: \.value
    ))
}

private struct AggregationReducerEntity: Persistable {
    typealias ID = String

    let id: String
    let group: FieldValue?
    let value: FieldValue?

    static let persistableType = "AggregationReducerEntity"
    static let allFields = ["id", "group", "value"]
    static let indexDescriptors: [IndexDescriptor] = []

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id":
            return id
        case "group":
            return group
        case "value":
            return value
        default:
            return nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<AggregationReducerEntity, Value>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<AggregationReducerEntity>)
    }

    static func fieldName(
        for keyPath: PartialKeyPath<AggregationReducerEntity>
    ) -> String {
        switch keyPath {
        case \AggregationReducerEntity.id:
            return "id"
        case \AggregationReducerEntity.group:
            return "group"
        case \AggregationReducerEntity.value:
            return "value"
        default:
            preconditionFailure("Unsupported aggregation reducer key path")
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<AggregationReducerEntity> else {
            preconditionFailure("Unsupported aggregation reducer key path type")
        }
        return fieldName(for: keyPath)
    }
}

private struct AggregationTupleEntity: Persistable {
    typealias ID = String

    let id: String
    let group: String
    let number: Int64

    static let persistableType = "AggregationTupleEntity"
    static let allFields = ["id", "group", "number"]
    static let indexDescriptors: [IndexDescriptor] = []

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id":
            return id
        case "group":
            return group
        case "number":
            return number
        default:
            return nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<AggregationTupleEntity, Value>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<AggregationTupleEntity>)
    }

    static func fieldName(
        for keyPath: PartialKeyPath<AggregationTupleEntity>
    ) -> String {
        switch keyPath {
        case \AggregationTupleEntity.id:
            return "id"
        case \AggregationTupleEntity.group:
            return "group"
        case \AggregationTupleEntity.number:
            return "number"
        default:
            preconditionFailure("Unsupported aggregation tuple key path")
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<AggregationTupleEntity> else {
            preconditionFailure("Unsupported aggregation tuple key path type")
        }
        return fieldName(for: keyPath)
    }
}
