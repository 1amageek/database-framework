import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import AggregationIndex

@Suite("Canonical aggregation reduction semantics")
struct CanonicalAggregationReducerTests {
    @Test("aggregation grouping restores declared integer widths")
    func aggregationGroupingRestoresDeclaredIntegerWidths() throws {
        let stored = try FieldValue.toTupleElements([
            .int8(127),
            .uint8(255),
            .uint64(UInt64.max),
        ])
        let decoded = try AggregationGroupingValueDecoder.decode(
            stored
        )

        #expect(decoded == [.int8(127), .uint8(255), .uint64(UInt64.max)])
    }

    @Test("aggregation grouping rejects noncanonical storage elements")
    func aggregationGroupingRejectsNoncanonicalStorageElement() {
        #expect(throws: FieldValueTupleCodecError.self) {
            try AggregationGroupingValueDecoder.decode([Int64(256)])
        }
    }

    @Test("empty global aggregate returns one canonical reduced row")
    func emptyGlobalAggregateReturnsOneReducedRow() async throws {
        let context = try await makeEmptyQueryContext()

        let results = try await context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "entityCount")
            .sum(EmptyGlobalAggregationEntity.fields.value, as: "sum")
            .avg(EmptyGlobalAggregationEntity.fields.value, as: "average")
            .min(EmptyGlobalAggregationEntity.fields.value, as: "minimum")
            .max(EmptyGlobalAggregationEntity.fields.value, as: "maximum")
            .percentile(
                EmptyGlobalAggregationEntity.fields.value,
                p: 0.5,
                as: "median"
            )
            .execute()

        assertCanonicalEmptyGlobalResult(results)
    }

    @Test("empty global aggregate returns one canonical row from indexes")
    func emptyGlobalAggregateReturnsOneIndexedRow() async throws {
        let context = try await makeEmptyQueryContext()

        let query = context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "entityCount")
            .sum(EmptyGlobalAggregationEntity.fields.value, as: "sum")
            .avg(EmptyGlobalAggregationEntity.fields.value, as: "average")
            .min(EmptyGlobalAggregationEntity.fields.value, as: "minimum")
            .max(EmptyGlobalAggregationEntity.fields.value, as: "maximum")
        Self.assertAllIndexBacked(
            try query.determineExecutionStrategies(),
            names: ["entityCount", "sum", "average", "minimum", "maximum"]
        )

        let results = try await query.execute()

        assertCanonicalEmptyGlobalResult(results)
    }

    @Test("non-empty global aggregate indexes accept zero grouping fields")
    func nonEmptyGlobalIndexesAcceptZeroGroupingFields() async throws {
        let schema = try Schema(
            entities: [
                try EmptyGlobalAggregationEntity.schemaEntity
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(EmptyGlobalAggregationEntity.self), try DatabaseFrameworkRuntime.entity(IndexedGlobalSketchEntity.self),
                ]),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        try context.insert(EmptyGlobalAggregationEntity(value: 4))
        try await context.save()

        let query = context
            .aggregate(EmptyGlobalAggregationEntity.self)
            .count(as: "count")
            .sum(EmptyGlobalAggregationEntity.fields.value, as: "sum")
            .avg(EmptyGlobalAggregationEntity.fields.value, as: "average")
            .min(EmptyGlobalAggregationEntity.fields.value, as: "minimum")
            .max(EmptyGlobalAggregationEntity.fields.value, as: "maximum")
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
            .distinct(IndexedGlobalSketchEntity.fields.value, as: "distinct")
            .percentile(
                IndexedGlobalSketchEntity.fields.value,
                p: 0.5,
                as: "median"
            )
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
        try context.insert(IndexedGlobalSketchEntity(id: "1", value: 10))
        try context.insert(IndexedGlobalSketchEntity(id: "2", value: 20))
        try context.insert(IndexedGlobalSketchEntity(id: "3", value: 20))
        try await context.save()

        let query = context
            .aggregate(IndexedGlobalSketchEntity.self)
            .distinct(IndexedGlobalSketchEntity.fields.value, as: "distinct")
            .percentile(
                IndexedGlobalSketchEntity.fields.value,
                p: 0.5,
                as: "median"
            )
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
                .sum(EmptyGlobalAggregationEntity.fields.value, as: "value")
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
                values: [.float64(.infinity)],
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

    @Test("extrema compare decimal and binary values without rounding")
    func extremaUseExactMixedNumericOrdering() throws {
        let lower = FieldValue.decimal(
            ExactDecimal(
                coefficient: 1_000_000_000_000_000_055,
                scale: 19
            )
        )
        let upper = FieldValue.decimal(
            ExactDecimal(
                coefficient: 1_000_000_000_000_000_056,
                scale: 19
            )
        )

        #expect(
            try CanonicalAggregationReducer.minimum(
                values: [.float64(0.1), lower],
                field: "value"
            ) == lower
        )
        #expect(
            try CanonicalAggregationReducer.maximum(
                values: [.float64(0.1), upper],
                field: "value"
            ) == upper
        )
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

    @Test("distinct ignores null and uses canonical numeric identity")
    func distinctUsesCanonicalIdentity() throws {
        let result = try CanonicalAggregationReducer.distinct(
            values: [.int64(1), .uint64(1), .float64(1), .null],
            field: "value"
        )
        let entity = AggregationReducerEntity(
            id: "1",
            group: nil,
            value: nil
        )
        let identity = try CanonicalAggregationReducer.groupIdentity(
            item: entity,
            fields: [FieldIdentity(name: "group", number: 2)]
        )

        #expect(result == .int64(1))
        #expect(identity == [.null])
    }

    @Test("grouping preserves output values and canonicalizes only the key")
    func groupingSeparatesOutputFromIdentity() throws {
        let field = FieldIdentity(name: "value", number: 3)
        let decimal = try CanonicalAggregationReducer.canonicalGroupIdentity(
            values: [
                .decimal(ExactDecimal(coefficient: 15, scale: 1)),
            ],
            fields: [field]
        )
        let floatingPoint = try CanonicalAggregationReducer
            .canonicalGroupIdentity(
                values: [.float64(1.5)],
                fields: [field]
            )
        let entity = AggregationReducerEntity(
            id: "1",
            group: nil,
            value: 1.5
        )
        let output = try CanonicalAggregationReducer.groupIdentity(
            item: entity,
            fields: [field]
        )

        #expect(decimal == floatingPoint)
        #expect(output == [.float64(1.5)])
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
                fields: [FieldIdentity(name: "missing", number: 99)]
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
        let index = try ResolvedIndex(
            for: AggregationTupleEntity.self,
            name: "minimum_by_group",
            definition: numericAggregationIndexDefinition(
                .minimum,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "number", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "number"),
            ]),
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

        await #expect(throws: AggregationIndexError.self) {
            try await maintainer.getAllMins(transaction: transaction)
        }
    }

    @Test("integer index reads do not round through Double")
    func integerIndexReadRejectsLossyDoubleConversion() throws {
        let index = try ResolvedIndex(
            for: AggregationTupleEntity.self,
            name: "sum_by_group",
            definition: numericAggregationIndexDefinition(
                .sum,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "number", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "number"),
            ]),
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
        let maintainer = try makeSumMaintainer(
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

        #expect(sum == .float64(entity.value))
    }

    @Test("materialized floating aggregates preserve compensated contributions")
    func materializedFloatingAggregatesUseCompensatedState() async throws {
        let engine = InMemoryEngine()
        let sumMaintainer = try makeSumMaintainer(
            valueType: Double.self,
            name: "compensated-double-sum"
        )
        let averageMaintainer = try makeAverageMaintainer(
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
        #expect(initial.0 == .float64(1))
        #expect(initial.1.count == 3)
        #expect(initial.1.average == .float64(1.0 / 3.0))

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
        #expect(afterDelete.0 == .float64(0))
        #expect(afterDelete.1.count == 2)
        #expect(afterDelete.1.average == .float64(0))
    }

    @Test("floating aggregate storage rejects noncanonical payload width")
    func floatingAggregateStorageRejectsLegacyWidth() throws {
        let maintainer = try makeSumMaintainer(
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
        let maintainer = try makeSumMaintainer(
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
        let maintainer = try makeSumMaintainer(
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
        let maintainer = try makeCountMaintainer(name: "count-membership")
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
        let maintainer = try makeCountMaintainer(name: "invalid-zero-count")
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
        let maintainer = try makeAverageMaintainer(
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
        let maintainer = try makeSumMaintainer(
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
        let maintainer = try makeSumMaintainer(
            valueType: Int64.self,
            name: name
        )
        let subspace = Subspace(prefix: Tuple(name).pack())
        let grouping = try FieldValue.toTupleElements(
            ["group"] as [FieldValue]
        )
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

        await #expect(throws: AggregationIndexError.self) {
            try await engine.withTransaction { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: entity,
                    transaction: transaction
                )
            }
        }
        await #expect(throws: AggregationIndexError.self) {
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
        let maintainer = try makeSumMaintainer(
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
        let minimum = try makeMinimumMaintainer(
            valueType: UInt64.self,
            name: "uint64-minimum"
        )
        let maximum = try makeMaximumMaintainer(
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

    private func makeSumMaintainer<
        Value: IndexNumericValue & FieldValueEncodable
    >(
        valueType _: Value.Type,
        name: String
    ) throws -> SumIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = try ResolvedIndex(
            for: AggregationValueEntity<Value>.self,
            name: name,
            definition: numericAggregationIndexDefinition(
                .sum,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "value", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return SumIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeEmptyQueryContext() async throws -> DatabaseContext {
        let schema = try Schema(
            entities: [
                try EmptyGlobalAggregationEntity.schemaEntity
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(EmptyGlobalAggregationEntity.self), try DatabaseFrameworkRuntime.entity(IndexedGlobalSketchEntity.self),
                ]),
            security: .testingDisabled
        )
        return container.testBaseContext()
    }

    private func makeGlobalSketchQueryContext() async throws -> DatabaseContext {
        let schema = try Schema(
            entities: [
                try IndexedGlobalSketchEntity.schemaEntity
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(EmptyGlobalAggregationEntity.self), try DatabaseFrameworkRuntime.entity(IndexedGlobalSketchEntity.self),
                ]),
            security: .testingDisabled
        )
        return container.testBaseContext()
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
    ) throws -> CountIndexMaintainer<AggregationTupleEntity> {
        let index = try ResolvedIndex(
            for: AggregationTupleEntity.self,
            name: name,
            definition: countIndexDefinition(
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ]
            ),
            rootExpression: FieldKeyExpression(fieldName: "group"),
            itemTypes: [AggregationTupleEntity.persistableType]
        )
        return CountIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeAverageMaintainer<
        Value: IndexNumericValue & FieldValueEncodable
    >(
        valueType _: Value.Type,
        name: String
    ) throws -> AverageIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = try ResolvedIndex(
            for: AggregationValueEntity<Value>.self,
            name: name,
            definition: numericAggregationIndexDefinition(
                .average,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "value", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return AverageIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeMinimumMaintainer<
        Value: IndexNumericValue & IndexComparableValue & FieldValueEncodable
    >(
        valueType _: Value.Type,
        name: String
    ) throws -> MinIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = try ResolvedIndex(
            for: AggregationValueEntity<Value>.self,
            name: name,
            definition: numericAggregationIndexDefinition(
                .minimum,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "value", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            itemTypes: [AggregationValueEntity<Value>.persistableType]
        )
        return MinIndexMaintainer(
            index: index,
            subspace: Subspace(prefix: Tuple(name).pack()),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    private func makeMaximumMaintainer<
        Value: IndexNumericValue & IndexComparableValue & FieldValueEncodable
    >(
        valueType _: Value.Type,
        name: String
    ) throws -> MaxIndexMaintainer<AggregationValueEntity<Value>, Value> {
        let index = try ResolvedIndex(
            for: AggregationValueEntity<Value>.self,
            name: name,
            definition: numericAggregationIndexDefinition(
                .maximum,
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "value", number: 3),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
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

private struct AggregationValueEntity<
    Value: IndexNumericValue & FieldValueEncodable
>: Persistable {
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

    func persistedFieldValue(
        for field: FieldIdentity
    ) throws(PersistableEncodingError) -> FieldValue? {
        switch (field.name, field.number) {
        case ("id", 1): .string(id)
        case ("group", 2): .string(group)
        case ("value", 3): value.fieldValue
        default: nil
        }
    }

    func encodePersistedFields<Output: PersistedFieldOutput>(
        to output: inout Output
    ) throws(PersistableEncodingFailure<Output.Failure>) {
        try output.write(
            FieldIdentity(name: "id", number: 1),
            value: id,
            entity: Self.persistableType
        )
        try output.write(
            FieldIdentity(name: "group", number: 2),
            value: group,
            entity: Self.persistableType
        )
        try output.write(
            FieldIdentity(name: "value", number: 3),
            value: value,
            entity: Self.persistableType
        )
    }

    static func decodePersistedFields<Input: PersistedFieldInput>(
        from input: inout Input
    ) throws(PersistableDecodingFailure<Input.Failure>) -> Self {
        let id = try input.decode(
            String.self,
            for: FieldIdentity(name: "id", number: 1),
            entity: persistableType
        )
        let group = try input.decode(
            String.self,
            for: FieldIdentity(name: "group", number: 2),
            entity: persistableType
        )
        let value = try input.decode(
            Value.self,
            for: FieldIdentity(name: "value", number: 3),
            entity: persistableType
        )
        try input.finish(entity: persistableType)
        return Self(id: id, group: group, value: value)
    }

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
        case .int8: return .int8
        case .int16: return .int16
        case .int32: return .int32
        case .int64: return .int64
        case .uint8: return .uint8
        case .uint16: return .uint16
        case .uint32: return .uint32
        case .uint64: return .uint64
        case .float32: return .float32
        case .float64: return .float64
        case .string, .date, .timestamp:
            preconditionFailure("Aggregation test value must be numeric")
        }
    }
}

@Persistable
private struct EmptyGlobalAggregationEntity {
    #Directory<EmptyGlobalAggregationEntity>("tests", "empty-global-aggregate")

    var id: String = ""
    var value: Int64 = 0

    #Index(.aggregate(name: "EmptyGlobalAggregationEntity_count", function: .count, groupBy: []))
    #Index(
        .aggregate(
            name: "EmptyGlobalAggregationEntity_sum_value", function: .sum,
        groupBy: [],
        value: \EmptyGlobalAggregationEntity.value))
    #Index(
        .aggregate(
            name: "EmptyGlobalAggregationEntity_avg_value", function: .average,
        groupBy: [],
        value: \EmptyGlobalAggregationEntity.value))
    #Index(
        .aggregate(
            name: "EmptyGlobalAggregationEntity_min_value", function: .minimum,
        groupBy: [],
        value: \EmptyGlobalAggregationEntity.value))
    #Index(
        .aggregate(
            name: "EmptyGlobalAggregationEntity_max_value", function: .maximum,
        groupBy: [],
        value: \EmptyGlobalAggregationEntity.value))
}

@Persistable
private struct IndexedGlobalSketchEntity {
    #Directory<IndexedGlobalSketchEntity>("tests", "indexed-global-sketch")

    var id: String = ""
    var value: Int64 = 0

    #Index(
        .aggregate(
            name: "IndexedGlobalSketchEntity_distinct_value",
            function: .approximateDistinct(precision: 14),
        groupBy: [],
        value: \IndexedGlobalSketchEntity.value))
    #Index(
        .aggregate(
            name: "IndexedGlobalSketchEntity_percentile_value",
            function: .percentile(compression: 100),
        groupBy: [],
        value: \IndexedGlobalSketchEntity.value))
}

@Persistable
private struct AggregationReducerEntity {
    let id: String
    let group: String?
    let value: Double?
}

@Persistable
private struct AggregationTupleEntity {
    let id: String
    let group: String
    let number: Int64
}
