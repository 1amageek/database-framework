import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing

@testable import AggregationIndex

@Suite("Nullable aggregation grouping")
struct NullableAggregationIndexTests {
    @Test("Indexed grouping preserves null and unsigned presentation types")
    func indexedGroupingPreservesNullAndUnsignedTypes() async throws {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try NullableUnsignedAggregationEntity.schemaEntity
                ]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(NullableUnsignedAggregationEntity.self)]),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        try context.insert(NullableUnsignedAggregationEntity(
            group: nil,
            value: 1,
            optionalValue: nil
        ))
        try context.insert(NullableUnsignedAggregationEntity(
            group: nil,
            value: 2,
            optionalValue: "present"
        ))
        try context.insert(NullableUnsignedAggregationEntity(
            group: 7,
            value: 4,
            optionalValue: "present"
        ))
        try await context.save()

        let builder = context
            .aggregate(NullableUnsignedAggregationEntity.self)
            .groupBy(NullableUnsignedAggregationEntity.fields.group)
            .count(as: "count")
            .sum(NullableUnsignedAggregationEntity.fields.value, as: "sum")
        let strategies = try builder.determineExecutionStrategies()
        guard case .useIndex = strategies["count"],
              case .useIndex = strategies["sum"] else {
            Issue.record("COUNT and SUM must execute through their indexes")
            return
        }

        let results = try await builder.execute()
        #expect(results.count == 2)

        let nullGroup = try #require(results.first(where: {
            $0.groupKey["group"] == .null
        }))
        #expect(nullGroup.aggregateInt64("count") == 2)
        #expect(nullGroup.aggregateInt64("sum") == 3)

        let unsignedGroup = try #require(results.first(where: {
            $0.groupKey["group"] == .uint64(7)
        }))
        #expect(unsignedGroup.groupKey["group"] == .uint64(7))
        #expect(unsignedGroup.aggregateInt64("count") == 1)
        #expect(unsignedGroup.aggregateInt64("sum") == 4)
    }

    @Test("COUNT_NOT_NULL evaluates null value before null grouping")
    func countNotNullOrdersNullExtractionCorrectly() async throws {
        let engine = InMemoryEngine()
        let index = try ResolvedIndex(
            for: NullableUnsignedAggregationEntity.self,
            name: "nullable-count-not-null",
            definition: countNotNullIndexDefinition(
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "optionalValue", number: 4)
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "optionalValue"),
            ]),
            itemTypes: [NullableUnsignedAggregationEntity.persistableType]
        )
        let maintainer = CountNotNullIndexMaintainer<
            NullableUnsignedAggregationEntity
        >(
            index: index,
            subspace: Subspace(
                prefix: Tuple("nullable-count-not-null").pack()
            ),
            idExpression: FieldKeyExpression(fieldName: "id"),
            groupByFieldNames: ["group"],
            valueFieldName: "optionalValue"
        )
        let excluded = NullableUnsignedAggregationEntity(
            group: nil,
            value: 1,
            optionalValue: nil
        )
        let included = NullableUnsignedAggregationEntity(
            group: nil,
            value: 1,
            optionalValue: "present"
        )

        try await engine.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: excluded,
                transaction: transaction
            )
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: included,
                transaction: transaction
            )
        }

        let counts = try await engine.withTransaction { transaction in
            try await maintainer.getAllCounts(transaction: transaction)
        }
        #expect(counts.count == 1)
        let result = try #require(counts.first)
        #expect(result.count == 1)
        #expect(result.grouping.count == 1)
        #expect(
            result.grouping[0] == .null
        )
    }
}

@Persistable
private struct NullableUnsignedAggregationEntity {
    #Directory<NullableUnsignedAggregationEntity>(
        "tests",
        "nullable-unsigned-aggregation"
    )

    var id: String = UUID().uuidString
    var group: UInt64?
    var value: Int64 = 0
    var optionalValue: String?

    #Index(
        .aggregate(
            name: "NullableUnsignedAggregationEntity_count_group", function: .count,
        groupBy: [.ascending(\NullableUnsignedAggregationEntity.group)]))
    #Index(
        .aggregate(
            name: "NullableUnsignedAggregationEntity_sum_group_value", function: .sum,
        groupBy: [.ascending(\NullableUnsignedAggregationEntity.group)],
        value: \NullableUnsignedAggregationEntity.value))
}
