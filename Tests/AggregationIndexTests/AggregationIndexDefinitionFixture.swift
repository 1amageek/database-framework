import DatabaseKit

func countIndexDefinition(
    groupingFields: [FieldIdentity]
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: .count,
        groupBy: groupingFields.map(IndexKey.ascending),
        value: nil
    )
}

func numericAggregationIndexDefinition(
    _ function: AggregateIndexFunction,
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: function,
        groupBy: groupingFields.map(IndexKey.ascending),
        value: valueField
    )
}

func countNotNullIndexDefinition(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: .nonNullCount,
        groupBy: groupingFields.map(IndexKey.ascending),
        value: valueField
    )
}

func distinctIndexDefinition(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity,
    precision: Int
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: .approximateDistinct(precision: precision),
        groupBy: groupingFields.map(IndexKey.ascending),
        value: valueField
    )
}

func percentileIndexDefinition(
    groupingFields: [FieldIdentity],
    valueField: FieldIdentity,
    compression: Double
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: .percentile(compression: compression),
        groupBy: groupingFields.map(IndexKey.ascending),
        value: valueField
    )
}
