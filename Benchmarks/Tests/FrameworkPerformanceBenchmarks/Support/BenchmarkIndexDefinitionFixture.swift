import DatabaseKit

func benchmarkCountIndexDefinition(
    groupingFields: [FieldIdentity]
) -> IndexDefinition<FieldIdentity> {
    .aggregate(
        function: .count,
        groupBy: groupingFields.map(IndexKey.ascending),
        value: nil
    )
}

func benchmarkNumericAggregationIndexDefinition(
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

func benchmarkBitmapIndexDefinition(
    fieldName: String,
    fieldNumber: Int
) -> IndexDefinition<FieldIdentity> {
    .bitmap(field: FieldIdentity(name: fieldName, number: fieldNumber))
}

func benchmarkFullTextIndexDefinition(
    fieldName: String,
    fieldNumber: Int,
    tokenizer: TokenizationStrategy,
    storePositions: Bool,
    ngramSize: Int = 3,
    minimumTermLength: Int = 2
) -> IndexDefinition<FieldIdentity> {
    .text(
        fields: [FieldIdentity(name: fieldName, number: fieldNumber)],
        mode: .fullText(
            tokenizer: tokenizer,
            storePositions: storePositions,
            ngramSize: ngramSize,
            minimumTermLength: minimumTermLength
        )
    )
}

func benchmarkPropertyGraphIndexDefinition(
    source: FieldIdentity,
    label: FieldIdentity,
    target: FieldIdentity,
    namespace: FieldIdentity? = nil,
    strategy: PropertyGraphIndexStrategy
) -> GraphIndexDefinition<FieldIdentity> {
    .property(
        source: source,
        label: .field(label),
        target: target,
        graph: namespace,
        strategy: strategy
    )
}

func benchmarkRankIndexDefinition(
    fieldNumber: Int
) -> IndexDefinition<FieldIdentity> {
    .rank(score: FieldIdentity(name: "score", number: fieldNumber))
}

func benchmarkTimeWindowLeaderboardIndexDefinition(
    scoreFieldName: String,
    scoreFieldNumber: Int,
    window: LeaderboardWindowType,
    windowCount: Int
) -> IndexDefinition<FieldIdentity> {
    .leaderboard(
        groupBy: [],
        score: FieldIdentity(
            name: scoreFieldName,
            number: scoreFieldNumber
        ),
        window: window,
        windowCount: windowCount
    )
}
