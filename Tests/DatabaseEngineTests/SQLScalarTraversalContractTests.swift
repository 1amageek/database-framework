import DatabaseKit
import StorageKit
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("SQL scalar traversal authority")
struct SQLScalarTraversalContractTests {
    @Test("Dynamic pattern and escape columns require field authority")
    func dynamicPatternFieldsRequireAuthority() throws {
        let entity = try Schema.Entity(
            name: "PatternScope", identifierType: .string,
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "subject", fieldNumber: 2, type: .string),
                FieldSchema(name: "pattern", fieldNumber: 3, type: .string),
                FieldSchema(name: "escape", fieldNumber: 4, type: .string),
            ])
        let query = SelectQuery(
            projection: [ProjectionItem(.patternMatch(
                subject: .concatenate(.col("p", "subject"), .string("suffix")),
                pattern: .col("p", "pattern"), escape: .col("p", "escape"),
                kind: .like))],
            source: .aliased(.table(TableRef(table: "PatternScope")), name: "p"))
        let plan = DatabaseFieldReadAuthorizationPlan.make(
            query: query, schema: try Schema(entities: [entity]))
        #expect(plan.fieldsByEntity["PatternScope"] == ["subject", "pattern", "escape"])
        #expect(!plan.isCovered(by: ["PatternScope": ["subject", "pattern"]]))
        #expect(plan.isCovered(by: ["PatternScope": ["subject", "pattern", "escape"]]))
    }

    @Test("Fusion admits physical sources nested in every SQL pattern operand")
    func patternSubqueriesRequireSourcePreflight() throws {
        func nested(_ name: String) -> Expression {
            .subquery(SelectQuery(projection: [ProjectionItem(.string("value"))],
                                 source: .table(TableRef(name))))
        }
        let expression = Expression.patternMatch(
            subject: .concatenate(nested("SubjectSource"), .string("suffix")),
            pattern: nested("PatternSource"), escape: nested("EscapeSource"), kind: .like)
        let query = SelectQuery(projection: [ProjectionItem(expression)],
                                source: .values([[]]))
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: SystemStorageClock())
        #expect(try FusionSelectQueryGraphWalker.containsNestedQuery(in: expression, workMeter: meter))
        var sources = Set<String>()
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: query, workMeter: meter, { _ in },
            visitTable: { table, _ in _ = sources.insert(table.table) },
            visitPolymorphic: { _, _ in Issue.record("Unexpected polymorphic source") })
        #expect(sources == ["SubjectSource", "PatternSource", "EscapeSource"])
        let access = Expression.arrayAccess(nested("ArraySource"), subscripts: [
            .index(nested("IndexSource")),
            .slice(lower: nil, upper: nested("UpperSource")),
            .slice(lower: nested("LowerSource"), upper: nil),
        ])
        let collection = Expression.collectionOperation(.jsonPath,
            left: access, right: nested("PathSource"))
        #expect(try FusionSelectQueryGraphWalker.containsNestedQuery(in: collection, workMeter: meter))
        var collectionSources = Set<String>()
        try FusionSelectQueryGraphWalker.forEachQuery(
            in: SelectQuery(projection: [ProjectionItem(collection)], source: .values([[]])),
            workMeter: meter, { _ in },
            visitTable: { table, _ in _ = collectionSources.insert(table.table) },
            visitPolymorphic: { _, _ in Issue.record("Unexpected polymorphic source") })
        #expect(collectionSources == ["ArraySource", "IndexSource", "UpperSource", "LowerSource", "PathSource"])
        let aggregate = SQLAggregateCall(function: .stringAgg,
            arguments: [nested("AggregateArgument"), nested("AggregateDelimiter")],
            orderBy: [SortKey(nested("AggregateOrder"))], filter: nested("AggregateFilter"),
            withinGroup: [SortKey(nested("AggregateWithin"))])
        let window = Expression.window(WindowCall(function: .aggregate(aggregate),
            reference: .specification(WindowSpecification(
                partitionBy: [nested("WindowPartition")], orderBy: [SortKey(nested("WindowOrder"))],
                frame: WindowFrame(mode: .rows, start: .preceding(nested("WindowStart")),
                    end: .following(nested("WindowEnd")))))))
        let grouped = SelectQuery(projection: [ProjectionItem(window)], source: .values([[]]),
            groupBy: [.groupingSets([[.rollup([[nested("GroupingUnit")]])]])],
            windows: [NamedWindow(name: "named", specification: WindowSpecification(
                partitionBy: [nested("NamedPartition")], orderBy: [SortKey(nested("NamedOrder"))],
                frame: WindowFrame(mode: .groups, start: .preceding(nested("NamedStart")), end: .currentRow)))])
        #expect(try FusionSelectQueryGraphWalker.containsNestedQuery(in: window, workMeter: meter))
        var aggregateSources = Set<String>()
        try FusionSelectQueryGraphWalker.forEachQuery(in: grouped, workMeter: meter, { _ in },
            visitTable: { table, _ in _ = aggregateSources.insert(table.table) },
            visitPolymorphic: { _, _ in Issue.record("Unexpected polymorphic source") })
        #expect(aggregateSources == ["AggregateArgument", "AggregateDelimiter", "AggregateOrder", "AggregateFilter",
            "AggregateWithin", "WindowPartition", "WindowOrder", "WindowStart", "WindowEnd", "GroupingUnit",
            "NamedPartition", "NamedOrder", "NamedStart"])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }
}
