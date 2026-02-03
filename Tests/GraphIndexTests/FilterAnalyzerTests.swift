/// FilterAnalyzerTests.swift
/// Unit tests for SPARQL filter analysis and pushdown classification

import Testing
import Foundation
import Core
import Graph
import DatabaseEngine
@testable import GraphIndex

@Suite("FilterAnalyzer Unit Tests")
struct FilterAnalyzerTests {

    // MARK: - Test Data

    private let availableFields = Set(["since", "status", "score"])
    private let subjectVar = "?s"
    private let predicateVar = "?p"
    private let objectVar = "?o"

    private func makeAnalyzer() -> FilterAnalyzer {
        FilterAnalyzer(
            subjectVar: subjectVar,
            predicateVar: predicateVar,
            objectVar: objectVar,
            graphVar: nil,
            availablePropertyFields: availableFields
        )
    }

    // MARK: - Simple Pushdown Tests

    @Test("Pushdown: simple equality on property field")
    func testSimpleEqualityPushdown() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.equals("?since", .int64(2020))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "since")
        #expect(pushable.first?.op == .equal)
        #expect(pushable.first?.value == .int64(2020))
        #expect(remaining == nil)
    }

    @Test("Pushdown: lessThan on property field")
    func testLessThanPushdown() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.lessThan("?score", .double(0.5))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "score")
        #expect(pushable.first?.op == .lessThan)
        #expect(remaining == nil)
    }

    @Test("Pushdown: greaterThanOrEqual on property field")
    func testGreaterThanOrEqualPushdown() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.greaterThanOrEqual("?since", .int64(2020))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "since")
        #expect(pushable.first?.op == .greaterThanOrEqual)
        #expect(remaining == nil)
    }

    @Test("Pushdown: string contains")
    func testContainsPushdown() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.contains("?status", "active")
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "status")
        #expect(pushable.first?.op == .contains)
        #expect(pushable.first?.value == .string("active"))
        #expect(remaining == nil)
    }

    // MARK: - Rejection Tests

    @Test("Reject: structure variable (subject)")
    func testStructureVariableRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.equals("?s", .string("Alice"))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: structure variable (predicate)")
    func testPredicateVariableRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.equals("?p", .string("knows"))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: field not in storedFieldNames")
    func testUnknownFieldRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.equals("?unknown", .int64(123))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: variable-to-variable comparison")
    func testVariableComparisonRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.variableEquals("?since", "?score")
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    // MARK: - Complex Filter Tests

    @Test("Reject: OR expression")
    func testOrRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.or(
            .equals("?since", .int64(2020)),
            .equals("?since", .int64(2021))
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: NOT expression")
    func testNotRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.not(.equals("?since", .int64(2020)))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: regex")
    func testRegexRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.regex("?status", "^active")
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    @Test("Reject: bound check")
    func testBoundCheckRejection() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.bound("?since")
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    // MARK: - AND Decomposition Tests

    @Test("AND decomposition: both pushable")
    func testAndBothPushable() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.and(
            .equals("?since", .int64(2020)),
            .equals("?status", .string("active"))
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 2)
        #expect(pushable[0].fieldName == "since")
        #expect(pushable[1].fieldName == "status")
        #expect(remaining == nil)
    }

    @Test("AND decomposition: one pushable, one complex")
    func testAndMixedFilters() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.and(
            .equals("?since", .int64(2020)),
            .regex("?status", "^active")
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "since")
        #expect(remaining != nil)  // regex remains
    }

    @Test("AND decomposition: pushable + structure variable")
    func testAndPushableAndStructure() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.and(
            .equals("?since", .int64(2020)),
            .equals("?s", .string("Alice"))
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "since")
        #expect(remaining != nil)  // structure filter remains
    }

    @Test("AND decomposition: both complex")
    func testAndBothComplex() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.and(
            .regex("?status", "^active"),
            .not(.equals("?score", .double(0.0)))
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)
    }

    // MARK: - Edge Cases

    @Test("Edge case: alwaysTrue")
    func testAlwaysTrue() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.alwaysTrue
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining == nil)  // No filtering needed
    }

    @Test("Edge case: alwaysFalse")
    func testAlwaysFalse() {
        let analyzer = makeAnalyzer()

        let filter = FilterExpression.alwaysFalse
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.isEmpty)
        #expect(remaining != nil)  // Keep as complex filter
    }

    @Test("Edge case: nested AND")
    func testNestedAnd() {
        let analyzer = makeAnalyzer()

        // (since = 2020 AND status = "active") AND score > 0.5
        let filter = FilterExpression.and(
            .and(
                .equals("?since", .int64(2020)),
                .equals("?status", .string("active"))
            ),
            .greaterThan("?score", .double(0.5))
        )
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 3)
        #expect(remaining == nil)
    }

    @Test("Edge case: variable without question mark prefix")
    func testVariableWithoutPrefix() {
        let analyzer = makeAnalyzer()

        // Some callers might not use "?" prefix
        let filter = FilterExpression.equals("since", .int64(2020))
        let (pushable, remaining) = analyzer.analyze(filter)

        #expect(pushable.count == 1)
        #expect(pushable.first?.fieldName == "since")
        #expect(remaining == nil)
    }
}
