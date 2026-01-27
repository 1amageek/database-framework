// NullSemanticsTests.swift
// GraphIndex - SPARQL three-valued logic NULL semantics tests
//
// Verifies that FilterExpression correctly handles .null values
// according to SPARQL 1.1 Section 17.2 (Filter Evaluation).
//
// SPARQL three-valued logic requires that any comparison involving NULL
// yields "error", which FILTER evaluates as false.
// FieldValue's Equatable has .null == .null → true (system-wide),
// but SPARQL layer must override this to return false.

import Testing
@testable import GraphIndex
import Core

@Suite("SPARQL NULL Semantics")
struct NullSemanticsTests {

    // MARK: - equals

    @Test("null == null → false (SPARQL error)")
    func testNullEqualsNull() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null == 'Alice' → false (SPARQL error)")
    func testNullEqualsString() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .string("Alice"))
        #expect(filter.evaluate(binding) == false)
    }

    @Test("'Alice' == null literal → false (SPARQL error)")
    func testStringEqualsNull() {
        let binding = VariableBinding(["?x": .string("Alice")])
        let filter = FilterExpression.equals("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null == 42 → false (SPARQL error)")
    func testNullEqualsInt() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .int64(42))
        #expect(filter.evaluate(binding) == false)
    }

    // MARK: - notEquals

    @Test("null != null → false (SPARQL error, not true)")
    func testNullNotEqualsNull() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notEquals("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null != 'Alice' → false (SPARQL error, not true)")
    func testNullNotEqualsString() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notEquals("?x", .string("Alice"))
        #expect(filter.evaluate(binding) == false)
    }

    @Test("'Alice' != null literal → false (SPARQL error, not true)")
    func testStringNotEqualsNull() {
        let binding = VariableBinding(["?x": .string("Alice")])
        let filter = FilterExpression.notEquals("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    // MARK: - Ordering comparisons

    @Test("null < 42 → false (SPARQL error)")
    func testNullLessThanInt() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.lessThan("?x", .int64(42))
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null <= null → false (SPARQL error)")
    func testNullLessThanOrEqualNull() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.lessThanOrEqual("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null > 0 → false (SPARQL error)")
    func testNullGreaterThanInt() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.greaterThan("?x", .int64(0))
        #expect(filter.evaluate(binding) == false)
    }

    @Test("null >= null → false (SPARQL error)")
    func testNullGreaterThanOrEqualNull() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.greaterThanOrEqual("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("42 < null literal → false (SPARQL error)")
    func testIntLessThanNull() {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.lessThan("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    @Test("42 >= null literal → false (SPARQL error)")
    func testIntGreaterThanOrEqualNull() {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.greaterThanOrEqual("?x", .null)
        #expect(filter.evaluate(binding) == false)
    }

    // MARK: - Variable comparison

    @Test("variableEquals: both null → false (SPARQL error)")
    func testVariableEqualsBothNull() {
        let binding = VariableBinding(["?x": .null, "?y": .null])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(filter.evaluate(binding) == false)
    }

    @Test("variableEquals: one null → false (SPARQL error)")
    func testVariableEqualsOneNull() {
        let binding = VariableBinding(["?x": .null, "?y": .string("Alice")])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(filter.evaluate(binding) == false)
    }

    @Test("variableNotEquals: both null → false (SPARQL error, not true)")
    func testVariableNotEqualsBothNull() {
        let binding = VariableBinding(["?x": .null, "?y": .null])
        let filter = FilterExpression.variableNotEquals("?x", "?y")
        #expect(filter.evaluate(binding) == false)
    }

    @Test("variableNotEquals: one null → false (SPARQL error, not true)")
    func testVariableNotEqualsOneNull() {
        let binding = VariableBinding(["?x": .null, "?y": .int64(42)])
        let filter = FilterExpression.variableNotEquals("?x", "?y")
        #expect(filter.evaluate(binding) == false)
    }

    // MARK: - BOUND check (unchanged behavior)

    @Test("BOUND(?x) where ?x is bound to .null → true (key exists)")
    func testBoundWithNullValue() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.bound("?x")
        #expect(filter.evaluate(binding) == true)
    }

    @Test("!BOUND(?x) where ?x is bound to .null → false (key exists)")
    func testNotBoundWithNullValue() {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notBound("?x")
        #expect(filter.evaluate(binding) == false)
    }

    @Test("BOUND(?x) where ?x is unbound → false")
    func testBoundWithUnbound() {
        let binding = VariableBinding()
        let filter = FilterExpression.bound("?x")
        #expect(filter.evaluate(binding) == false)
    }

    // MARK: - Non-null comparisons (sanity checks)

    @Test("Non-null equality still works: 42 == 42 → true")
    func testNonNullEquality() {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.equals("?x", .int64(42))
        #expect(filter.evaluate(binding) == true)
    }

    @Test("Non-null inequality still works: 42 != 99 → true")
    func testNonNullInequality() {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.notEquals("?x", .int64(99))
        #expect(filter.evaluate(binding) == true)
    }

    @Test("Non-null ordering still works: 10 < 20 → true")
    func testNonNullOrdering() {
        let binding = VariableBinding(["?x": .int64(10)])
        let filter = FilterExpression.lessThan("?x", .int64(20))
        #expect(filter.evaluate(binding) == true)
    }

    @Test("Non-null variable equality: both 'Alice' → true")
    func testNonNullVariableEquals() {
        let binding = VariableBinding(["?x": .string("Alice"), "?y": .string("Alice")])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(filter.evaluate(binding) == true)
    }
}
