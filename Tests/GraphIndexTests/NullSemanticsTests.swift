#if !os(WASI)
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
import TestHeartbeat
@testable import GraphIndex
import DatabaseKit
import DatabaseTypes

@Suite("SPARQL NULL Semantics", .heartbeat)
struct NullSemanticsTests {

    // MARK: - equals

    @Test("null == null → false (SPARQL error)")
    func testNullEqualsNull() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null == 'Alice' → false (SPARQL error)")
    func testNullEqualsString() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .string("Alice"))
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("'Alice' == null literal → false (SPARQL error)")
    func testStringEqualsNull() throws {
        let binding = VariableBinding(["?x": .string("Alice")])
        let filter = FilterExpression.equals("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null == 42 → false (SPARQL error)")
    func testNullEqualsInt() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.equals("?x", .int64(42))
        #expect(try filter.evaluate(binding) == false)
    }

    // MARK: - notEquals

    @Test("null != null → false (SPARQL error, not true)")
    func testNullNotEqualsNull() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notEquals("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null != 'Alice' → false (SPARQL error, not true)")
    func testNullNotEqualsString() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notEquals("?x", .string("Alice"))
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("'Alice' != null literal → false (SPARQL error, not true)")
    func testStringNotEqualsNull() throws {
        let binding = VariableBinding(["?x": .string("Alice")])
        let filter = FilterExpression.notEquals("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    // MARK: - Ordering comparisons

    @Test("null < 42 → false (SPARQL error)")
    func testNullLessThanInt() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.lessThan("?x", .int64(42))
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null <= null → false (SPARQL error)")
    func testNullLessThanOrEqualNull() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.lessThanOrEqual("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null > 0 → false (SPARQL error)")
    func testNullGreaterThanInt() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.greaterThan("?x", .int64(0))
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("null >= null → false (SPARQL error)")
    func testNullGreaterThanOrEqualNull() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.greaterThanOrEqual("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("42 < null literal → false (SPARQL error)")
    func testIntLessThanNull() throws {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.lessThan("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("42 >= null literal → false (SPARQL error)")
    func testIntGreaterThanOrEqualNull() throws {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.greaterThanOrEqual("?x", .null)
        #expect(try filter.evaluate(binding) == false)
    }

    // MARK: - Variable comparison

    @Test("variableEquals: both null → false (SPARQL error)")
    func testVariableEqualsBothNull() throws {
        let binding = VariableBinding(["?x": .null, "?y": .null])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("variableEquals: one null → false (SPARQL error)")
    func testVariableEqualsOneNull() throws {
        let binding = VariableBinding(["?x": .null, "?y": .string("Alice")])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("variableNotEquals: both null → false (SPARQL error, not true)")
    func testVariableNotEqualsBothNull() throws {
        let binding = VariableBinding(["?x": .null, "?y": .null])
        let filter = FilterExpression.variableNotEquals("?x", "?y")
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("variableNotEquals: one null → false (SPARQL error, not true)")
    func testVariableNotEqualsOneNull() throws {
        let binding = VariableBinding(["?x": .null, "?y": .int64(42)])
        let filter = FilterExpression.variableNotEquals("?x", "?y")
        #expect(try filter.evaluate(binding) == false)
    }

    // MARK: - BOUND check (unchanged behavior)

    @Test("BOUND(?x) where ?x is bound to .null → true (key exists)")
    func testBoundWithNullValue() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.bound("?x")
        #expect(try filter.evaluate(binding) == true)
    }

    @Test("!BOUND(?x) where ?x is bound to .null → false (key exists)")
    func testNotBoundWithNullValue() throws {
        let binding = VariableBinding(["?x": .null])
        let filter = FilterExpression.notBound("?x")
        #expect(try filter.evaluate(binding) == false)
    }

    @Test("BOUND(?x) where ?x is unbound → false")
    func testBoundWithUnbound() throws {
        let binding = VariableBinding()
        let filter = FilterExpression.bound("?x")
        #expect(try filter.evaluate(binding) == false)
    }

    // MARK: - Non-null comparisons (sanity checks)

    @Test("Non-null equality still works: 42 == 42 → true")
    func testNonNullEquality() throws {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.equals("?x", .int64(42))
        #expect(try filter.evaluate(binding) == true)
    }

    @Test("Non-null inequality still works: 42 != 99 → true")
    func testNonNullInequality() throws {
        let binding = VariableBinding(["?x": .int64(42)])
        let filter = FilterExpression.notEquals("?x", .int64(99))
        #expect(try filter.evaluate(binding) == true)
    }

    @Test("Non-null ordering still works: 10 < 20 → true")
    func testNonNullOrdering() throws {
        let binding = VariableBinding(["?x": .int64(10)])
        let filter = FilterExpression.lessThan("?x", .int64(20))
        #expect(try filter.evaluate(binding) == true)
    }

    @Test("Non-null variable equality: both 'Alice' → true")
    func testNonNullVariableEquals() throws {
        let binding = VariableBinding(["?x": .string("Alice"), "?y": .string("Alice")])
        let filter = FilterExpression.variableEquals("?x", "?y")
        #expect(try filter.evaluate(binding) == true)
    }
}
#endif
