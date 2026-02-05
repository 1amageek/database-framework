/// ExpressionOperatorTests.swift
/// Tests for QueryIR Expression operators

import Testing
@testable import QueryIR

@Suite("Expression Operators")
struct ExpressionOperatorTests {

    // MARK: - Arithmetic Operators

    @Test func additionOperator() {
        let a = Expression.int(5)
        let b = Expression.int(3)
        let result = a + b
        #expect(result == .add(.int(5), .int(3)))
    }

    @Test func subtractionOperator() {
        let a = Expression.int(10)
        let b = Expression.int(4)
        let result = a - b
        #expect(result == .subtract(.int(10), .int(4)))
    }

    @Test func multiplicationOperator() {
        let a = Expression.int(6)
        let b = Expression.int(7)
        let result = a * b
        #expect(result == .multiply(.int(6), .int(7)))
    }

    @Test func divisionOperator() {
        let a = Expression.int(20)
        let b = Expression.int(4)
        let result = a / b
        #expect(result == .divide(.int(20), .int(4)))
    }

    @Test func moduloOperator() {
        let a = Expression.int(17)
        let b = Expression.int(5)
        let result = a % b
        #expect(result == .modulo(.int(17), .int(5)))
    }

    @Test func negationOperator() {
        let a = Expression.int(42)
        let result = -a
        #expect(result == .negate(.int(42)))
    }

    // MARK: - Comparison Operators

    @Test func equalityOperator() {
        let a = Expression.col("name")
        let b = Expression.string("Alice")
        let result = a .== b
        #expect(result == .equal(.col("name"), .string("Alice")))
    }

    @Test func inequalityOperator() {
        let a = Expression.col("status")
        let b = Expression.string("deleted")
        let result = a .!= b
        #expect(result == .notEqual(.col("status"), .string("deleted")))
    }

    @Test func lessThanOperator() {
        let a = Expression.col("age")
        let b = Expression.int(18)
        let result = a .< b
        #expect(result == .lessThan(.col("age"), .int(18)))
    }

    @Test func lessThanOrEqualOperator() {
        let a = Expression.col("price")
        let b = Expression.double(99.99)
        let result = a .<= b
        #expect(result == .lessThanOrEqual(.col("price"), .double(99.99)))
    }

    @Test func greaterThanOperator() {
        let a = Expression.col("score")
        let b = Expression.int(100)
        let result = a .> b
        #expect(result == .greaterThan(.col("score"), .int(100)))
    }

    @Test func greaterThanOrEqualOperator() {
        let a = Expression.col("quantity")
        let b = Expression.int(1)
        let result = a .>= b
        #expect(result == .greaterThanOrEqual(.col("quantity"), .int(1)))
    }

    // MARK: - Logical Operators

    @Test func andOperator() {
        let a = Expression.col("active") .== Expression.bool(true)
        let b = Expression.col("verified") .== Expression.bool(true)
        let result = a && b
        if case .and(let left, let right) = result {
            #expect(left == a)
            #expect(right == b)
        } else {
            Issue.record("Expected .and expression")
        }
    }

    @Test func orOperator() {
        let a = Expression.col("status") .== Expression.string("pending")
        let b = Expression.col("status") .== Expression.string("active")
        let result = a || b
        if case .or(let left, let right) = result {
            #expect(left == a)
            #expect(right == b)
        } else {
            Issue.record("Expected .or expression")
        }
    }

    @Test func notOperator() {
        let a = Expression.col("deleted") .== Expression.bool(true)
        let result = !a
        if case .not(let inner) = result {
            #expect(inner == a)
        } else {
            Issue.record("Expected .not expression")
        }
    }

    // MARK: - Operator Precedence

    @Test func multiplicationBeforeAddition() {
        // 2 + 3 * 4 should parse as 2 + (3 * 4)
        let two = Expression.int(2)
        let three = Expression.int(3)
        let four = Expression.int(4)
        let result = two + three * four

        // Should be add(2, multiply(3, 4))
        if case .add(let left, let right) = result {
            #expect(left == .int(2))
            #expect(right == .multiply(.int(3), .int(4)))
        } else {
            Issue.record("Expected addition with multiplication on right")
        }
    }

    @Test func parenthesesOverridePrecedence() {
        // (2 + 3) * 4
        let two = Expression.int(2)
        let three = Expression.int(3)
        let four = Expression.int(4)
        let result = (two + three) * four

        // Should be multiply(add(2, 3), 4)
        if case .multiply(let left, let right) = result {
            #expect(left == .add(.int(2), .int(3)))
            #expect(right == .int(4))
        } else {
            Issue.record("Expected multiplication with addition on left")
        }
    }

    // MARK: - Nested Expressions

    @Test func nestedArithmetic() {
        // (a + b) * (c - d)
        let a = Expression.col("a")
        let b = Expression.col("b")
        let c = Expression.col("c")
        let d = Expression.col("d")
        let result = (a + b) * (c - d)

        if case .multiply(let left, let right) = result {
            #expect(left == .add(.col("a"), .col("b")))
            #expect(right == .subtract(.col("c"), .col("d")))
        } else {
            Issue.record("Expected nested multiplication")
        }
    }

    @Test func nestedLogical() {
        // (a && b) || (c && d)
        let a = Expression.col("a") .== Expression.bool(true)
        let b = Expression.col("b") .== Expression.bool(true)
        let c = Expression.col("c") .== Expression.bool(true)
        let d = Expression.col("d") .== Expression.bool(true)
        let result = (a && b) || (c && d)

        if case .or(_, _) = result {
            // Just verify structure is correct
            #expect(true)
        } else {
            Issue.record("Expected .or expression at top level")
        }
    }

    @Test func complexExpression() {
        // age >= 18 && (status == "active" || status == "pending")
        let age = Expression.col("age")
        let status = Expression.col("status")

        let condition = (age .>= Expression.int(18)) && (
            (status .== Expression.string("active")) ||
            (status .== Expression.string("pending"))
        )

        if case .and(let left, let right) = condition {
            #expect(left == .greaterThanOrEqual(.col("age"), .int(18)))
            if case .or(_, _) = right {
                #expect(true)
            } else {
                Issue.record("Expected .or on right side")
            }
        } else {
            Issue.record("Expected .and at top level")
        }
    }

    // MARK: - Expression Builder Helpers

    @Test func columnHelper() {
        let col = Expression.col("name")
        #expect(col == .column(ColumnRef(column: "name")))
    }

    @Test func qualifiedColumnHelper() {
        let col = Expression.col("users", "name")
        #expect(col == .column(ColumnRef(table: "users", column: "name")))
    }

    @Test func variableHelper() {
        let v = Expression.var("person")
        #expect(v == .variable(Variable("person")))
    }

    @Test func literalHelpers() {
        #expect(Expression.string("hello") == .literal(.string("hello")))
        #expect(Expression.int(42) == .literal(.int(42)))
        #expect(Expression.double(3.14) == .literal(.double(3.14)))
        #expect(Expression.bool(true) == .literal(.bool(true)))
        #expect(Expression.null == .literal(.null))
    }

    // MARK: - Equatable / Hashable

    @Test func expressionEquality() {
        let a = Expression.col("name") .== Expression.string("Alice")
        let b = Expression.col("name") .== Expression.string("Alice")
        let c = Expression.col("name") .== Expression.string("Bob")

        #expect(a == b)
        #expect(a != c)
    }

    @Test func expressionHashable() {
        let a = Expression.col("name") .== Expression.string("Alice")
        let b = Expression.col("name") .== Expression.string("Alice")

        var set: Set<Expression> = []
        set.insert(a)
        set.insert(b)

        #expect(set.count == 1)
    }
}

@Suite("ColumnRef")
struct ColumnRefTests {

    @Test func unqualifiedColumn() {
        let col = ColumnRef("name")
        #expect(col.table == nil)
        #expect(col.column == "name")
    }

    @Test func qualifiedColumn() {
        let col = ColumnRef(table: "users", column: "name")
        #expect(col.table == "users")
        #expect(col.column == "name")
    }

    @Test func descriptionUnqualified() {
        let col = ColumnRef("name")
        #expect(col.description == "\"name\"")
    }

    @Test func descriptionQualified() {
        let col = ColumnRef(table: "users", column: "name")
        #expect(col.description == "\"users\".\"name\"")
    }

    @Test func displayNameUnqualified() {
        let col = ColumnRef("name")
        #expect(col.displayName == "name")
    }

    @Test func displayNameQualified() {
        let col = ColumnRef(table: "users", column: "name")
        #expect(col.displayName == "users.name")
    }
}

@Suite("Variable")
struct VariableTests {

    @Test func variableWithoutPrefix() {
        let v = Variable("person")
        #expect(v.name == "person")
    }

    @Test func variableWithQuestionMark() {
        let v = Variable("?person")
        #expect(v.name == "person")
    }

    @Test func variableWithDollar() {
        let v = Variable("$person")
        #expect(v.name == "person")
    }

    @Test func variableDescription() {
        let v = Variable("person")
        #expect(v.description == "?person")
    }
}
