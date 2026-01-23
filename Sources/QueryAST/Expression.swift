/// Expression.swift
/// Unified expression representation for SQL and SPARQL
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL expressions)
/// - W3C SPARQL 1.1/1.2 (SPARQL expressions and filters)

import Foundation

// MARK: - Custom Operators for Expression Building

/// Equality expression operator (distinct from Equatable's ==)
infix operator .== : ComparisonPrecedence

/// Inequality expression operator (distinct from Equatable's !=)
infix operator .!= : ComparisonPrecedence

/// Less-than expression operator
infix operator .< : ComparisonPrecedence

/// Less-than-or-equal expression operator
infix operator .<= : ComparisonPrecedence

/// Greater-than expression operator
infix operator .> : ComparisonPrecedence

/// Greater-than-or-equal expression operator
infix operator .>= : ComparisonPrecedence

/// Column reference (SQL)
public struct ColumnRef: Sendable, Equatable, Hashable {
    /// Optional table/alias qualifier
    public let table: String?

    /// Column name
    public let column: String

    public init(table: String? = nil, column: String) {
        self.table = table
        self.column = column
    }

    /// Create an unqualified column reference
    public init(_ column: String) {
        self.table = nil
        self.column = column
    }
}

extension ColumnRef: CustomStringConvertible {
    public var description: String {
        if let table = table {
            return "\(table).\(column)"
        }
        return column
    }
}

/// Variable reference (SPARQL)
public struct Variable: Sendable, Equatable, Hashable {
    /// Variable name (without ? prefix)
    public let name: String

    public init(_ name: String) {
        // Remove leading ? or $ if present
        if name.hasPrefix("?") || name.hasPrefix("$") {
            self.name = String(name.dropFirst())
        } else {
            self.name = name
        }
    }
}

extension Variable: CustomStringConvertible {
    public var description: String {
        "?\(name)"
    }
}

/// Aggregate function types
public enum AggregateFunction: Sendable, Equatable, Hashable {
    case count(Expression?, distinct: Bool)
    case sum(Expression, distinct: Bool)
    case avg(Expression, distinct: Bool)
    case min(Expression)
    case max(Expression)
    case groupConcat(Expression, separator: String?, distinct: Bool)
    case sample(Expression)  // SPARQL
    case arrayAgg(Expression, orderBy: [SortKey]?, distinct: Bool)  // SQL

    public func hash(into hasher: inout Hasher) {
        switch self {
        case .count(let expr, let distinct):
            hasher.combine(0)
            hasher.combine(expr)
            hasher.combine(distinct)
        case .sum(let expr, let distinct):
            hasher.combine(1)
            hasher.combine(expr)
            hasher.combine(distinct)
        case .avg(let expr, let distinct):
            hasher.combine(2)
            hasher.combine(expr)
            hasher.combine(distinct)
        case .min(let expr):
            hasher.combine(3)
            hasher.combine(expr)
        case .max(let expr):
            hasher.combine(4)
            hasher.combine(expr)
        case .groupConcat(let expr, let sep, let distinct):
            hasher.combine(5)
            hasher.combine(expr)
            hasher.combine(sep)
            hasher.combine(distinct)
        case .sample(let expr):
            hasher.combine(6)
            hasher.combine(expr)
        case .arrayAgg(let expr, let orderBy, let distinct):
            hasher.combine(7)
            hasher.combine(expr)
            if let orderBy = orderBy {
                for key in orderBy {
                    hasher.combine(key)
                }
            }
            hasher.combine(distinct)
        }
    }
}

/// Function call
public struct FunctionCall: Sendable, Equatable, Hashable {
    /// Function name (possibly qualified)
    public let name: String

    /// Arguments
    public let arguments: [Expression]

    /// DISTINCT modifier (for aggregate-like functions)
    public let distinct: Bool

    public init(name: String, arguments: [Expression], distinct: Bool = false) {
        self.name = name
        self.arguments = arguments
        self.distinct = distinct
    }
}

/// Sort direction
public enum SortDirection: Sendable, Equatable, Hashable {
    case ascending
    case descending
}

/// NULL ordering
public enum NullOrdering: Sendable, Equatable, Hashable {
    case first
    case last
}

/// Sort key for ORDER BY
public struct SortKey: Sendable, Equatable, Hashable {
    public let expression: Expression
    public let direction: SortDirection
    public let nulls: NullOrdering?

    public init(
        _ expression: Expression,
        direction: SortDirection = .ascending,
        nulls: NullOrdering? = nil
    ) {
        self.expression = expression
        self.direction = direction
        self.nulls = nulls
    }
}

/// Aggregate binding (for SPARQL GROUP BY)
public struct AggregateBinding: Sendable, Equatable, Hashable {
    public let variable: String
    public let aggregate: AggregateFunction

    public init(variable: String, aggregate: AggregateFunction) {
        self.variable = variable
        self.aggregate = aggregate
    }
}

/// SQL data types for CAST
public indirect enum DataType: Sendable, Equatable, Hashable {
    case boolean
    case smallint
    case integer
    case bigint
    case real
    case doublePrecision
    case decimal(precision: Int?, scale: Int?)
    case char(length: Int?)
    case varchar(length: Int?)
    case text
    case date
    case time(withTimeZone: Bool)
    case timestamp(withTimeZone: Bool)
    case interval
    case binary(length: Int?)
    case varbinary(length: Int?)
    case blob
    case json
    case jsonb
    case uuid
    case array(DataType)
    case custom(String)  // For database-specific types
}

/// Unified expression representation
/// Combines SQL expressions and SPARQL filter expressions
public indirect enum Expression: Sendable, Hashable {
    // MARK: - Literals

    /// Literal value
    case literal(Literal)

    // MARK: - Identifiers

    /// Column reference: table.column or column
    case column(ColumnRef)

    /// Variable reference: ?var (SPARQL)
    case variable(Variable)

    // MARK: - Arithmetic Operations

    /// Addition: a + b
    case add(Expression, Expression)

    /// Subtraction: a - b
    case subtract(Expression, Expression)

    /// Multiplication: a * b
    case multiply(Expression, Expression)

    /// Division: a / b
    case divide(Expression, Expression)

    /// Modulo: a % b
    case modulo(Expression, Expression)

    /// Unary negation: -a
    case negate(Expression)

    // MARK: - Comparison Operations

    /// Equality: a = b
    case equal(Expression, Expression)

    /// Inequality: a != b or a <> b
    case notEqual(Expression, Expression)

    /// Less than: a < b
    case lessThan(Expression, Expression)

    /// Less than or equal: a <= b
    case lessThanOrEqual(Expression, Expression)

    /// Greater than: a > b
    case greaterThan(Expression, Expression)

    /// Greater than or equal: a >= b
    case greaterThanOrEqual(Expression, Expression)

    // MARK: - Logical Operations

    /// Logical AND: a AND b
    case and(Expression, Expression)

    /// Logical OR: a OR b
    case or(Expression, Expression)

    /// Logical NOT: NOT a
    case not(Expression)

    // MARK: - NULL / BOUND Checks

    /// IS NULL check
    case isNull(Expression)

    /// IS NOT NULL check
    case isNotNull(Expression)

    /// SPARQL BOUND(?var) check
    case bound(Variable)

    // MARK: - Pattern Matching

    /// SQL LIKE: expr LIKE pattern
    case like(Expression, pattern: String)

    /// Regex match: REGEX(expr, pattern, flags)
    case regex(Expression, pattern: String, flags: String?)

    // MARK: - Range Operations

    /// BETWEEN: expr BETWEEN low AND high
    case between(Expression, low: Expression, high: Expression)

    /// IN list: expr IN (val1, val2, ...)
    case inList(Expression, values: [Expression])

    /// IN subquery: expr IN (SELECT ...)
    case inSubquery(Expression, subquery: SelectQuery)

    // MARK: - Aggregate Functions

    /// Aggregate function call
    case aggregate(AggregateFunction)

    // MARK: - Scalar Functions

    /// Function call: function(args...)
    case function(FunctionCall)

    // MARK: - Conditional Expressions

    /// CASE WHEN: CASE WHEN c1 THEN r1 ... ELSE r END
    case caseWhen(cases: [(condition: Expression, result: Expression)], elseResult: Expression?)

    /// COALESCE: COALESCE(e1, e2, ...)
    case coalesce([Expression])

    /// NULLIF: NULLIF(e1, e2)
    case nullIf(Expression, Expression)

    // MARK: - Type Conversion

    /// CAST: CAST(expr AS type)
    case cast(Expression, targetType: DataType)

    // MARK: - SPARQL RDF-star Operations

    /// Triple constructor (RDF-star): << s p o >>
    case triple(subject: Expression, predicate: Expression, object: Expression)

    /// IS TRIPLE check
    case isTriple(Expression)

    /// SUBJECT accessor
    case subject(Expression)

    /// PREDICATE accessor
    case predicate(Expression)

    /// OBJECT accessor
    case object(Expression)

    // MARK: - Subqueries

    /// Scalar subquery: (SELECT ...)
    case subquery(SelectQuery)

    /// EXISTS subquery: EXISTS (SELECT ...)
    case exists(SelectQuery)
}

// MARK: - Expression Builder Helpers

extension Expression {
    /// Create a column expression
    public static func col(_ name: String) -> Expression {
        .column(ColumnRef(column: name))
    }

    /// Create a qualified column expression
    public static func col(_ table: String, _ column: String) -> Expression {
        .column(ColumnRef(table: table, column: column))
    }

    /// Create a variable expression
    public static func `var`(_ name: String) -> Expression {
        .variable(Variable(name))
    }

    /// Create a literal expression
    public static func lit(_ value: Literal) -> Expression {
        .literal(value)
    }

    /// Create a literal from a value
    public static func lit(_ value: Any) -> Expression? {
        guard let literal = Literal(value) else { return nil }
        return .literal(literal)
    }

    /// Create a string literal
    public static func string(_ value: String) -> Expression {
        .literal(.string(value))
    }

    /// Create an integer literal
    public static func int(_ value: Int64) -> Expression {
        .literal(.int(value))
    }

    /// Create a double literal
    public static func double(_ value: Double) -> Expression {
        .literal(.double(value))
    }

    /// Create a boolean literal
    public static func bool(_ value: Bool) -> Expression {
        .literal(.bool(value))
    }

    /// Create a NULL literal
    public static var null: Expression {
        .literal(.null)
    }
}

// MARK: - Operator Overloads

extension Expression {
    public static func + (lhs: Expression, rhs: Expression) -> Expression {
        .add(lhs, rhs)
    }

    public static func - (lhs: Expression, rhs: Expression) -> Expression {
        .subtract(lhs, rhs)
    }

    public static func * (lhs: Expression, rhs: Expression) -> Expression {
        .multiply(lhs, rhs)
    }

    public static func / (lhs: Expression, rhs: Expression) -> Expression {
        .divide(lhs, rhs)
    }

    public static func % (lhs: Expression, rhs: Expression) -> Expression {
        .modulo(lhs, rhs)
    }

    public static prefix func - (expr: Expression) -> Expression {
        .negate(expr)
    }

    /// Expression equality (creates an .equal expression)
    /// Use `.==` operator for building SQL/SPARQL equality conditions
    public static func .== (lhs: Expression, rhs: Expression) -> Expression {
        .equal(lhs, rhs)
    }

    /// Expression inequality (creates a .notEqual expression)
    /// Use `.!=` operator for building SQL/SPARQL inequality conditions
    public static func .!= (lhs: Expression, rhs: Expression) -> Expression {
        .notEqual(lhs, rhs)
    }

    public static func .< (lhs: Expression, rhs: Expression) -> Expression {
        .lessThan(lhs, rhs)
    }

    public static func .<= (lhs: Expression, rhs: Expression) -> Expression {
        .lessThanOrEqual(lhs, rhs)
    }

    public static func .> (lhs: Expression, rhs: Expression) -> Expression {
        .greaterThan(lhs, rhs)
    }

    public static func .>= (lhs: Expression, rhs: Expression) -> Expression {
        .greaterThanOrEqual(lhs, rhs)
    }

    public static func && (lhs: Expression, rhs: Expression) -> Expression {
        .and(lhs, rhs)
    }

    public static func || (lhs: Expression, rhs: Expression) -> Expression {
        .or(lhs, rhs)
    }

    public static prefix func ! (expr: Expression) -> Expression {
        .not(expr)
    }
}

// MARK: - Equatable Conformance

extension Expression: Equatable {
    public static func == (lhs: Expression, rhs: Expression) -> Bool {
        switch (lhs, rhs) {
        case (.literal(let l1), .literal(let l2)):
            return l1 == l2
        case (.column(let c1), .column(let c2)):
            return c1 == c2
        case (.variable(let v1), .variable(let v2)):
            return v1 == v2
        case (.add(let l1, let r1), .add(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.subtract(let l1, let r1), .subtract(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.multiply(let l1, let r1), .multiply(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.divide(let l1, let r1), .divide(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.modulo(let l1, let r1), .modulo(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.negate(let e1), .negate(let e2)):
            return e1 == e2
        case (.equal(let l1, let r1), .equal(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.notEqual(let l1, let r1), .notEqual(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.lessThan(let l1, let r1), .lessThan(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.lessThanOrEqual(let l1, let r1), .lessThanOrEqual(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.greaterThan(let l1, let r1), .greaterThan(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.greaterThanOrEqual(let l1, let r1), .greaterThanOrEqual(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.and(let l1, let r1), .and(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.or(let l1, let r1), .or(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.not(let e1), .not(let e2)):
            return e1 == e2
        case (.isNull(let e1), .isNull(let e2)):
            return e1 == e2
        case (.isNotNull(let e1), .isNotNull(let e2)):
            return e1 == e2
        case (.bound(let v1), .bound(let v2)):
            return v1 == v2
        case (.like(let e1, let p1), .like(let e2, let p2)):
            return e1 == e2 && p1 == p2
        case (.regex(let e1, let p1, let f1), .regex(let e2, let p2, let f2)):
            return e1 == e2 && p1 == p2 && f1 == f2
        case (.between(let e1, let l1, let h1), .between(let e2, let l2, let h2)):
            return e1 == e2 && l1 == l2 && h1 == h2
        case (.inList(let e1, let v1), .inList(let e2, let v2)):
            return e1 == e2 && v1 == v2
        case (.inSubquery(let e1, let s1), .inSubquery(let e2, let s2)):
            return e1 == e2 && s1 == s2
        case (.aggregate(let a1), .aggregate(let a2)):
            return a1 == a2
        case (.function(let f1), .function(let f2)):
            return f1 == f2
        case (.caseWhen(let c1, let e1), .caseWhen(let c2, let e2)):
            guard c1.count == c2.count else { return false }
            for (pair1, pair2) in zip(c1, c2) {
                if pair1.condition != pair2.condition || pair1.result != pair2.result {
                    return false
                }
            }
            return e1 == e2
        case (.coalesce(let e1), .coalesce(let e2)):
            return e1 == e2
        case (.nullIf(let l1, let r1), .nullIf(let l2, let r2)):
            return l1 == l2 && r1 == r2
        case (.cast(let e1, let t1), .cast(let e2, let t2)):
            return e1 == e2 && t1 == t2
        case (.triple(let s1, let p1, let o1), .triple(let s2, let p2, let o2)):
            return s1 == s2 && p1 == p2 && o1 == o2
        case (.isTriple(let e1), .isTriple(let e2)):
            return e1 == e2
        case (.subject(let e1), .subject(let e2)):
            return e1 == e2
        case (.predicate(let e1), .predicate(let e2)):
            return e1 == e2
        case (.object(let e1), .object(let e2)):
            return e1 == e2
        case (.subquery(let s1), .subquery(let s2)):
            return s1 == s2
        case (.exists(let s1), .exists(let s2)):
            return s1 == s2
        default:
            return false
        }
    }
}

// MARK: - Hashable Conformance

extension Expression {
    public func hash(into hasher: inout Hasher) {
        switch self {
        case .literal(let lit):
            hasher.combine(0)
            hasher.combine(lit)
        case .column(let col):
            hasher.combine(1)
            hasher.combine(col)
        case .variable(let v):
            hasher.combine(2)
            hasher.combine(v)
        case .add(let l, let r):
            hasher.combine(3)
            hasher.combine(l)
            hasher.combine(r)
        case .subtract(let l, let r):
            hasher.combine(4)
            hasher.combine(l)
            hasher.combine(r)
        case .multiply(let l, let r):
            hasher.combine(5)
            hasher.combine(l)
            hasher.combine(r)
        case .divide(let l, let r):
            hasher.combine(6)
            hasher.combine(l)
            hasher.combine(r)
        case .modulo(let l, let r):
            hasher.combine(7)
            hasher.combine(l)
            hasher.combine(r)
        case .negate(let e):
            hasher.combine(8)
            hasher.combine(e)
        case .equal(let l, let r):
            hasher.combine(9)
            hasher.combine(l)
            hasher.combine(r)
        case .notEqual(let l, let r):
            hasher.combine(10)
            hasher.combine(l)
            hasher.combine(r)
        case .lessThan(let l, let r):
            hasher.combine(11)
            hasher.combine(l)
            hasher.combine(r)
        case .lessThanOrEqual(let l, let r):
            hasher.combine(12)
            hasher.combine(l)
            hasher.combine(r)
        case .greaterThan(let l, let r):
            hasher.combine(13)
            hasher.combine(l)
            hasher.combine(r)
        case .greaterThanOrEqual(let l, let r):
            hasher.combine(14)
            hasher.combine(l)
            hasher.combine(r)
        case .and(let l, let r):
            hasher.combine(15)
            hasher.combine(l)
            hasher.combine(r)
        case .or(let l, let r):
            hasher.combine(16)
            hasher.combine(l)
            hasher.combine(r)
        case .not(let e):
            hasher.combine(17)
            hasher.combine(e)
        case .isNull(let e):
            hasher.combine(18)
            hasher.combine(e)
        case .isNotNull(let e):
            hasher.combine(19)
            hasher.combine(e)
        case .bound(let v):
            hasher.combine(20)
            hasher.combine(v)
        case .like(let e, let pattern):
            hasher.combine(21)
            hasher.combine(e)
            hasher.combine(pattern)
        case .regex(let e, let pattern, let flags):
            hasher.combine(22)
            hasher.combine(e)
            hasher.combine(pattern)
            hasher.combine(flags)
        case .between(let e, let low, let high):
            hasher.combine(23)
            hasher.combine(e)
            hasher.combine(low)
            hasher.combine(high)
        case .inList(let e, let values):
            hasher.combine(24)
            hasher.combine(e)
            for v in values {
                hasher.combine(v)
            }
        case .inSubquery(let e, let subquery):
            hasher.combine(25)
            hasher.combine(e)
            hasher.combine(subquery)
        case .aggregate(let agg):
            hasher.combine(26)
            hasher.combine(agg)
        case .function(let func_):
            hasher.combine(27)
            hasher.combine(func_)
        case .caseWhen(let cases, let elseResult):
            hasher.combine(28)
            for (condition, result) in cases {
                hasher.combine(condition)
                hasher.combine(result)
            }
            hasher.combine(elseResult)
        case .coalesce(let exprs):
            hasher.combine(29)
            for e in exprs {
                hasher.combine(e)
            }
        case .nullIf(let l, let r):
            hasher.combine(30)
            hasher.combine(l)
            hasher.combine(r)
        case .cast(let e, let targetType):
            hasher.combine(31)
            hasher.combine(e)
            hasher.combine(targetType)
        case .triple(let subject, let predicate, let object):
            hasher.combine(32)
            hasher.combine(subject)
            hasher.combine(predicate)
            hasher.combine(object)
        case .isTriple(let e):
            hasher.combine(33)
            hasher.combine(e)
        case .subject(let e):
            hasher.combine(34)
            hasher.combine(e)
        case .predicate(let e):
            hasher.combine(35)
            hasher.combine(e)
        case .object(let e):
            hasher.combine(36)
            hasher.combine(e)
        case .subquery(let query):
            hasher.combine(37)
            hasher.combine(query)
        case .exists(let query):
            hasher.combine(38)
            hasher.combine(query)
        }
    }
}
