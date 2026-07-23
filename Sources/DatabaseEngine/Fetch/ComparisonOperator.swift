/// Comparison operators used by typed fetch predicates.
public enum ComparisonOperator: String, Sendable, Hashable {
    case equal = "=="
    case notEqual = "!="
    case lessThan = "<"
    case lessThanOrEqual = "<="
    case greaterThan = ">"
    case greaterThanOrEqual = ">="
    case contains = "contains"
    case hasPrefix = "hasPrefix"
    case hasSuffix = "hasSuffix"
    case `in` = "in"
    case notIn = "notIn"
    case isNil = "isNil"
    case isNotNil = "isNotNil"
}
