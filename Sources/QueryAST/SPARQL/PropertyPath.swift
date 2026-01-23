/// PropertyPath.swift
/// SPARQL 1.1/1.2 Property Path types
///
/// Reference:
/// - W3C SPARQL 1.1 Property Paths
/// - W3C SPARQL 1.2 (Draft) - Extended Property Paths

import Foundation

// Note: Core PropertyPath enum is defined in DataSource.swift
// This file provides additional utilities and extensions.

// MARK: - PropertyPath Builders

extension PropertyPath {
    /// Create a simple IRI path
    public static func uri(_ iri: String) -> PropertyPath {
        .iri(iri)
    }

    /// Create an inverse path: ^path
    public static func inv(_ path: PropertyPath) -> PropertyPath {
        .inverse(path)
    }

    /// Create a sequence path: path1 / path2
    public static func seq(_ paths: PropertyPath...) -> PropertyPath {
        guard !paths.isEmpty else { fatalError("Empty path sequence") }
        return paths.dropFirst().reduce(paths[0]) { .sequence($0, $1) }
    }

    /// Create an alternative path: path1 | path2
    public static func alt(_ paths: PropertyPath...) -> PropertyPath {
        guard !paths.isEmpty else { fatalError("Empty path alternative") }
        return paths.dropFirst().reduce(paths[0]) { .alternative($0, $1) }
    }

    /// Create a zero-or-more path: path*
    public static func star(_ path: PropertyPath) -> PropertyPath {
        .zeroOrMore(path)
    }

    /// Create a one-or-more path: path+
    public static func plus(_ path: PropertyPath) -> PropertyPath {
        .oneOrMore(path)
    }

    /// Create a zero-or-one path: path?
    public static func opt(_ path: PropertyPath) -> PropertyPath {
        .zeroOrOne(path)
    }

    /// Create a negation path: !path
    public static func neg(_ iris: String...) -> PropertyPath {
        .negation(iris)
    }

    /// Create a ranged path: path{min,max}
    public static func ranged(_ path: PropertyPath, min: Int? = nil, max: Int? = nil) -> PropertyPath {
        .range(path, min: min, max: max)
    }
}

// MARK: - PropertyPath Analysis

extension PropertyPath {
    /// Returns all IRIs used in this path
    public var iris: Set<String> {
        var result = Set<String>()
        collectIRIs(into: &result)
        return result
    }

    private func collectIRIs(into result: inout Set<String>) {
        switch self {
        case .iri(let iri):
            result.insert(iri)
        case .inverse(let path):
            path.collectIRIs(into: &result)
        case .sequence(let left, let right):
            left.collectIRIs(into: &result)
            right.collectIRIs(into: &result)
        case .alternative(let left, let right):
            left.collectIRIs(into: &result)
            right.collectIRIs(into: &result)
        case .zeroOrMore(let path), .oneOrMore(let path), .zeroOrOne(let path):
            path.collectIRIs(into: &result)
        case .negation(let iris):
            result.formUnion(iris)
        case .range(let path, _, _):
            path.collectIRIs(into: &result)
        }
    }

    /// Returns true if this path contains any repetition operators (*, +, ?)
    public var hasRepetition: Bool {
        switch self {
        case .iri, .negation:
            return false
        case .inverse(let path):
            return path.hasRepetition
        case .sequence(let left, let right), .alternative(let left, let right):
            return left.hasRepetition || right.hasRepetition
        case .zeroOrMore, .oneOrMore, .zeroOrOne, .range:
            return true
        }
    }

    /// Returns true if this path can match zero-length paths
    public var canMatchEmpty: Bool {
        switch self {
        case .iri, .negation, .oneOrMore:
            return false
        case .inverse(let path):
            return path.canMatchEmpty
        case .sequence(let left, let right):
            return left.canMatchEmpty && right.canMatchEmpty
        case .alternative(let left, let right):
            return left.canMatchEmpty || right.canMatchEmpty
        case .zeroOrMore, .zeroOrOne:
            return true
        case .range(let path, let min, _):
            if let m = min, m > 0 {
                return false
            }
            return path.canMatchEmpty
        }
    }

    /// Returns the minimum path length
    public var minLength: Int {
        switch self {
        case .iri:
            return 1
        case .inverse(let path):
            return path.minLength
        case .sequence(let left, let right):
            return left.minLength + right.minLength
        case .alternative(let left, let right):
            return min(left.minLength, right.minLength)
        case .zeroOrMore, .zeroOrOne:
            return 0
        case .oneOrMore(let path):
            return path.minLength
        case .negation:
            return 1
        case .range(let path, let minVal, _):
            return path.minLength * (minVal ?? 0)
        }
    }

    /// Returns the maximum path length (nil if unbounded)
    public var maxLength: Int? {
        switch self {
        case .iri, .negation:
            return 1
        case .inverse(let path):
            return path.maxLength
        case .sequence(let left, let right):
            guard let l = left.maxLength, let r = right.maxLength else { return nil }
            return l + r
        case .alternative(let left, let right):
            guard let l = left.maxLength, let r = right.maxLength else { return nil }
            return max(l, r)
        case .zeroOrMore, .oneOrMore:
            return nil
        case .zeroOrOne(let path):
            return path.maxLength
        case .range(let path, _, let maxVal):
            guard let pathMax = path.maxLength, let m = maxVal else { return nil }
            return pathMax * m
        }
    }

    /// Returns true if this path is unbounded
    public var isUnbounded: Bool {
        maxLength == nil
    }

    /// Complexity estimate for query optimization
    public var complexity: Int {
        switch self {
        case .iri, .negation:
            return 1
        case .inverse(let path):
            return path.complexity
        case .sequence(let left, let right):
            return left.complexity + right.complexity
        case .alternative(let left, let right):
            return left.complexity + right.complexity
        case .zeroOrMore(let path), .oneOrMore(let path):
            return path.complexity * 10  // High complexity for unbounded
        case .zeroOrOne(let path):
            return path.complexity * 2
        case .range(let path, _, let maxVal):
            return path.complexity * (maxVal ?? 10)
        }
    }
}

// MARK: - PropertyPath Transformations

extension PropertyPath {
    /// Reverse the path direction
    public func reversed() -> PropertyPath {
        switch self {
        case .iri(let iri):
            return .inverse(.iri(iri))
        case .inverse(let path):
            return path  // Double inverse cancels out
        case .sequence(let left, let right):
            return .sequence(right.reversed(), left.reversed())
        case .alternative(let left, let right):
            return .alternative(left.reversed(), right.reversed())
        case .zeroOrMore(let path):
            return .zeroOrMore(path.reversed())
        case .oneOrMore(let path):
            return .oneOrMore(path.reversed())
        case .zeroOrOne(let path):
            return .zeroOrOne(path.reversed())
        case .negation(let iris):
            // Negation of inverse
            return .negation(iris)  // TODO: Handle inverse negation properly
        case .range(let path, let minVal, let maxVal):
            return .range(path.reversed(), min: minVal, max: maxVal)
        }
    }

    /// Simplify the path expression
    public func simplified() -> PropertyPath {
        switch self {
        case .iri, .negation:
            return self

        case .inverse(let path):
            let simplified = path.simplified()
            // Double inverse cancels out
            if case .inverse(let inner) = simplified {
                return inner
            }
            return .inverse(simplified)

        case .sequence(let left, let right):
            let l = left.simplified()
            let r = right.simplified()
            // Flatten nested sequences
            return .sequence(l, r)

        case .alternative(let left, let right):
            let l = left.simplified()
            let r = right.simplified()
            // Remove duplicate alternatives
            if l == r { return l }
            return .alternative(l, r)

        case .zeroOrMore(let path):
            let simplified = path.simplified()
            // (a*)* = a*
            if case .zeroOrMore = simplified {
                return simplified
            }
            // (a+)* = a*
            if case .oneOrMore(let inner) = simplified {
                return .zeroOrMore(inner)
            }
            return .zeroOrMore(simplified)

        case .oneOrMore(let path):
            let simplified = path.simplified()
            // (a+)+ = a+
            if case .oneOrMore = simplified {
                return simplified
            }
            // (a*)+ = a*
            if case .zeroOrMore = simplified {
                return simplified
            }
            return .oneOrMore(simplified)

        case .zeroOrOne(let path):
            let simplified = path.simplified()
            // (a?)? = a?
            if case .zeroOrOne = simplified {
                return simplified
            }
            return .zeroOrOne(simplified)

        case .range(let path, let minVal, let maxVal):
            let simplified = path.simplified()
            // {1,1} = plain path
            if minVal == 1 && maxVal == 1 {
                return simplified
            }
            // {0,1} = ?
            if minVal == 0 && maxVal == 1 {
                return .zeroOrOne(simplified)
            }
            // {1,} = +
            if minVal == 1 && maxVal == nil {
                return .oneOrMore(simplified)
            }
            // {0,} = *
            if minVal == 0 && maxVal == nil {
                return .zeroOrMore(simplified)
            }
            return .range(simplified, min: minVal, max: maxVal)
        }
    }
}

// MARK: - SPARQL Serialization

extension PropertyPath {
    /// Generate SPARQL property path syntax
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        switch self {
        case .iri(let iri):
            // Try to use prefix
            for (prefix, base) in prefixes {
                if iri.hasPrefix(base) {
                    let local = String(iri.dropFirst(base.count))
                    return "\(prefix):\(local)"
                }
            }
            return "<\(iri)>"

        case .inverse(let path):
            return "^\(path.toSPARQL(prefixes: prefixes))"

        case .sequence(let left, let right):
            return "\(left.toSPARQL(prefixes: prefixes))/\(right.toSPARQL(prefixes: prefixes))"

        case .alternative(let left, let right):
            return "(\(left.toSPARQL(prefixes: prefixes))|\(right.toSPARQL(prefixes: prefixes)))"

        case .zeroOrMore(let path):
            return "\(wrapIfComplex(path, prefixes: prefixes))*"

        case .oneOrMore(let path):
            return "\(wrapIfComplex(path, prefixes: prefixes))+"

        case .zeroOrOne(let path):
            return "\(wrapIfComplex(path, prefixes: prefixes))?"

        case .negation(let iris):
            if iris.count == 1 {
                return "!\(formatIRI(iris[0], prefixes: prefixes))"
            }
            return "!(\(iris.map { formatIRI($0, prefixes: prefixes) }.joined(separator: "|")))"

        case .range(let path, let minVal, let maxVal):
            let pathStr = wrapIfComplex(path, prefixes: prefixes)
            let minStr = minVal.map(String.init) ?? ""
            let maxStr = maxVal.map(String.init) ?? ""
            return "\(pathStr){\(minStr),\(maxStr)}"
        }
    }

    private func wrapIfComplex(_ path: PropertyPath, prefixes: [String: String]) -> String {
        switch path {
        case .iri, .inverse, .negation:
            return path.toSPARQL(prefixes: prefixes)
        default:
            return "(\(path.toSPARQL(prefixes: prefixes)))"
        }
    }

    private func formatIRI(_ iri: String, prefixes: [String: String]) -> String {
        for (prefix, base) in prefixes {
            if iri.hasPrefix(base) {
                let local = String(iri.dropFirst(base.count))
                return "\(prefix):\(local)"
            }
        }
        return "<\(iri)>"
    }
}

