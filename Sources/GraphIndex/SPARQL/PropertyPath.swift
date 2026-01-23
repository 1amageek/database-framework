// PropertyPath.swift
// GraphIndex - SPARQL Property Paths
//
// Represents property path expressions for SPARQL 1.1.
// Reference: W3C SPARQL 1.1 Property Paths (https://www.w3.org/TR/sparql11-property-paths/)

import Foundation

/// Property path expression for SPARQL queries
///
/// **Design**: Recursive enum representing property path algebra.
/// Supports all SPARQL 1.1 property path constructors.
///
/// **Usage**:
/// ```swift
/// // Simple IRI path
/// let knows = PropertyPath.iri("knows")
///
/// // Inverse path: ^knows
/// let knownBy = PropertyPath.inverse(.iri("knows"))
///
/// // Sequence path: knows/worksAt
/// let colleagues = PropertyPath.sequence(.iri("knows"), .iri("worksAt"))
///
/// // Transitive closure: knows+
/// let knowsChain = PropertyPath.oneOrMore(.iri("knows"))
///
/// // Optional path: knows?
/// let maybeKnows = PropertyPath.zeroOrOne(.iri("knows"))
///
/// // Alternative: knows|friendOf
/// let related = PropertyPath.alternative(.iri("knows"), .iri("friendOf"))
/// ```
///
/// **Reference**: W3C SPARQL 1.1, Section 9 (Property Paths)
public indirect enum PropertyPath: Sendable, Hashable {

    // MARK: - Atomic Paths

    /// Simple IRI property (predicate)
    ///
    /// Matches a single edge with the given predicate.
    /// ```sparql
    /// ?s ex:knows ?o
    /// ```
    case iri(String)

    /// Negated property set
    ///
    /// Matches any edge NOT in the given set.
    /// ```sparql
    /// ?s !(ex:knows|ex:hates) ?o
    /// ```
    case negatedPropertySet([String])

    // MARK: - Path Constructors

    /// Inverse path: ^path
    ///
    /// Reverses the direction of traversal.
    /// ```sparql
    /// ?s ^ex:knows ?o  -- equivalent to ?o ex:knows ?s
    /// ```
    case inverse(PropertyPath)

    /// Sequence path: path1/path2
    ///
    /// Concatenates two paths.
    /// ```sparql
    /// ?s ex:knows/ex:worksAt ?o
    /// ```
    case sequence(PropertyPath, PropertyPath)

    /// Alternative path: path1|path2
    ///
    /// Matches either path.
    /// ```sparql
    /// ?s ex:knows|ex:friendOf ?o
    /// ```
    case alternative(PropertyPath, PropertyPath)

    // MARK: - Quantified Paths

    /// Zero or more: path*
    ///
    /// Matches zero or more repetitions of the path.
    /// ```sparql
    /// ?s ex:knows* ?o  -- transitive closure including self
    /// ```
    case zeroOrMore(PropertyPath)

    /// One or more: path+
    ///
    /// Matches one or more repetitions of the path.
    /// ```sparql
    /// ?s ex:knows+ ?o  -- transitive closure (at least one hop)
    /// ```
    case oneOrMore(PropertyPath)

    /// Zero or one: path?
    ///
    /// Matches zero or one occurrence of the path.
    /// ```sparql
    /// ?s ex:knows? ?o  -- direct neighbor or self
    /// ```
    case zeroOrOne(PropertyPath)

    // MARK: - Properties

    /// Whether this path requires recursive/iterative evaluation
    public var isRecursive: Bool {
        switch self {
        case .iri, .negatedPropertySet:
            return false
        case .inverse(let path):
            return path.isRecursive
        case .sequence(let p1, let p2):
            return p1.isRecursive || p2.isRecursive
        case .alternative(let p1, let p2):
            return p1.isRecursive || p2.isRecursive
        case .zeroOrMore, .oneOrMore, .zeroOrOne:
            return true
        }
    }

    /// Whether this path is a simple IRI (no operators)
    public var isSimpleIRI: Bool {
        if case .iri = self { return true }
        return false
    }

    /// Get the IRI if this is a simple path
    public var simpleIRI: String? {
        if case .iri(let value) = self { return value }
        return nil
    }

    /// All IRIs used in this path
    public var allIRIs: Set<String> {
        switch self {
        case .iri(let value):
            return [value]
        case .negatedPropertySet(let iris):
            return Set(iris)
        case .inverse(let path):
            return path.allIRIs
        case .sequence(let p1, let p2), .alternative(let p1, let p2):
            return p1.allIRIs.union(p2.allIRIs)
        case .zeroOrMore(let path), .oneOrMore(let path), .zeroOrOne(let path):
            return path.allIRIs
        }
    }

    /// Estimated complexity for query planning (higher = more expensive)
    public var complexityEstimate: Int {
        switch self {
        case .iri:
            return 1
        case .negatedPropertySet:
            return 10  // Requires scanning all edges
        case .inverse(let path):
            return path.complexityEstimate  // Same as forward with different index
        case .sequence(let p1, let p2):
            return p1.complexityEstimate * p2.complexityEstimate
        case .alternative(let p1, let p2):
            return p1.complexityEstimate + p2.complexityEstimate
        case .zeroOrMore(let path), .oneOrMore(let path):
            return path.complexityEstimate * 100  // Unbounded iteration
        case .zeroOrOne(let path):
            return path.complexityEstimate + 1
        }
    }

    // MARK: - Normalization

    /// Normalize the path for optimization
    ///
    /// Simplifies equivalent expressions:
    /// - ^^p = p (double inverse)
    /// - p* = p+|ε
    /// - Flattens nested alternatives
    public func normalized() -> PropertyPath {
        switch self {
        case .iri, .negatedPropertySet:
            return self

        case .inverse(let inner):
            // ^^p = p
            if case .inverse(let innerInner) = inner {
                return innerInner.normalized()
            }
            return .inverse(inner.normalized())

        case .sequence(let p1, let p2):
            return PropertyPath.sequence(p1.normalized(), p2.normalized())

        case .alternative(let p1, let p2):
            // Flatten nested alternatives
            let norm1 = p1.normalized()
            let norm2 = p2.normalized()

            var alternatives: [PropertyPath] = []
            if case .alternative(let a, let b) = norm1 {
                alternatives.append(a)
                alternatives.append(b)
            } else {
                alternatives.append(norm1)
            }
            if case .alternative(let a, let b) = norm2 {
                alternatives.append(a)
                alternatives.append(b)
            } else {
                alternatives.append(norm2)
            }

            // Rebuild as right-associative chain
            return alternatives.dropFirst().reduce(alternatives.first!) { acc, next in
                PropertyPath.alternative(acc, next)
            }

        case .zeroOrMore(let path):
            return .zeroOrMore(path.normalized())

        case .oneOrMore(let path):
            return .oneOrMore(path.normalized())

        case .zeroOrOne(let path):
            return .zeroOrOne(path.normalized())
        }
    }
}

// MARK: - CustomStringConvertible

extension PropertyPath: CustomStringConvertible {
    public var description: String {
        switch self {
        case .iri(let value):
            return value
        case .negatedPropertySet(let iris):
            return "!(\(iris.joined(separator: "|")))"
        case .inverse(let path):
            return "^\(path.parenthesizedIfComplex)"
        case .sequence(let p1, let p2):
            return "\(p1.parenthesizedIfComplex)/\(p2.parenthesizedIfComplex)"
        case .alternative(let p1, let p2):
            return "\(p1)|\(p2)"
        case .zeroOrMore(let path):
            return "\(path.parenthesizedIfComplex)*"
        case .oneOrMore(let path):
            return "\(path.parenthesizedIfComplex)+"
        case .zeroOrOne(let path):
            return "\(path.parenthesizedIfComplex)?"
        }
    }

    private var parenthesizedIfComplex: String {
        switch self {
        case .iri, .negatedPropertySet:
            return description
        default:
            return "(\(description))"
        }
    }
}

// MARK: - Builder Pattern

extension PropertyPath {
    /// Create an inverse path
    public func inverted() -> PropertyPath {
        .inverse(self)
    }

    /// Create a sequence with another path
    public func then(_ other: PropertyPath) -> PropertyPath {
        PropertyPath.sequence(self, other)
    }

    /// Create an alternative with another path
    public func or(_ other: PropertyPath) -> PropertyPath {
        PropertyPath.alternative(self, other)
    }

    /// Create zero-or-more repetition
    public func star() -> PropertyPath {
        .zeroOrMore(self)
    }

    /// Create one-or-more repetition
    public func plus() -> PropertyPath {
        .oneOrMore(self)
    }

    /// Create zero-or-one repetition
    public func optional() -> PropertyPath {
        .zeroOrOne(self)
    }
}

// MARK: - Convenience Initializers

extension PropertyPath {
    /// Create a sequence from multiple paths
    public static func sequencePaths(_ paths: PropertyPath...) -> PropertyPath {
        guard !paths.isEmpty else {
            fatalError("PropertyPath.sequencePaths requires at least one path")
        }
        return paths.dropFirst().reduce(paths.first!) { acc, next in
            PropertyPath.sequence(acc, next)
        }
    }

    /// Create an alternative from multiple paths
    public static func alternativePaths(_ paths: PropertyPath...) -> PropertyPath {
        guard !paths.isEmpty else {
            fatalError("PropertyPath.alternativePaths requires at least one path")
        }
        return paths.dropFirst().reduce(paths.first!) { acc, next in
            PropertyPath.alternative(acc, next)
        }
    }
}

// MARK: - Property Path Configuration

/// Configuration for property path evaluation
public struct PropertyPathConfiguration: Sendable {
    /// Maximum depth for recursive paths (zeroOrMore, oneOrMore)
    ///
    /// Default is 10 to prevent infinite loops and memory exhaustion.
    public var maxDepth: Int

    /// Whether to detect and avoid cycles
    ///
    /// When true, visited nodes are tracked to avoid infinite loops.
    /// Default is true.
    public var detectCycles: Bool

    /// Maximum number of results per path evaluation
    ///
    /// Default is 10000.
    public var maxResults: Int

    /// Default configuration
    public static let `default` = PropertyPathConfiguration(
        maxDepth: 10,
        detectCycles: true,
        maxResults: 10000
    )

    public init(
        maxDepth: Int = 10,
        detectCycles: Bool = true,
        maxResults: Int = 10000
    ) {
        self.maxDepth = maxDepth
        self.detectCycles = detectCycles
        self.maxResults = maxResults
    }
}
