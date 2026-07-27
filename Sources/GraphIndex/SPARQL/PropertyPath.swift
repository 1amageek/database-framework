// ExecutionPropertyPath.swift
// GraphIndex - SPARQL Property Paths
//
// Represents property path expressions for SPARQL 1.1.
// Reference: W3C SPARQL 1.1 Property Paths (https://www.w3.org/TR/sparql11-property-paths/)

import DatabaseTypes
import DatabaseKit

/// Property path expression for SPARQL queries
///
/// **Design**: Recursive enum representing property path algebra.
/// Supports all SPARQL 1.1 property path constructors.
///
/// **Usage**:
/// ```swift
/// // Simple IRI path
/// let knowsIRI = try RDFPredicateIRI("https://example.com/knows")
/// let knows = ExecutionPropertyPath.iri(knowsIRI)
///
/// // Inverse path: ^knows
/// let knownBy = ExecutionPropertyPath.inverse(knows)
///
/// // Sequence path: knows/worksAt
/// let worksAtIRI = try RDFPredicateIRI("https://example.com/worksAt")
/// let colleagues = ExecutionPropertyPath.sequence(knows, .iri(worksAtIRI))
///
/// // Transitive closure: knows+
/// let knowsChain = ExecutionPropertyPath.oneOrMore(knows)
///
/// // Optional path: knows?
/// let maybeKnows = ExecutionPropertyPath.zeroOrOne(knows)
///
/// // Alternative: knows|friendOf
/// let friendOfIRI = try RDFPredicateIRI("https://example.com/friendOf")
/// let related = ExecutionPropertyPath.alternative(knows, .iri(friendOfIRI))
/// ```
///
/// **Reference**: W3C SPARQL 1.1, Section 9 (Property Paths)
public indirect enum ExecutionPropertyPath: Sendable, Hashable {

    // MARK: - Atomic Paths

    /// Empty path (identity - matches subject == object)
    ///
    /// Used as a degenerate case for operations on empty path lists.
    /// Semantically equivalent to zero-length path.
    case empty

    /// Simple IRI property (predicate)
    ///
    /// Matches a single edge with the given predicate.
    /// ```sparql
    /// ?s ex:knows ?o
    /// ```
    case iri(RDFPredicateIRI)

    /// Negated property set
    ///
    /// Matches any edge NOT in the given set.
    /// ```sparql
    /// ?s !(ex:knows|ex:hates) ?o
    /// ```
    case negatedPropertySet(PropertyPathNegatedSet)

    // MARK: - Path Constructors

    /// Inverse path: ^path
    ///
    /// Reverses the direction of traversal.
    /// ```sparql
    /// ?s ^ex:knows ?o  -- equivalent to ?o ex:knows ?s
    /// ```
    case inverse(ExecutionPropertyPath)

    /// Sequence path: path1/path2
    ///
    /// Concatenates two paths.
    /// ```sparql
    /// ?s ex:knows/ex:worksAt ?o
    /// ```
    case sequence(ExecutionPropertyPath, ExecutionPropertyPath)

    /// Alternative path: path1|path2
    ///
    /// Matches either path.
    /// ```sparql
    /// ?s ex:knows|ex:friendOf ?o
    /// ```
    case alternative(ExecutionPropertyPath, ExecutionPropertyPath)

    // MARK: - Quantified Paths

    /// Zero or more: path*
    ///
    /// Matches zero or more repetitions of the path.
    /// ```sparql
    /// ?s ex:knows* ?o  -- transitive closure including self
    /// ```
    case zeroOrMore(ExecutionPropertyPath)

    /// One or more: path+
    ///
    /// Matches one or more repetitions of the path.
    /// ```sparql
    /// ?s ex:knows+ ?o  -- transitive closure (at least one hop)
    /// ```
    case oneOrMore(ExecutionPropertyPath)

    /// Zero or one: path?
    ///
    /// Matches zero or one occurrence of the path.
    /// ```sparql
    /// ?s ex:knows? ?o  -- direct neighbor or self
    /// ```
    case zeroOrOne(ExecutionPropertyPath)

    /// A validated finite or lower-bounded repetition range.
    case range(ExecutionPropertyPath, PropertyPathRange)

    // MARK: - Properties

    /// Whether this path requires recursive/iterative evaluation
    public var isRecursive: Bool {
        switch self {
        case .empty, .iri, .negatedPropertySet:
            return false
        case .inverse(let path):
            return path.isRecursive
        case .sequence(let p1, let p2):
            return p1.isRecursive || p2.isRecursive
        case .alternative(let p1, let p2):
            return p1.isRecursive || p2.isRecursive
        case .zeroOrMore, .oneOrMore, .range:
            return true
        case .zeroOrOne:
            return false
        }
    }

    /// Whether this path is a simple IRI (no operators)
    public var isSimpleIRI: Bool {
        if case .iri = self { return true }
        return false
    }

    /// Maximum number of nested path constructors below the root expression.
    public var nestingDepth: Int {
        var maximum = 0
        var pending: [(path: ExecutionPropertyPath, depth: Int)] = [(self, 0)]

        while let current = pending.popLast() {
            maximum = max(maximum, current.depth)
            let childDepth = current.depth == Int.max
                ? Int.max
                : current.depth + 1
            switch current.path {
            case .empty, .iri, .negatedPropertySet:
                break
            case .inverse(let path),
                 .zeroOrMore(let path),
                 .oneOrMore(let path),
                 .zeroOrOne(let path),
                 .range(let path, _):
                pending.append((path, childDepth))
            case .sequence(let left, let right),
                 .alternative(let left, let right):
                pending.append((left, childDepth))
                pending.append((right, childDepth))
            }
        }

        return maximum
    }

    /// Get the IRI if this is a simple path
    public var simpleIRI: RDFPredicateIRI? {
        if case .iri(let value) = self { return value }
        return nil
    }

    /// All IRIs used in this path
    public var allIRIs: Set<RDFPredicateIRI> {
        switch self {
        case .empty:
            return []
        case .iri(let value):
            return [value]
        case .negatedPropertySet(let exclusions):
            return (exclusions.forward ?? []).union(exclusions.inverse ?? [])
        case .inverse(let path):
            return path.allIRIs
        case .sequence(let p1, let p2), .alternative(let p1, let p2):
            return p1.allIRIs.union(p2.allIRIs)
        case .zeroOrMore(let path), .oneOrMore(let path), .zeroOrOne(let path):
            return path.allIRIs
        case .range(let path, _):
            return path.allIRIs
        }
    }

    /// All IRIs expanded with ontology sub-property knowledge
    ///
    /// Like `allIRIs`, but for each IRI, also includes all sub-property IRIs
    /// from the ontology context. Used for query planning and optimization.
    ///
    /// F-7: Extends literal-only extraction with ontology-based expansion.
    ///
    /// - Parameter context: The ontology context for sub-property resolution
    /// - Returns: Set of all IRIs including sub-property expansions
    public func expandedIRIs(using context: OntologyContext) -> Set<String> {
        var result = Set<String>()
        for iri in allIRIs {
            result.formUnion(context.expandedProperties(of: iri.rawValue))
        }
        return result
    }

    /// Estimated complexity for query planning (higher = more expensive)
    public var complexityEstimate: Int {
        switch self {
        case .empty:
            return 0  // Identity path
        case .iri:
            return 1
        case .negatedPropertySet:
            return 10  // Requires scanning all edges
        case .inverse(let path):
            return path.complexityEstimate + 1  // Requires reverse index lookup
        case .sequence(let p1, let p2):
            return p1.complexityEstimate + p2.complexityEstimate
        case .alternative(let p1, let p2):
            return p1.complexityEstimate + p2.complexityEstimate
        case .zeroOrMore(let path), .oneOrMore(let path):
            return path.complexityEstimate * 100  // Unbounded iteration
        case .zeroOrOne(let path):
            return path.complexityEstimate + 1
        case .range(let path, let bounds):
            return path.complexityEstimate * (bounds.maximum ?? 100)
        }
    }

    // MARK: - Normalization

    /// Normalize the path for optimization
    ///
    /// Simplifies equivalent expressions:
    /// - ^^p = p (double inverse)
    /// - p* = p+|ε
    /// - Flattens nested alternatives
    public func normalized() -> ExecutionPropertyPath {
        switch self {
        case .empty, .iri, .negatedPropertySet:
            return self

        case .inverse(let inner):
            let norm = inner.normalized()
            switch norm {
            case .inverse(let p):
                // ^^p = p — double inverse cancels
                return p
            case .sequence(let p1, let p2):
                // ^(p1/p2) = (^p2)/(^p1) — SPARQL 1.1 Section 18.4
                return .sequence(.inverse(p2).normalized(), .inverse(p1).normalized())
            case .alternative(let p1, let p2):
                // ^(p1|p2) = (^p1)|(^p2) — distribute inverse over alternative
                return .alternative(.inverse(p1).normalized(), .inverse(p2).normalized())
            case .negatedPropertySet(let exclusions):
                return .negatedPropertySet(exclusions.reversed)
            case .zeroOrMore(let path):
                return .zeroOrMore(.inverse(path).normalized())
            case .oneOrMore(let path):
                return .oneOrMore(.inverse(path).normalized())
            case .zeroOrOne(let path):
                return .zeroOrOne(.inverse(path).normalized())
            case .range(let path, let bounds):
                return .range(.inverse(path).normalized(), bounds)
            default:
                return .inverse(norm)
            }

        case .sequence(let p1, let p2):
            return ExecutionPropertyPath.sequence(p1.normalized(), p2.normalized())

        case .alternative(let p1, let p2):
            // Flatten nested alternatives
            let norm1 = p1.normalized()
            let norm2 = p2.normalized()

            var alternatives: [ExecutionPropertyPath] = []
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
            guard let last = alternatives.last else { return .empty }
            return alternatives.dropLast().reversed().reduce(last) { acc, next in
                ExecutionPropertyPath.alternative(next, acc)
            }

        case .zeroOrMore(let path):
            return .zeroOrMore(path.normalized())

        case .oneOrMore(let path):
            return .oneOrMore(path.normalized())

        case .zeroOrOne(let path):
            return .zeroOrOne(path.normalized())

        case .range(let path, let bounds):
            return .range(path.normalized(), bounds)
        }
    }
}

// MARK: - CustomStringConvertible

extension ExecutionPropertyPath: CustomStringConvertible {
    public var description: String {
        switch self {
        case .empty:
            return "()"  // Empty path representation
        case .iri(let value):
            return value.rawValue
        case .negatedPropertySet(let exclusions):
            var values = (exclusions.forward ?? []).sorted().map(\.rawValue)
            values.append(contentsOf: (exclusions.inverse ?? []).sorted().map {
                "^\($0.rawValue)"
            })
            if values.isEmpty {
                switch (exclusions.forward != nil, exclusions.inverse != nil) {
                case (true, false):
                    return "!()"
                case (false, true):
                    return "!^()"
                case (true, true):
                    return "!(()|^())"
                case (false, false):
                    return "!()"
                }
            }
            if values.count == 1 {
                return "!\(values[0])"
            }
            return "!(\(values.joined(separator: "|")))"
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
        case .range(let path, let bounds):
            let maximum = bounds.maximum.map(String.init) ?? ""
            return "\(path.parenthesizedIfComplex){\(bounds.minimum),\(maximum)}"
        }
    }

    private var parenthesizedIfComplex: String {
        switch self {
        case .empty, .iri, .negatedPropertySet:
            return description
        default:
            return "(\(description))"
        }
    }
}

// MARK: - Builder Pattern

extension ExecutionPropertyPath {
    /// Create an inverse path
    public func inverted() -> ExecutionPropertyPath {
        .inverse(self)
    }

    /// Create a sequence with another path
    public func then(_ other: ExecutionPropertyPath) -> ExecutionPropertyPath {
        ExecutionPropertyPath.sequence(self, other)
    }

    /// Create an alternative with another path
    public func or(_ other: ExecutionPropertyPath) -> ExecutionPropertyPath {
        ExecutionPropertyPath.alternative(self, other)
    }

    /// Create zero-or-more repetition
    public func star() -> ExecutionPropertyPath {
        .zeroOrMore(self)
    }

    /// Create one-or-more repetition
    public func plus() -> ExecutionPropertyPath {
        .oneOrMore(self)
    }

    /// Create zero-or-one repetition
    public func optional() -> ExecutionPropertyPath {
        .zeroOrOne(self)
    }
}

// MARK: - Convenience Initializers

extension ExecutionPropertyPath {
    /// Create a sequence from multiple paths
    public static func sequencePaths(_ paths: ExecutionPropertyPath...) -> ExecutionPropertyPath {
        guard let first = paths.first else { return .empty }
        return paths.dropFirst().reduce(first) { acc, next in
            ExecutionPropertyPath.sequence(acc, next)
        }
    }

    /// Create an alternative from multiple paths
    public static func alternativePaths(_ paths: ExecutionPropertyPath...) -> ExecutionPropertyPath {
        guard let first = paths.first else { return .empty }
        return paths.dropFirst().reduce(first) { acc, next in
            ExecutionPropertyPath.alternative(acc, next)
        }
    }
}

// MARK: - Property Path Configuration

/// Configuration for property path evaluation
public struct ExecutionPropertyPathConfiguration: Sendable {
    /// Maximum nesting depth of the property-path expression tree.
    public let maximumExpressionDepth: Int

    /// Maximum graph traversal depth for recursive paths.
    public let maximumTraversalDepth: Int

    /// Whether to detect and avoid cycles
    ///
    /// When true, visited nodes are tracked to avoid infinite loops.
    /// Default is true.
    public let detectCycles: Bool

    /// Maximum number of results per path evaluation
    ///
    /// Default is 10000.
    public let maximumResults: Int

    /// Default configuration
    public static let `default` = ExecutionPropertyPathConfiguration(
        maximumExpressionDepth: 100,
        maximumTraversalDepth: 100,
        detectCycles: true,
        maximumResults: 10000
    )

    public init(
        maximumExpressionDepth: Int = 100,
        maximumTraversalDepth: Int = 100,
        detectCycles: Bool = true,
        maximumResults: Int = 10000
    ) {
        self.maximumExpressionDepth = maximumExpressionDepth
        self.maximumTraversalDepth = maximumTraversalDepth
        self.detectCycles = detectCycles
        self.maximumResults = maximumResults
    }
}
