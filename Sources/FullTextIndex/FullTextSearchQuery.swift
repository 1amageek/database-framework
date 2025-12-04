// FullTextSearchQuery.swift
// FullTextIndex - Advanced query types for full-text search
//
// Reference: Lucene BooleanQuery, FDB Record Layer

import Foundation

// MARK: - FullTextSearchQuery

/// Full-text search query types (recursive ADT)
///
/// Supports Lucene-like query operations:
/// - Term and phrase queries
/// - Fuzzy matching with edit distance
/// - Prefix and wildcard patterns
/// - Boolean combinations (MUST/SHOULD/MUST_NOT)
/// - Score boosting
///
/// **Usage**:
/// ```swift
/// // Simple term search
/// let query: FullTextSearchQuery = .term("swift")
///
/// // Fuzzy search (typo tolerance)
/// let fuzzy: FullTextSearchQuery = .fuzzy("concurency", maxEdits: 2)
///
/// // Boolean query
/// let boolean: FullTextSearchQuery = .boolean([
///     .must(.term("swift")),
///     .should(.term("concurrency")),
///     .mustNot(.term("deprecated"))
/// ])
///
/// // Boosted query
/// let boosted: FullTextSearchQuery = .boosted(.term("important"), boost: 2.0)
/// ```
///
/// **Reference**: Lucene BooleanQuery, Elasticsearch Query DSL
public indirect enum FullTextSearchQuery: Sendable, Hashable {
    /// Single term search
    ///
    /// Matches documents containing the exact term (after analysis).
    case term(String)

    /// Phrase search with optional slop
    ///
    /// - Parameters:
    ///   - terms: Ordered terms to match
    ///   - slop: Maximum number of positions between terms (0 = exact phrase)
    ///
    /// **Example**:
    /// ```swift
    /// // Exact phrase: "quick brown fox"
    /// .phrase(["quick", "brown", "fox"], slop: 0)
    ///
    /// // Proximity: "quick" within 2 words of "fox"
    /// .phrase(["quick", "fox"], slop: 2)
    /// ```
    case phrase([String], slop: Int)

    /// Fuzzy term search with edit distance
    ///
    /// Uses Levenshtein distance for typo tolerance.
    ///
    /// - Parameters:
    ///   - term: The term to match
    ///   - maxEdits: Maximum edit distance (0-2, default: 2)
    ///   - prefixLength: Prefix that must match exactly (optimization)
    ///
    /// **Reference**: Lucene FuzzyQuery uses Levenshtein Automata
    /// http://blog.notdot.net/2010/07/Damn-Cool-Algorithms-Levenshtein-Automata
    case fuzzy(String, maxEdits: Int, prefixLength: Int)

    /// Prefix search
    ///
    /// Matches terms starting with the given prefix.
    ///
    /// **Example**: `.prefix("swi")` matches "swift", "switch", "swim"
    case prefix(String)

    /// Wildcard search
    ///
    /// Supports `*` (any characters) and `?` (single character).
    ///
    /// **Example**: `.wildcard("sw?ft")` matches "swift", "swaft"
    case wildcard(String)

    /// Boolean combination of queries
    ///
    /// Combines multiple queries with AND/OR/NOT logic.
    ///
    /// - Parameters:
    ///   - clauses: Array of boolean clauses
    ///   - minimumShouldMatch: Minimum number of SHOULD clauses that must match
    case boolean([BooleanClause], minimumShouldMatch: MinimumShouldMatch?)

    /// Boosted query
    ///
    /// Multiplies the score of matching documents by the boost factor.
    ///
    /// - Parameters:
    ///   - query: The query to boost
    ///   - boost: Score multiplier (default: 1.0)
    case boosted(FullTextSearchQuery, boost: Float)

    /// Range query on a field
    ///
    /// Matches documents where the field value falls within a range.
    case range(field: String, lower: String?, upper: String?, includeLower: Bool, includeUpper: Bool)

    // MARK: - Convenience Initializers

    /// Create a phrase query with default slop of 0
    public static func phrase(_ terms: [String]) -> FullTextSearchQuery {
        .phrase(terms, slop: 0)
    }

    /// Create a fuzzy query with default parameters
    ///
    /// Uses Lucene defaults: maxEdits=2, prefixLength=0
    public static func fuzzy(_ term: String) -> FullTextSearchQuery {
        .fuzzy(term, maxEdits: 2, prefixLength: 0)
    }

    /// Create a boolean query with minimum should match
    public static func boolean(_ clauses: [BooleanClause]) -> FullTextSearchQuery {
        .boolean(clauses, minimumShouldMatch: nil)
    }
}

// MARK: - BooleanClause

/// A clause in a boolean query
///
/// Represents how a sub-query participates in the boolean combination.
///
/// **Reference**: Lucene BooleanClause.Occur
public struct BooleanClause: Sendable, Hashable {
    /// The sub-query
    public let query: FullTextSearchQuery

    /// How this clause participates in matching
    public let occur: BooleanOccur

    public init(query: FullTextSearchQuery, occur: BooleanOccur) {
        self.query = query
        self.occur = occur
    }

    // MARK: - Convenience Factory Methods

    /// Create a MUST clause (required, contributes to score)
    public static func must(_ query: FullTextSearchQuery) -> BooleanClause {
        BooleanClause(query: query, occur: .must)
    }

    /// Create a SHOULD clause (optional, contributes to score)
    public static func should(_ query: FullTextSearchQuery) -> BooleanClause {
        BooleanClause(query: query, occur: .should)
    }

    /// Create a MUST_NOT clause (excluded)
    public static func mustNot(_ query: FullTextSearchQuery) -> BooleanClause {
        BooleanClause(query: query, occur: .mustNot)
    }

    /// Create a FILTER clause (required, no score contribution)
    public static func filter(_ query: FullTextSearchQuery) -> BooleanClause {
        BooleanClause(query: query, occur: .filter)
    }
}

// MARK: - BooleanOccur

/// How a clause participates in a boolean query
///
/// **Reference**: Lucene BooleanClause.Occur
public enum BooleanOccur: String, Sendable, Hashable, Codable {
    /// The clause must match (AND)
    ///
    /// Documents must match this clause to be included in results.
    /// Contributes to relevance score.
    case must

    /// The clause should match (OR)
    ///
    /// Documents matching this clause score higher.
    /// At least one SHOULD clause must match if no MUST clauses exist.
    case should

    /// The clause must not match (NOT)
    ///
    /// Documents matching this clause are excluded from results.
    case mustNot

    /// The clause must match but doesn't affect score
    ///
    /// Like MUST, but doesn't contribute to relevance score.
    /// Useful for filtering without affecting ranking.
    case filter
}

// MARK: - MinimumShouldMatch

/// Specification for minimum number of SHOULD clauses that must match
///
/// **Reference**: Elasticsearch minimum_should_match
public enum MinimumShouldMatch: Sendable, Hashable {
    /// Fixed number of clauses
    case fixed(Int)

    /// Percentage of SHOULD clauses (0.0 - 1.0)
    case percentage(Float)

    /// Combination: fixed + percentage
    case combined(fixed: Int, percentage: Float)

    /// Calculate actual minimum for given total SHOULD count
    public func calculate(totalShould: Int) -> Int {
        switch self {
        case .fixed(let n):
            return min(n, totalShould)
        case .percentage(let pct):
            return max(1, Int(Float(totalShould) * pct))
        case .combined(let fixed, let percentage):
            let fromPct = Int(Float(totalShould) * percentage)
            return min(fixed + fromPct, totalShould)
        }
    }
}

// MARK: - Query Utilities

extension FullTextSearchQuery {
    /// Check if this query contains any fuzzy components
    public var containsFuzzy: Bool {
        switch self {
        case .fuzzy:
            return true
        case .boolean(let clauses, _):
            return clauses.contains { $0.query.containsFuzzy }
        case .boosted(let query, _):
            return query.containsFuzzy
        default:
            return false
        }
    }

    /// Check if this query contains wildcards
    public var containsWildcard: Bool {
        switch self {
        case .wildcard, .prefix:
            return true
        case .boolean(let clauses, _):
            return clauses.contains { $0.query.containsWildcard }
        case .boosted(let query, _):
            return query.containsWildcard
        default:
            return false
        }
    }

    /// Extract all terms from the query (for highlighting)
    public var allTerms: [String] {
        switch self {
        case .term(let t):
            return [t]
        case .phrase(let terms, _):
            return terms
        case .fuzzy(let t, _, _):
            return [t]
        case .prefix(let p):
            return [p]
        case .wildcard(let w):
            return [w]
        case .boolean(let clauses, _):
            return clauses.flatMap { $0.query.allTerms }
        case .boosted(let query, _):
            return query.allTerms
        case .range:
            return []
        }
    }
}

// MARK: - CustomStringConvertible

extension FullTextSearchQuery: CustomStringConvertible {
    public var description: String {
        switch self {
        case .term(let t):
            return t
        case .phrase(let terms, let slop):
            let phrase = terms.joined(separator: " ")
            return slop > 0 ? "\"\(phrase)\"~\(slop)" : "\"\(phrase)\""
        case .fuzzy(let t, let edits, _):
            return "\(t)~\(edits)"
        case .prefix(let p):
            return "\(p)*"
        case .wildcard(let w):
            return w
        case .boolean(let clauses, let minMatch):
            let clauseStr = clauses.map { clause in
                switch clause.occur {
                case .must: return "+\(clause.query)"
                case .should: return "\(clause.query)"
                case .mustNot: return "-\(clause.query)"
                case .filter: return "#\(clause.query)"
                }
            }.joined(separator: " ")
            if let min = minMatch {
                return "(\(clauseStr))~\(min)"
            }
            return "(\(clauseStr))"
        case .boosted(let query, let boost):
            return "\(query)^\(boost)"
        case .range(let field, let lower, let upper, let inclLower, let inclUpper):
            let lBracket = inclLower ? "[" : "{"
            let rBracket = inclUpper ? "]" : "}"
            return "\(field):\(lBracket)\(lower ?? "*") TO \(upper ?? "*")\(rBracket)"
        }
    }
}

extension MinimumShouldMatch: CustomStringConvertible {
    public var description: String {
        switch self {
        case .fixed(let n):
            return "\(n)"
        case .percentage(let pct):
            return "\(Int(pct * 100))%"
        case .combined(let fixed, let percentage):
            return "\(fixed)+\(Int(percentage * 100))%"
        }
    }
}
