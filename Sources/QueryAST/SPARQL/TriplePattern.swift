/// TriplePattern.swift
/// SPARQL triple pattern types
///
/// Reference:
/// - W3C SPARQL 1.1/1.2 Query Language
/// - W3C RDF 1.1 Concepts

import Foundation

// Note: Core TriplePattern struct is defined in DataSource.swift
// This file provides additional utilities and extensions.

// MARK: - TriplePattern Builders

extension TriplePattern {
    /// Create a triple pattern with variables
    public static func pattern(
        _ subject: String,
        _ predicate: String,
        _ object: String
    ) -> TriplePattern {
        TriplePattern(
            subject: .variable(subject),
            predicate: .variable(predicate),
            object: .variable(object)
        )
    }

    /// Create a triple pattern: ?subject predicate ?object
    public static func withPredicate(
        _ subject: String,
        _ predicateIRI: String,
        _ object: String
    ) -> TriplePattern {
        TriplePattern(
            subject: .variable(subject),
            predicate: .iri(predicateIRI),
            object: .variable(object)
        )
    }

    /// Create an rdf:type triple pattern
    public static func rdfType(_ subject: String, _ typeIRI: String) -> TriplePattern {
        TriplePattern(
            subject: .variable(subject),
            predicate: .rdfType,
            object: .iri(typeIRI)
        )
    }

    /// Create a triple pattern with a literal object
    public static func withLiteral(
        _ subject: String,
        _ predicateIRI: String,
        _ literal: Literal
    ) -> TriplePattern {
        TriplePattern(
            subject: .variable(subject),
            predicate: .iri(predicateIRI),
            object: .literal(literal)
        )
    }
}

// MARK: - TriplePattern Analysis

extension TriplePattern {
    /// Returns all variables in this triple pattern
    public var variables: Set<String> {
        var vars = Set<String>()
        if case .variable(let v) = subject { vars.insert(v) }
        if case .variable(let v) = predicate { vars.insert(v) }
        if case .variable(let v) = object { vars.insert(v) }
        return vars
    }

    /// Returns true if this pattern has any variables
    public var hasVariables: Bool {
        subject.isVariable || predicate.isVariable || object.isVariable
    }

    /// Returns true if all terms are variables
    public var isFullyVariable: Bool {
        subject.isVariable && predicate.isVariable && object.isVariable
    }

    /// Returns true if all terms are concrete (no variables)
    public var isConcrete: Bool {
        !subject.isVariable && !predicate.isVariable && !object.isVariable
    }

    /// Returns the selectivity estimate for this pattern
    /// Lower values indicate more selective patterns
    public var selectivityEstimate: Double {
        var selectivity = 1.0

        // Concrete terms are more selective
        if !subject.isVariable { selectivity *= 0.01 }
        if !predicate.isVariable { selectivity *= 0.1 }
        if !object.isVariable { selectivity *= 0.01 }

        return selectivity
    }

    /// Returns the pattern type based on which positions are variables
    public var patternType: TriplePatternType {
        let s = subject.isVariable
        let p = predicate.isVariable
        let o = object.isVariable

        switch (s, p, o) {
        case (false, false, false): return .spo  // Concrete triple
        case (false, false, true):  return .spX  // Known subject and predicate
        case (false, true, false):  return .sXo  // Known subject and object
        case (false, true, true):   return .sXX  // Known subject only
        case (true, false, false):  return .Xpo  // Known predicate and object
        case (true, false, true):   return .XpX  // Known predicate only
        case (true, true, false):   return .XXo  // Known object only
        case (true, true, true):    return .XXX  // All variables
        }
    }
}

/// Triple pattern type classification
public enum TriplePatternType: String, Sendable {
    case spo = "SPO"  // Concrete
    case spX = "SP?"  // Subject-Predicate bound
    case sXo = "S?O"  // Subject-Object bound
    case sXX = "S??"  // Subject bound
    case Xpo = "?PO"  // Predicate-Object bound
    case XpX = "?P?"  // Predicate bound
    case XXo = "??O"  // Object bound
    case XXX = "???"  // All variables

    /// Returns the optimal index for this pattern type
    public var preferredIndex: TripleIndex {
        switch self {
        case .spo, .spX, .sXX: return .spo
        case .Xpo: return .pos
        case .XXo, .sXo: return .osp
        case .XpX: return .pso
        case .XXX: return .spo  // Default
        }
    }
}

/// Triple index types (Hexastore)
public enum TripleIndex: String, Sendable {
    case spo  // Subject-Predicate-Object
    case sop  // Subject-Object-Predicate
    case pso  // Predicate-Subject-Object
    case pos  // Predicate-Object-Subject
    case osp  // Object-Subject-Predicate
    case ops  // Object-Predicate-Subject
}

// MARK: - SPARQL Serialization

extension TriplePattern {
    /// Generate SPARQL syntax
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        "\(subject.toSPARQL(prefixes: prefixes)) \(predicate.toSPARQL(prefixes: prefixes)) \(object.toSPARQL(prefixes: prefixes)) ."
    }
}

// MARK: - Triple Pattern Matching

extension TriplePattern {
    /// Check if this pattern can match the given concrete triple
    public func matches(
        subject s: SPARQLTerm,
        predicate p: SPARQLTerm,
        object o: SPARQLTerm
    ) -> Bool {
        (self.subject.isVariable || self.subject == s) &&
        (self.predicate.isVariable || self.predicate == p) &&
        (self.object.isVariable || self.object == o)
    }

    /// Try to bind variables from a concrete triple
    public func bind(
        subject s: SPARQLTerm,
        predicate p: SPARQLTerm,
        object o: SPARQLTerm
    ) -> [String: SPARQLTerm]? {
        var bindings: [String: SPARQLTerm] = [:]

        // Bind subject
        if case .variable(let v) = self.subject {
            bindings[v] = s
        } else if self.subject != s {
            return nil
        }

        // Bind predicate
        if case .variable(let v) = self.predicate {
            if let existing = bindings[v] {
                if existing != p { return nil }
            } else {
                bindings[v] = p
            }
        } else if self.predicate != p {
            return nil
        }

        // Bind object
        if case .variable(let v) = self.object {
            if let existing = bindings[v] {
                if existing != o { return nil }
            } else {
                bindings[v] = o
            }
        } else if self.object != o {
            return nil
        }

        return bindings
    }

    /// Apply variable bindings to produce a (potentially) more concrete pattern
    public func applyBindings(_ bindings: [String: SPARQLTerm]) -> TriplePattern {
        TriplePattern(
            subject: applyBinding(subject, bindings),
            predicate: applyBinding(predicate, bindings),
            object: applyBinding(object, bindings)
        )
    }

    private func applyBinding(_ term: SPARQLTerm, _ bindings: [String: SPARQLTerm]) -> SPARQLTerm {
        if case .variable(let v) = term, let bound = bindings[v] {
            return bound
        }
        return term
    }
}

// MARK: - Triple Pattern Collections

extension Array where Element == TriplePattern {
    /// Returns all variables used across all patterns
    public var allVariables: Set<String> {
        var vars = Set<String>()
        for pattern in self {
            vars.formUnion(pattern.variables)
        }
        return vars
    }

    /// Sort patterns by selectivity (most selective first)
    public func sortedBySelectivity() -> [TriplePattern] {
        sorted { $0.selectivityEstimate < $1.selectivityEstimate }
    }

    /// Find patterns that share variables with the given pattern
    public func connected(to pattern: TriplePattern) -> [TriplePattern] {
        let vars = pattern.variables
        return filter { p in
            p != pattern && !p.variables.isDisjoint(with: vars)
        }
    }

    /// Group patterns by connected components
    public func connectedComponents() -> [[TriplePattern]] {
        var remaining = Set(self.indices)
        var components: [[TriplePattern]] = []

        while !remaining.isEmpty {
            var component: [TriplePattern] = []
            var queue = [remaining.removeFirst()]

            while !queue.isEmpty {
                let idx = queue.removeFirst()
                component.append(self[idx])

                let connected = connected(to: self[idx])
                for conn in connected {
                    if let connIdx = self.firstIndex(of: conn), remaining.contains(connIdx) {
                        remaining.remove(connIdx)
                        queue.append(connIdx)
                    }
                }
            }

            components.append(component)
        }

        return components
    }

    /// Generate SPARQL syntax for all patterns
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        map { $0.toSPARQL(prefixes: prefixes) }.joined(separator: "\n")
    }
}

