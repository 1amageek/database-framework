/// PathPattern.swift
/// SQL/PGQ Path Pattern extensions
///
/// Reference:
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)
/// - GQL (Graph Query Language) specification

import Foundation

// Note: Core PathPattern, PathElement, NodePattern, EdgePattern types
// are defined in DataSource.swift. This file provides additional
// path pattern analysis and transformation utilities.

// MARK: - Path Pattern Analysis

extension PathPattern {
    /// Returns the minimum length of this path pattern
    public var minLength: Int {
        var count = 0
        for element in elements {
            switch element {
            case .node:
                count += 1
            case .edge:
                count += 1
            case .quantified(let inner, let quantifier):
                let innerMin = inner.minLength
                switch quantifier {
                case .exactly(let n):
                    count += innerMin * n
                case .range(let min, _):
                    count += innerMin * (min ?? 0)
                case .zeroOrMore, .zeroOrOne:
                    // Zero minimum
                    break
                case .oneOrMore:
                    count += innerMin
                }
            case .alternation(let alts):
                // Minimum of all alternatives
                count += alts.map(\.minLength).min() ?? 0
            }
        }
        return count
    }

    /// Returns the maximum length of this path pattern (nil if unbounded)
    public var maxLength: Int? {
        var count = 0
        for element in elements {
            switch element {
            case .node:
                count += 1
            case .edge:
                count += 1
            case .quantified(let inner, let quantifier):
                guard let innerMax = inner.maxLength else { return nil }
                switch quantifier {
                case .exactly(let n):
                    count += innerMax * n
                case .range(_, let max):
                    guard let m = max else { return nil }
                    count += innerMax * m
                case .zeroOrMore, .oneOrMore:
                    return nil  // Unbounded
                case .zeroOrOne:
                    count += innerMax
                }
            case .alternation(let alts):
                // Maximum of all alternatives
                var altMax = 0
                for alt in alts {
                    guard let m = alt.maxLength else { return nil }
                    altMax = max(altMax, m)
                }
                count += altMax
            }
        }
        return count
    }

    /// Returns true if this pattern can match zero-length paths
    public var canMatchEmpty: Bool {
        minLength == 0
    }

    /// Returns true if this pattern is unbounded (can match infinite length)
    public var isUnbounded: Bool {
        maxLength == nil
    }

    /// Returns the number of node patterns in this path
    public var nodeCount: Int {
        var count = 0
        for element in elements {
            switch element {
            case .node:
                count += 1
            case .edge:
                break
            case .quantified(let inner, _):
                count += inner.nodeCount
            case .alternation(let alts):
                count += alts.map(\.nodeCount).max() ?? 0
            }
        }
        return count
    }

    /// Returns the number of edge patterns in this path
    public var edgeCount: Int {
        var count = 0
        for element in elements {
            switch element {
            case .node:
                break
            case .edge:
                count += 1
            case .quantified(let inner, _):
                count += inner.edgeCount
            case .alternation(let alts):
                count += alts.map(\.edgeCount).max() ?? 0
            }
        }
        return count
    }

    /// Returns all node variables defined in this path
    public var nodeVariables: Set<String> {
        var vars = Set<String>()
        for element in elements {
            switch element {
            case .node(let node):
                if let v = node.variable { vars.insert(v) }
            case .edge:
                break
            case .quantified(let inner, _):
                vars.formUnion(inner.nodeVariables)
            case .alternation(let alts):
                for alt in alts {
                    vars.formUnion(alt.nodeVariables)
                }
            }
        }
        return vars
    }

    /// Returns all edge variables defined in this path
    public var edgeVariables: Set<String> {
        var vars = Set<String>()
        for element in elements {
            switch element {
            case .node:
                break
            case .edge(let edge):
                if let v = edge.variable { vars.insert(v) }
            case .quantified(let inner, _):
                vars.formUnion(inner.edgeVariables)
            case .alternation(let alts):
                for alt in alts {
                    vars.formUnion(alt.edgeVariables)
                }
            }
        }
        return vars
    }

    /// Returns all labels used in node patterns
    public var nodeLabels: Set<String> {
        var labels = Set<String>()
        for element in elements {
            switch element {
            case .node(let node):
                if let nodeLabels = node.labels {
                    labels.formUnion(nodeLabels)
                }
            case .edge:
                break
            case .quantified(let inner, _):
                labels.formUnion(inner.nodeLabels)
            case .alternation(let alts):
                for alt in alts {
                    labels.formUnion(alt.nodeLabels)
                }
            }
        }
        return labels
    }

    /// Returns all labels used in edge patterns
    public var edgeLabels: Set<String> {
        var labels = Set<String>()
        for element in elements {
            switch element {
            case .node:
                break
            case .edge(let edge):
                if let edgeLabels = edge.labels {
                    labels.formUnion(edgeLabels)
                }
            case .quantified(let inner, _):
                labels.formUnion(inner.edgeLabels)
            case .alternation(let alts):
                for alt in alts {
                    labels.formUnion(alt.edgeLabels)
                }
            }
        }
        return labels
    }
}

// MARK: - Path Pattern Transformations

extension PathPattern {
    /// Reverse the path pattern (swap direction of all edges)
    public func reversed() -> PathPattern {
        PathPattern(
            pathVariable: pathVariable,
            elements: elements.reversed().map { $0.reversed() },
            mode: mode
        )
    }

    /// Remove all variable bindings (anonymous pattern)
    public func anonymous() -> PathPattern {
        PathPattern(
            pathVariable: nil,
            elements: elements.map { $0.anonymous() },
            mode: mode
        )
    }

    /// Set the path mode
    public func mode(_ newMode: PathMode) -> PathPattern {
        PathPattern(
            pathVariable: pathVariable,
            elements: elements,
            mode: newMode
        )
    }

    /// Set the path variable
    public func `as`(_ variable: String) -> PathPattern {
        PathPattern(
            pathVariable: variable,
            elements: elements,
            mode: mode
        )
    }
}

extension PathElement {
    /// Reverse the element (swap edge direction)
    public func reversed() -> PathElement {
        switch self {
        case .node:
            return self
        case .edge(let edge):
            return .edge(edge.reversed())
        case .quantified(let inner, let quant):
            return .quantified(inner.reversed(), quantifier: quant)
        case .alternation(let alts):
            return .alternation(alts.map { $0.reversed() })
        }
    }

    /// Remove variable binding
    public func anonymous() -> PathElement {
        switch self {
        case .node(let node):
            return .node(NodePattern(variable: nil, labels: node.labels, properties: node.properties))
        case .edge(let edge):
            return .edge(EdgePattern(variable: nil, labels: edge.labels, properties: edge.properties, direction: edge.direction))
        case .quantified(let inner, let quant):
            return .quantified(inner.anonymous(), quantifier: quant)
        case .alternation(let alts):
            return .alternation(alts.map { $0.anonymous() })
        }
    }
}

extension EdgePattern {
    /// Reverse the edge direction
    public func reversed() -> EdgePattern {
        let newDirection: EdgeDirection
        switch direction {
        case .outgoing:
            newDirection = .incoming
        case .incoming:
            newDirection = .outgoing
        case .undirected, .any:
            newDirection = direction
        }
        return EdgePattern(
            variable: variable,
            labels: labels,
            properties: properties,
            direction: newDirection
        )
    }
}

// MARK: - Pattern Normalization

extension PathPattern {
    /// Normalize the pattern by expanding nested quantifications
    /// and simplifying alternations where possible
    public func normalized() -> PathPattern {
        var normalizedElements: [PathElement] = []

        for element in elements {
            switch element {
            case .node, .edge:
                normalizedElements.append(element)

            case .quantified(let inner, let quant):
                // Flatten nested quantified patterns if possible
                if inner.elements.count == 1, case .quantified = inner.elements[0] {
                    // Nested quantification - can sometimes be combined
                    normalizedElements.append(element)
                } else {
                    normalizedElements.append(.quantified(inner.normalized(), quantifier: quant))
                }

            case .alternation(let alts):
                // Remove duplicate alternatives
                let normalizedAlts = alts.map { $0.normalized() }
                let uniqueAlts = removeDuplicates(normalizedAlts)
                if uniqueAlts.count == 1 {
                    normalizedElements.append(contentsOf: uniqueAlts[0].elements)
                } else {
                    normalizedElements.append(.alternation(uniqueAlts))
                }
            }
        }

        return PathPattern(
            pathVariable: pathVariable,
            elements: normalizedElements,
            mode: mode
        )
    }

    private func removeDuplicates(_ patterns: [PathPattern]) -> [PathPattern] {
        var seen = Set<Int>()
        var unique: [PathPattern] = []

        for pattern in patterns {
            let hash = pattern.hashValue
            if !seen.contains(hash) {
                seen.insert(hash)
                unique.append(pattern)
            }
        }

        return unique
    }
}

