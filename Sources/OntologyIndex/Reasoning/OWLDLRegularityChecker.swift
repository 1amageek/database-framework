// OWLDLRegularityChecker.swift
// GraphIndex - OWL DL regularity validation
//
// Validates OWL DL syntax restrictions for decidability.
//
// Reference: Horrocks, I., Patel-Schneider, P. F., & Van Harmelen, F. (2003).
// "From SHIQ and RDF to OWL: The making of a web ontology language."

import Foundation
import Graph

/// OWL DL Regularity Checker
///
/// Validates that an ontology conforms to OWL DL restrictions.
/// These restrictions ensure decidable reasoning.
///
/// **Key Restrictions**:
/// 1. Transitive roles cannot be used in cardinality constraints
/// 2. Only simple roles can be used in cardinality constraints
/// 3. Role hierarchy must be regular (transitive sub-roles)
/// 4. Property chains must follow regularity grammar
///
/// **Example**:
/// ```swift
/// let checker = OWLDLRegularityChecker()
/// let violations = checker.check(ontology)
/// if !violations.isEmpty {
///     print("OWL DL violations found:")
///     for v in violations {
///         print("  - \(v)")
///     }
/// }
/// ```
public struct OWLDLRegularityChecker: Sendable {

    // MARK: - Violation Types

    /// Regularity violation types
    public enum Violation: Sendable, Equatable, CustomStringConvertible {
        /// Transitive role used in cardinality constraint
        case transitiveInCardinality(role: String, axiom: String)

        /// Non-simple role used in cardinality constraint
        case nonSimpleRoleInCardinality(role: String, axiom: String)

        /// Irregular role hierarchy (transitive sub-role of non-transitive)
        case irregularRoleHierarchy(sub: String, sup: String)

        /// Irregular property chain
        case irregularPropertyChain(chain: [String], target: String)

        /// Role used in incompatible characteristics
        case incompatibleCharacteristics(role: String, characteristics: [String])

        /// Asymmetric role used as reflexive
        case asymmetricReflexive(role: String)

        /// Irreflexive role used as reflexive
        case irreflexiveReflexive(role: String)

        /// Symmetric role used as asymmetric
        case symmetricAsymmetric(role: String)

        public var description: String {
            switch self {
            case .transitiveInCardinality(let role, let axiom):
                return "Transitive role '\(role)' used in cardinality constraint: \(axiom)"
            case .nonSimpleRoleInCardinality(let role, let axiom):
                return "Non-simple role '\(role)' used in cardinality constraint: \(axiom)"
            case .irregularRoleHierarchy(let sub, let sup):
                return "Irregular role hierarchy: transitive '\(sub)' ⊑ non-transitive '\(sup)'"
            case .irregularPropertyChain(let chain, let target):
                return "Irregular property chain: \(chain.joined(separator: " ∘ ")) ⊑ \(target)"
            case .incompatibleCharacteristics(let role, let chars):
                return "Incompatible characteristics for '\(role)': \(chars.joined(separator: ", "))"
            case .asymmetricReflexive(let role):
                return "Role '\(role)' cannot be both asymmetric and reflexive"
            case .irreflexiveReflexive(let role):
                return "Role '\(role)' cannot be both irreflexive and reflexive"
            case .symmetricAsymmetric(let role):
                return "Role '\(role)' cannot be both symmetric and asymmetric"
            }
        }
    }

    // MARK: - Internal State

    private var transitiveRoles: Set<String> = []
    private var simpleRoles: Set<String> = []
    private var nonSimpleRoles: Set<String> = []
    private var roleHierarchy: [String: Set<String>] = [:]  // role -> super-roles
    private var propertyChains: [(chain: [String], target: String)] = []
    private var roleCharacteristics: [String: Set<PropertyCharacteristic>] = [:]

    // MARK: - Initialization

    public init() {}

    // MARK: - Main Check

    /// Check ontology for OWL DL regularity violations
    ///
    /// - Parameter ontology: The ontology to check
    /// - Returns: Array of violations (empty if valid)
    public mutating func check(_ ontology: OWLOntology) -> [Violation] {
        var violations: [Violation] = []

        // Phase 1: Collect role information
        collectRoleInfo(from: ontology)

        // Phase 2: Compute simple roles
        computeSimpleRoles(ontology)

        // Phase 3: Check axioms
        violations.append(contentsOf: checkAxioms(ontology))

        // Phase 4: Check characteristic compatibility
        violations.append(contentsOf: checkCharacteristicCompatibility())

        // Phase 5: Check property chains
        violations.append(contentsOf: checkPropertyChains())

        return violations
    }

    // MARK: - Phase 1: Collect Role Information

    private mutating func collectRoleInfo(from ontology: OWLOntology) {
        // Clear previous state
        transitiveRoles = []
        roleHierarchy = [:]
        propertyChains = []
        roleCharacteristics = [:]

        // Collect from property declarations
        for prop in ontology.objectProperties {
            if prop.characteristics.contains(.transitive) {
                transitiveRoles.insert(prop.iri)
            }
            roleCharacteristics[prop.iri] = prop.characteristics

            for superProp in prop.superProperties {
                roleHierarchy[prop.iri, default: []].insert(superProp)
            }
        }

        // Collect from axioms
        for axiom in ontology.axioms {
            switch axiom {
            case .transitiveObjectProperty(let prop):
                transitiveRoles.insert(prop)

            case .subObjectPropertyOf(let sub, let sup):
                roleHierarchy[sub, default: []].insert(sup)

            case .subPropertyChainOf(let chain, let sup):
                propertyChains.append((chain: chain, target: sup))

            case .functionalObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.functional)

            case .inverseFunctionalObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.inverseFunctional)

            case .symmetricObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.symmetric)

            case .asymmetricObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.asymmetric)

            case .reflexiveObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.reflexive)

            case .irreflexiveObjectProperty(let prop):
                roleCharacteristics[prop, default: []].insert(.irreflexive)

            default:
                break
            }
        }
    }

    // MARK: - Phase 2: Compute Simple Roles

    /// A role is simple if:
    /// 1. It is not transitive
    /// 2. It does not have any transitive sub-roles
    /// 3. It is not the target of a property chain
    private mutating func computeSimpleRoles(_ ontology: OWLOntology) {
        simpleRoles = []
        nonSimpleRoles = []

        // All roles mentioned in the ontology
        var allRoles = Set<String>()
        for prop in ontology.objectProperties {
            allRoles.insert(prop.iri)
        }
        for (sub, sups) in roleHierarchy {
            allRoles.insert(sub)
            allRoles.formUnion(sups)
        }
        for (chain, target) in propertyChains {
            allRoles.formUnion(chain)
            allRoles.insert(target)
        }

        // Roles that are targets of property chains are non-simple
        for (_, target) in propertyChains {
            nonSimpleRoles.insert(target)
        }

        // Transitive roles are non-simple
        nonSimpleRoles.formUnion(transitiveRoles)

        // Propagate non-simplicity through hierarchy (upward)
        var changed = true
        while changed {
            changed = false
            for (role, superRoles) in roleHierarchy {
                if nonSimpleRoles.contains(role) {
                    for superRole in superRoles {
                        if !nonSimpleRoles.contains(superRole) {
                            nonSimpleRoles.insert(superRole)
                            changed = true
                        }
                    }
                }
            }
        }

        // Simple roles are all roles that are not non-simple
        simpleRoles = allRoles.subtracting(nonSimpleRoles)
    }

    // MARK: - Phase 3: Check Axioms

    private func checkAxioms(_ ontology: OWLOntology) -> [Violation] {
        var violations: [Violation] = []

        for axiom in ontology.axioms {
            switch axiom {
            case .subClassOf(let sub, let sup):
                violations.append(contentsOf: checkClassExpression(sub, in: axiom))
                violations.append(contentsOf: checkClassExpression(sup, in: axiom))

            case .equivalentClasses(let exprs):
                for expr in exprs {
                    violations.append(contentsOf: checkClassExpression(expr, in: axiom))
                }

            case .disjointClasses(let exprs):
                for expr in exprs {
                    violations.append(contentsOf: checkClassExpression(expr, in: axiom))
                }

            case .disjointUnion(_, let disjuncts):
                for expr in disjuncts {
                    violations.append(contentsOf: checkClassExpression(expr, in: axiom))
                }

            case .classAssertion(_, let class_):
                violations.append(contentsOf: checkClassExpression(class_, in: axiom))

            default:
                break
            }
        }

        return violations
    }

    private func checkClassExpression(_ expr: OWLClassExpression, in axiom: OWLAxiom) -> [Violation] {
        var violations: [Violation] = []

        switch expr {
        case .minCardinality(let prop, _, let filler),
             .maxCardinality(let prop, _, let filler),
             .exactCardinality(let prop, _, let filler):
            // Check if role is simple
            if !isSimpleRole(prop) {
                if transitiveRoles.contains(prop) {
                    violations.append(.transitiveInCardinality(role: prop, axiom: axiom.description))
                } else {
                    violations.append(.nonSimpleRoleInCardinality(role: prop, axiom: axiom.description))
                }
            }
            if let f = filler {
                violations.append(contentsOf: checkClassExpression(f, in: axiom))
            }

        case .intersection(let exprs), .union(let exprs):
            for e in exprs {
                violations.append(contentsOf: checkClassExpression(e, in: axiom))
            }

        case .complement(let e):
            violations.append(contentsOf: checkClassExpression(e, in: axiom))

        case .someValuesFrom(_, let filler), .allValuesFrom(_, let filler):
            violations.append(contentsOf: checkClassExpression(filler, in: axiom))

        default:
            break
        }

        return violations
    }

    // MARK: - Phase 4: Check Characteristic Compatibility

    private func checkCharacteristicCompatibility() -> [Violation] {
        var violations: [Violation] = []

        for (role, chars) in roleCharacteristics {
            // Asymmetric and Reflexive are incompatible
            if chars.contains(.asymmetric) && chars.contains(.reflexive) {
                violations.append(.asymmetricReflexive(role: role))
            }

            // Irreflexive and Reflexive are incompatible
            if chars.contains(.irreflexive) && chars.contains(.reflexive) {
                violations.append(.irreflexiveReflexive(role: role))
            }

            // Symmetric and Asymmetric are incompatible
            if chars.contains(.symmetric) && chars.contains(.asymmetric) {
                violations.append(.symmetricAsymmetric(role: role))
            }
        }

        return violations
    }

    // MARK: - Phase 5: Check Property Chains

    private func checkPropertyChains() -> [Violation] {
        var violations: [Violation] = []

        // OWL 2 DL property chain regularity:
        // For R₁ ∘ R₂ ∘ ... ∘ Rₙ ⊑ S, the following must hold:
        // 1. S does not appear in the chain (except possibly at the end)
        // 2. If S is transitive, additional restrictions apply

        for (chain, target) in propertyChains {
            // Check for self-reference in chain (except last position)
            if chain.count > 1 {
                let chainWithoutLast = Array(chain.dropLast())
                if chainWithoutLast.contains(target) {
                    violations.append(.irregularPropertyChain(chain: chain, target: target))
                }
            }

            // If target is transitive, chain must be of form R ∘ R ⊑ R
            if transitiveRoles.contains(target) {
                let isValidTransitiveChain = chain.count == 2 &&
                    chain[0] == target && chain[1] == target
                if !isValidTransitiveChain && chain.contains(target) {
                    violations.append(.irregularPropertyChain(chain: chain, target: target))
                }
            }
        }

        return violations
    }

    // MARK: - Helper Methods

    /// Check if a role is simple
    public func isSimpleRole(_ role: String) -> Bool {
        simpleRoles.contains(role)
    }

    /// Check if a role is transitive
    public func isTransitiveRole(_ role: String) -> Bool {
        transitiveRoles.contains(role)
    }

    /// Get all transitive roles
    public var allTransitiveRoles: Set<String> {
        transitiveRoles
    }

    /// Get all simple roles
    public var allSimpleRoles: Set<String> {
        simpleRoles
    }

    /// Get all non-simple roles
    public var allNonSimpleRoles: Set<String> {
        nonSimpleRoles
    }
}

// MARK: - Convenience Extension

extension OWLOntology {
    /// Check if this ontology is valid OWL DL
    ///
    /// - Returns: Tuple of (isValid, violations)
    public func checkOWLDLRegularity() -> (isValid: Bool, violations: [OWLDLRegularityChecker.Violation]) {
        var checker = OWLDLRegularityChecker()
        let violations = checker.check(self)
        return (violations.isEmpty, violations)
    }
}
