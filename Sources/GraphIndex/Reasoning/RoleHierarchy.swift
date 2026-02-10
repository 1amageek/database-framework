// RoleHierarchy.swift
// GraphIndex - Role hierarchy management for OWL reasoning
//
// Manages role (object property) hierarchy, inverses, and characteristics.
// Supports SHOIN(D) features: S (transitive), H (hierarchy), I (inverse).
//
// Reference: Horrocks, I., & Sattler, U. (2004).
// "Decidability of SHIQ with complex role inclusion axioms."

import Foundation
import Graph

/// Role Hierarchy Manager
///
/// Manages the hierarchy of object properties (roles) including:
/// - Sub-property relationships (H)
/// - Inverse properties (I)
/// - Transitive properties (S)
/// - Property chains
///
/// **Example**:
/// ```swift
/// var hierarchy = RoleHierarchy()
/// hierarchy.load(from: ontology)
///
/// // Check relationships
/// let superRoles = hierarchy.superRoles(of: "ex:hasChild")
/// let inverse = hierarchy.inverse(of: "ex:hasChild")  // ex:hasParent
/// let isTransitive = hierarchy.isTransitive("ex:ancestorOf")
///
/// // Check subsumption
/// let subsumes = hierarchy.subsumes("ex:hasRelative", "ex:hasChild")
/// ```
public struct RoleHierarchy: Sendable {

    // MARK: - Types

    /// Role information
    public struct RoleInfo: Sendable {
        public let iri: String
        public var superRoles: Set<String> = []
        public var subRoles: Set<String> = []
        public var inverseRole: String?
        public var characteristics: Set<PropertyCharacteristic> = []
        public var propertyChains: [[String]] = []
        public var domains: [OWLClassExpression] = []
        public var ranges: [OWLClassExpression] = []

        public init(iri: String) {
            self.iri = iri
        }
    }

    // MARK: - Properties

    /// Role information indexed by IRI
    private var roles: [String: RoleInfo] = [:]

    /// Cached transitive closure of super-roles
    private var superRoleClosure: [String: Set<String>] = [:]

    /// Cached transitive closure of sub-roles
    private var subRoleClosure: [String: Set<String>] = [:]

    /// Whether closures are computed
    private var closuresComputed: Bool = false

    // MARK: - Initialization

    public init() {}

    /// Initialize from ontology
    public init(ontology: OWLOntology) {
        var hierarchy = RoleHierarchy()
        hierarchy.load(from: ontology)
        self = hierarchy
    }

    /// Initialize from ontology with pre-built index (avoids redundant O(n) scans)
    public init(ontology: OWLOntology, index: OntologyIndex) {
        var hierarchy = RoleHierarchy()
        hierarchy.loadWithIndex(from: ontology, index: index)
        self = hierarchy
    }

    // MARK: - Loading

    /// Load role hierarchy from ontology
    public mutating func load(from ontology: OWLOntology) {
        // Clear existing data
        roles = [:]
        superRoleClosure = [:]
        subRoleClosure = [:]
        closuresComputed = false

        // Load from property declarations
        for prop in ontology.objectProperties {
            var info = getOrCreateRole(prop.iri)
            info.characteristics = prop.characteristics
            info.inverseRole = prop.inverseOf
            info.domains = prop.domains
            info.ranges = prop.ranges

            for superProp in prop.superProperties {
                info.superRoles.insert(superProp)
                var superInfo = getOrCreateRole(superProp)
                superInfo.subRoles.insert(prop.iri)
                roles[superProp] = superInfo
            }

            for chain in prop.propertyChains {
                info.propertyChains.append(chain)
            }

            roles[prop.iri] = info
        }

        // Load from axioms
        for axiom in ontology.axioms {
            switch axiom {
            case .subObjectPropertyOf(let sub, let sup):
                var subInfo = getOrCreateRole(sub)
                subInfo.superRoles.insert(sup)
                roles[sub] = subInfo

                var supInfo = getOrCreateRole(sup)
                supInfo.subRoles.insert(sub)
                roles[sup] = supInfo

            case .subPropertyChainOf(let chain, let sup):
                var info = getOrCreateRole(sup)
                info.propertyChains.append(chain)
                roles[sup] = info

            case .equivalentObjectProperties(let props):
                // Equivalent properties are mutual sub-properties
                for i in 0..<props.count {
                    for j in 0..<props.count where i != j {
                        var info = getOrCreateRole(props[i])
                        info.superRoles.insert(props[j])
                        info.subRoles.insert(props[j])
                        roles[props[i]] = info
                    }
                }

            case .inverseObjectProperties(let first, let second):
                var info1 = getOrCreateRole(first)
                info1.inverseRole = second
                roles[first] = info1

                var info2 = getOrCreateRole(second)
                info2.inverseRole = first
                roles[second] = info2

            case .transitiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.transitive)
                roles[prop] = info

            case .symmetricObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.symmetric)
                roles[prop] = info

            case .asymmetricObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.asymmetric)
                roles[prop] = info

            case .reflexiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.reflexive)
                roles[prop] = info

            case .irreflexiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.irreflexive)
                roles[prop] = info

            case .functionalObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.functional)
                roles[prop] = info

            case .inverseFunctionalObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.inverseFunctional)
                roles[prop] = info

            case .objectPropertyDomain(let prop, let domain):
                var info = getOrCreateRole(prop)
                info.domains.append(domain)
                roles[prop] = info

            case .objectPropertyRange(let prop, let range):
                var info = getOrCreateRole(prop)
                info.ranges.append(range)
                roles[prop] = info

            default:
                break
            }
        }
    }

    /// Load role hierarchy using pre-built OntologyIndex
    public mutating func loadWithIndex(from ontology: OWLOntology, index: OntologyIndex) {
        // Clear existing data
        roles = [:]
        superRoleClosure = [:]
        subRoleClosure = [:]
        closuresComputed = false

        // Load from property declarations
        for prop in ontology.objectProperties {
            var info = getOrCreateRole(prop.iri)
            info.characteristics = prop.characteristics
            info.inverseRole = prop.inverseOf
            info.domains = prop.domains
            info.ranges = prop.ranges

            for superProp in prop.superProperties {
                info.superRoles.insert(superProp)
                var superInfo = getOrCreateRole(superProp)
                superInfo.subRoles.insert(prop.iri)
                roles[superProp] = superInfo
            }

            for chain in prop.propertyChains {
                info.propertyChains.append(chain)
            }

            roles[prop.iri] = info
        }

        // Load from RBox axioms only (pre-filtered by index)
        for axiom in index.rboxAxioms {
            switch axiom {
            case .subObjectPropertyOf(let sub, let sup):
                var subInfo = getOrCreateRole(sub)
                subInfo.superRoles.insert(sup)
                roles[sub] = subInfo

                var supInfo = getOrCreateRole(sup)
                supInfo.subRoles.insert(sub)
                roles[sup] = supInfo

            case .subPropertyChainOf(let chain, let sup):
                var info = getOrCreateRole(sup)
                info.propertyChains.append(chain)
                roles[sup] = info

            case .equivalentObjectProperties(let props):
                for i in 0..<props.count {
                    for j in 0..<props.count where i != j {
                        var info = getOrCreateRole(props[i])
                        info.superRoles.insert(props[j])
                        info.subRoles.insert(props[j])
                        roles[props[i]] = info
                    }
                }

            case .inverseObjectProperties(let first, let second):
                var info1 = getOrCreateRole(first)
                info1.inverseRole = second
                roles[first] = info1

                var info2 = getOrCreateRole(second)
                info2.inverseRole = first
                roles[second] = info2

            case .transitiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.transitive)
                roles[prop] = info

            case .symmetricObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.symmetric)
                roles[prop] = info

            case .asymmetricObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.asymmetric)
                roles[prop] = info

            case .reflexiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.reflexive)
                roles[prop] = info

            case .irreflexiveObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.irreflexive)
                roles[prop] = info

            case .functionalObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.functional)
                roles[prop] = info

            case .inverseFunctionalObjectProperty(let prop):
                var info = getOrCreateRole(prop)
                info.characteristics.insert(.inverseFunctional)
                roles[prop] = info

            case .objectPropertyDomain(let prop, let domain):
                var info = getOrCreateRole(prop)
                info.domains.append(domain)
                roles[prop] = info

            case .objectPropertyRange(let prop, let range):
                var info = getOrCreateRole(prop)
                info.ranges.append(range)
                roles[prop] = info

            default:
                break
            }
        }
    }

    private mutating func getOrCreateRole(_ iri: String) -> RoleInfo {
        if let existing = roles[iri] {
            return existing
        }
        return RoleInfo(iri: iri)
    }

    // MARK: - Query Methods

    /// Get all known roles
    public var allRoles: Set<String> {
        Set(roles.keys)
    }

    /// Get role information
    public func info(for role: String) -> RoleInfo? {
        roles[role]
    }

    /// Get direct super-roles
    public func directSuperRoles(of role: String) -> Set<String> {
        roles[role]?.superRoles ?? []
    }

    /// Get direct sub-roles
    public func directSubRoles(of role: String) -> Set<String> {
        roles[role]?.subRoles ?? []
    }

    /// Get all super-roles (transitive closure)
    public mutating func superRoles(of role: String) -> Set<String> {
        computeClosuresIfNeeded()
        return superRoleClosure[role] ?? []
    }

    /// Get all sub-roles (transitive closure)
    public mutating func subRoles(of role: String) -> Set<String> {
        computeClosuresIfNeeded()
        return subRoleClosure[role] ?? []
    }

    /// Get inverse role (if exists)
    public func inverse(of role: String) -> String? {
        roles[role]?.inverseRole
    }

    /// Check if role is transitive
    public func isTransitive(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.transitive) ?? false
    }

    /// Check if role is symmetric
    public func isSymmetric(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.symmetric) ?? false
    }

    /// Check if role is functional
    public func isFunctional(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.functional) ?? false
    }

    /// Check if role is inverse functional
    public func isInverseFunctional(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.inverseFunctional) ?? false
    }

    /// Check if role is reflexive
    public func isReflexive(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.reflexive) ?? false
    }

    /// Check if role is irreflexive
    public func isIrreflexive(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.irreflexive) ?? false
    }

    /// Check if role is asymmetric
    public func isAsymmetric(_ role: String) -> Bool {
        roles[role]?.characteristics.contains(.asymmetric) ?? false
    }

    /// Get property chains that imply this role
    public func propertyChains(for role: String) -> [[String]] {
        roles[role]?.propertyChains ?? []
    }

    /// Get domain restrictions for role
    public func domains(of role: String) -> [OWLClassExpression] {
        roles[role]?.domains ?? []
    }

    /// Get range restrictions for role
    public func ranges(of role: String) -> [OWLClassExpression] {
        roles[role]?.ranges ?? []
    }

    // MARK: - Subsumption

    /// Check if sup subsumes sub (sub ⊑ sup)
    public mutating func subsumes(_ sup: String, _ sub: String) -> Bool {
        if sup == sub { return true }
        computeClosuresIfNeeded()
        return superRoleClosure[sub]?.contains(sup) ?? false
    }

    /// Check if two roles are equivalent
    public mutating func areEquivalent(_ role1: String, _ role2: String) -> Bool {
        subsumes(role1, role2) && subsumes(role2, role1)
    }

    // MARK: - Simple Role Check

    /// Check if a role is "simple" (OWL DL requirement)
    ///
    /// A role is simple if:
    /// 1. It is not transitive
    /// 2. It does not have any transitive sub-roles
    /// 3. It is not the target of a property chain
    public mutating func isSimple(_ role: String) -> Bool {
        // Check if transitive
        if isTransitive(role) {
            return false
        }

        // Check if it's the target of a property chain
        for (_, info) in roles {
            for chain in info.propertyChains {
                if chain.count > 1 {
                    // Property chains make the target non-simple
                    // (The role itself being the target is handled by having propertyChains)
                }
            }
        }
        if !(roles[role]?.propertyChains.isEmpty ?? true) {
            return false
        }

        // Check sub-roles for transitivity
        computeClosuresIfNeeded()
        if let subs = subRoleClosure[role] {
            for sub in subs {
                if isTransitive(sub) {
                    return false
                }
            }
        }

        return true
    }

    // MARK: - Transitive Closure Computation (Topological Sort)
    //
    // Uses topological ordering for O(V + E) closure computation
    // instead of per-role DFS.
    //
    // Reference: Kahn, A.B. (1962). "Topological sorting of large networks"

    private mutating func computeClosuresIfNeeded() {
        guard !closuresComputed else { return }

        let allRoles = Array(roles.keys)

        // Initialize closures
        for role in allRoles {
            superRoleClosure[role] = []
            subRoleClosure[role] = []
        }

        // --- Super-role closure (process roots first) ---
        var processedSuper = Set<String>()
        var queue: [String] = allRoles.filter { (roles[$0]?.superRoles ?? []).isEmpty }

        while !queue.isEmpty {
            let role = queue.removeFirst()
            if processedSuper.contains(role) { continue }
            processedSuper.insert(role)

            var closure = Set<String>()
            for superRole in roles[role]?.superRoles ?? [] {
                closure.insert(superRole)
                closure.formUnion(superRoleClosure[superRole] ?? [])
            }
            superRoleClosure[role] = closure

            // Enqueue roles whose superRoles include this role
            for (otherRole, otherInfo) in roles {
                if processedSuper.contains(otherRole) { continue }
                if otherInfo.superRoles.contains(role) {
                    let allDeps = otherInfo.superRoles.allSatisfy { processedSuper.contains($0) }
                    if allDeps {
                        queue.append(otherRole)
                    }
                }
            }
        }

        // Fallback for cycles
        for role in allRoles where !processedSuper.contains(role) {
            superRoleClosure[role] = computeSuperClosureFallback(for: role, visited: [])
        }

        // --- Sub-role closure (process leaves first) ---
        var processedSub = Set<String>()
        queue = allRoles.filter { (roles[$0]?.subRoles ?? []).isEmpty }

        while !queue.isEmpty {
            let role = queue.removeFirst()
            if processedSub.contains(role) { continue }
            processedSub.insert(role)

            var closure = Set<String>()
            for subRole in roles[role]?.subRoles ?? [] {
                closure.insert(subRole)
                closure.formUnion(subRoleClosure[subRole] ?? [])
            }
            subRoleClosure[role] = closure

            for (otherRole, otherInfo) in roles {
                if processedSub.contains(otherRole) { continue }
                if otherInfo.subRoles.contains(role) {
                    let allDeps = otherInfo.subRoles.allSatisfy { processedSub.contains($0) }
                    if allDeps {
                        queue.append(otherRole)
                    }
                }
            }
        }

        // Fallback for cycles
        for role in allRoles where !processedSub.contains(role) {
            subRoleClosure[role] = computeSubClosureFallback(for: role, visited: [])
        }

        closuresComputed = true
    }

    private func computeSuperClosureFallback(for role: String, visited: Set<String>) -> Set<String> {
        var result = Set<String>()
        guard let info = roles[role] else { return result }
        guard !visited.contains(role) else { return result }

        var newVisited = visited
        newVisited.insert(role)

        for superRole in info.superRoles {
            result.insert(superRole)
            result.formUnion(computeSuperClosureFallback(for: superRole, visited: newVisited))
        }

        return result
    }

    private func computeSubClosureFallback(for role: String, visited: Set<String>) -> Set<String> {
        var result = Set<String>()
        guard let info = roles[role] else { return result }
        guard !visited.contains(role) else { return result }

        var newVisited = visited
        newVisited.insert(role)

        for subRole in info.subRoles {
            result.insert(subRole)
            result.formUnion(computeSubClosureFallback(for: subRole, visited: newVisited))
        }

        return result
    }

    // MARK: - Modification Methods

    /// Add a sub-role relationship (subRole ⊑ superRole)
    ///
    /// - Parameters:
    ///   - sub: The sub-role IRI
    ///   - super: The super-role IRI
    public mutating func addSubRole(sub: String, super superRole: String) {
        guard sub != superRole else { return }

        // Ensure both roles exist
        if roles[sub] == nil {
            roles[sub] = RoleInfo(iri: sub)
        }
        if roles[superRole] == nil {
            roles[superRole] = RoleInfo(iri: superRole)
        }

        // Add relationship
        roles[sub]?.superRoles.insert(superRole)
        roles[superRole]?.subRoles.insert(sub)

        // Invalidate cached closures
        closuresComputed = false
        superRoleClosure = [:]
        subRoleClosure = [:]
    }

    /// Set a characteristic for a role
    ///
    /// - Parameters:
    ///   - characteristic: The property characteristic
    ///   - role: The role IRI
    ///   - value: Whether to set or unset the characteristic
    public mutating func setCharacteristic(_ characteristic: PropertyCharacteristic, for role: String, value: Bool) {
        if roles[role] == nil {
            roles[role] = RoleInfo(iri: role)
        }

        if value {
            roles[role]?.characteristics.insert(characteristic)
        } else {
            roles[role]?.characteristics.remove(characteristic)
        }
    }

    /// Set inverse relationship between two roles
    ///
    /// - Parameters:
    ///   - role1: First role IRI
    ///   - role2: Second role IRI
    public mutating func setInverse(_ role1: String, _ role2: String) {
        if roles[role1] == nil {
            roles[role1] = RoleInfo(iri: role1)
        }
        if roles[role2] == nil {
            roles[role2] = RoleInfo(iri: role2)
        }

        roles[role1]?.inverseRole = role2
        roles[role2]?.inverseRole = role1
    }

    /// Set domain for a role
    ///
    /// - Parameters:
    ///   - role: The role IRI
    ///   - domain: The domain class IRI
    public mutating func setDomain(for role: String, domain: String) {
        if roles[role] == nil {
            roles[role] = RoleInfo(iri: role)
        }
        roles[role]?.domains.append(.named(domain))
    }

    /// Set range for a role
    ///
    /// - Parameters:
    ///   - role: The role IRI
    ///   - range: The range class IRI
    public mutating func setRange(for role: String, range: String) {
        if roles[role] == nil {
            roles[role] = RoleInfo(iri: role)
        }
        roles[role]?.ranges.append(.named(range))
    }

    /// Add a property chain axiom
    ///
    /// - Parameters:
    ///   - chain: The property chain (list of role IRIs)
    ///   - implies: The implied role IRI
    public mutating func addPropertyChain(_ chain: [String], implies role: String) {
        if roles[role] == nil {
            roles[role] = RoleInfo(iri: role)
        }
        roles[role]?.propertyChains.append(chain)
    }

    /// Get all property chains that imply a given role
    public func propertyChains(implying role: String) -> [[String]] {
        roles[role]?.propertyChains ?? []
    }

    /// Get all property chains in the hierarchy
    public func allPropertyChains() -> [(chain: [String], implies: String)] {
        var result: [(chain: [String], implies: String)] = []
        for (role, info) in roles {
            for chain in info.propertyChains {
                result.append((chain: chain, implies: role))
            }
        }
        return result
    }

    /// Check if sub is a sub-role of super
    public mutating func isSubRoleOf(sub: String, super superRole: String) -> Bool {
        if sub == superRole { return true }
        return subsumes(superRole, sub)
    }

    // MARK: - Role Inference

    /// Get all roles that hold between two individuals given base assertions
    ///
    /// This includes:
    /// - Direct assertions
    /// - Inverse inference
    /// - Transitive closure
    /// - Role hierarchy inference
    public mutating func inferRoles(
        from subject: String,
        to object: String,
        baseAssertions: [(subject: String, role: String, object: String)]
    ) -> Set<String> {
        var result = Set<String>()

        for (s, r, o) in baseAssertions {
            // Direct assertion
            if s == subject && o == object {
                result.insert(r)
                // Add super-roles
                result.formUnion(superRoles(of: r))
            }

            // Inverse assertion
            if s == object && o == subject {
                if let inv = inverse(of: r) {
                    result.insert(inv)
                    result.formUnion(superRoles(of: inv))
                }
                // Symmetric role
                if isSymmetric(r) {
                    result.insert(r)
                    result.formUnion(superRoles(of: r))
                }
            }
        }

        return result
    }
}

// MARK: - CustomStringConvertible

extension RoleHierarchy: CustomStringConvertible {
    public var description: String {
        var lines: [String] = ["RoleHierarchy:"]

        for (iri, info) in roles.sorted(by: { $0.key < $1.key }) {
            var line = "  \(iri)"
            if !info.superRoles.isEmpty {
                line += " ⊑ \(info.superRoles.sorted().joined(separator: ", "))"
            }
            if let inv = info.inverseRole {
                line += " [inverse: \(inv)]"
            }
            if !info.characteristics.isEmpty {
                let chars = info.characteristics.map { $0.rawValue }.sorted()
                line += " [\(chars.joined(separator: ", "))]"
            }
            lines.append(line)
        }

        return lines.joined(separator: "\n")
    }
}

// MARK: - Convenience Extension

extension OWLOntology {
    /// Build role hierarchy from this ontology
    public func buildRoleHierarchy() -> RoleHierarchy {
        RoleHierarchy(ontology: self)
    }
}
