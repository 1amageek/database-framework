// OntologyContext.swift
// GraphIndex - Ontology context for SPARQL-ontology integration
//
// Provides property hierarchy information to SPARQLQueryExecutor
// for ontology-aware query evaluation.
//
// Design: Eagerly pre-computes role hierarchy closures at init time,
// enabling all methods to be non-mutating. This allows the context
// to be stored as `let` in SPARQLQueryExecutor.
//
// Capabilities:
//   - Expand predicates to include sub-properties (F-1)
//   - Resolve owl:inverseOf declarations (F-2)
//   - Detect transitive properties for BFS optimization (F-3)
//   - Check functional property hints for cardinality estimation (F-4)

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Graph

import OntologyIndex
/// Ontology context for SPARQL property path evaluation
///
/// When provided to SPARQLQueryExecutor, enables ontology-aware evaluation:
/// - `.iri(predicate)` expands to include sub-properties
/// - `.inverse()` consults owl:inverseOf declarations
/// - Transitive properties enable optimized BFS expansion
///
/// All methods are non-mutating — closures are eagerly computed at init.
///
/// **Example**:
/// ```swift
/// let ontology = try await context.ontology.get(iri: "http://example.org/family")!
/// let ontoCtx = OntologyContext(ontology: ontology)
/// let executor = SPARQLQueryExecutor(
///     database: db,
///     indexSubspace: subspace,
///     strategy: .hexastore,
///     fromFieldName: "subject",
///     edgeFieldName: "predicate",
///     toFieldName: "object",
///     ontologyContext: ontoCtx
/// )
/// ```
public struct OntologyContext: Sendable {

    /// Pre-computed role hierarchy for property expansion
    private let roleHierarchy: RoleHierarchy

    /// Immutable direction-aware physical scan closure for every known role.
    /// It is built at runtime bootstrap so request execution performs no
    /// ontology Set traversal or closure allocation.
    private let entailedPropertyScans: [
        String: [OntologyEntailedPropertyScan]
    ]

    /// Initialize from an OWL ontology
    ///
    /// Eagerly computes transitive closures for all role hierarchies.
    public init(ontology: OWLOntology) {
        var rh = RoleHierarchy(ontology: ontology)
        rh.ensureClosuresComputed()
        self.roleHierarchy = rh
        self.entailedPropertyScans = Self.makeEntailedPropertyScans(
            roleHierarchy: rh
        )
    }

    /// Initialize from a pre-built role hierarchy
    ///
    /// Eagerly computes transitive closures if not already computed.
    public init(roleHierarchy: RoleHierarchy) {
        var rh = roleHierarchy
        rh.ensureClosuresComputed()
        self.roleHierarchy = rh
        self.entailedPropertyScans = Self.makeEntailedPropertyScans(
            roleHierarchy: rh
        )
    }

    /// Get all sub-properties of a property (transitive closure)
    ///
    /// For `ex:hasFather ⊑ ex:hasParent`, querying `ex:hasParent`
    /// should also match `ex:hasFather` edges.
    public func subProperties(of propertyIRI: String) -> Set<String> {
        roleHierarchy.subRolesPrecomputed(of: propertyIRI)
    }

    /// Get the inverse property (owl:inverseOf or symmetric self-inverse)
    ///
    /// OWL semantics:
    /// - If `owl:inverseOf(P, Q)` is declared, returns Q.
    /// - If P is symmetric, P is its own inverse (P⁻¹ = P).
    ///
    /// Reference: OWL 2 Structural Specification, Section 9.2.1
    public func inverseProperty(of propertyIRI: String) -> String? {
        if let declared = roleHierarchy.inverse(of: propertyIRI) {
            return declared
        }
        // Symmetric properties are their own inverse: R ≡ R⁻¹
        if roleHierarchy.isSymmetric(propertyIRI) {
            return propertyIRI
        }
        return nil
    }

    /// Check if a property is transitive
    ///
    /// Transitive properties enable BFS expansion optimization
    /// in property path evaluation.
    public func isTransitive(_ propertyIRI: String) -> Bool {
        roleHierarchy.isTransitive(propertyIRI)
    }

    /// Check if a property is symmetric
    public func isSymmetric(_ propertyIRI: String) -> Bool {
        roleHierarchy.isSymmetric(propertyIRI)
    }

    /// Check if a property is functional
    ///
    /// Functional properties have at most one value per subject.
    /// Used for cardinality estimation hints.
    public func isFunctional(_ propertyIRI: String) -> Bool {
        roleHierarchy.isFunctional(propertyIRI)
    }

    /// Get all property IRIs expanded with their sub-properties
    ///
    /// Returns the original IRI plus all sub-property IRIs.
    /// Used by `.iri(predicate)` evaluation to expand queries.
    public func expandedProperties(of propertyIRI: String) -> Set<String> {
        var result = Set<String>([propertyIRI])
        result.formUnion(subProperties(of: propertyIRI))
        return result
    }

    /// Returns a deterministic direction-aware closure for a known ontology
    /// property. Unknown properties return `nil` and require one direct scan.
    func knownEntailedPropertyScans(
        of propertyIRI: String
    ) -> [OntologyEntailedPropertyScan]? {
        entailedPropertyScans[propertyIRI]
    }

    private static func makeEntailedPropertyScans(
        roleHierarchy: RoleHierarchy
    ) -> [String: [OntologyEntailedPropertyScan]] {
        var roleUniverse = roleHierarchy.allRoles
        var discoveredRole = true
        while discoveredRole {
            discoveredRole = false
            let currentRoles = roleUniverse
            for role in currentRoles {
                guard let inverse = roleHierarchy.inverse(of: role) else {
                    continue
                }
                if roleUniverse.insert(inverse).inserted {
                    discoveredRole = true
                }
            }
        }

        let orderedRoles = roleUniverse.sorted()
        var result: [String: [OntologyEntailedPropertyScan]] = [:]
        result.reserveCapacity(orderedRoles.count)

        for requestedRole in orderedRoles {
            let initial = OntologyEntailedPropertyScan(
                predicateIRI: requestedRole,
                isInverse: false
            )
            var closure: [OntologyEntailedPropertyScan] = [initial]
            var seen: Set<OntologyEntailedPropertyScan> = [initial]
            var cursor = 0

            while cursor < closure.count {
                let current = closure[cursor]
                cursor += 1
                var candidates: [OntologyEntailedPropertyScan] = []

                for subrole in roleHierarchy
                    .subRolesPrecomputed(of: current.predicateIRI)
                    .sorted() {
                    candidates.append(
                        OntologyEntailedPropertyScan(
                            predicateIRI: subrole,
                            isInverse: current.isInverse
                        )
                    )
                }

                if let inverse = roleHierarchy.inverse(
                    of: current.predicateIRI
                ) {
                    candidates.append(
                        OntologyEntailedPropertyScan(
                            predicateIRI: inverse,
                            isInverse: !current.isInverse
                        )
                    )
                }

                for reverseDeclaredRole in orderedRoles {
                    guard roleHierarchy.inverse(of: reverseDeclaredRole)
                            == current.predicateIRI else {
                        continue
                    }
                    candidates.append(
                        OntologyEntailedPropertyScan(
                            predicateIRI: reverseDeclaredRole,
                            isInverse: !current.isInverse
                        )
                    )
                }

                if roleHierarchy.isSymmetric(current.predicateIRI) {
                    candidates.append(
                        OntologyEntailedPropertyScan(
                            predicateIRI: current.predicateIRI,
                            isInverse: !current.isInverse
                        )
                    )
                }

                candidates.sort { lhs, rhs in
                    if lhs.predicateIRI != rhs.predicateIRI {
                        return lhs.predicateIRI < rhs.predicateIRI
                    }
                    return !lhs.isInverse && rhs.isInverse
                }
                for candidate in candidates {
                    guard seen.insert(candidate).inserted else { continue }
                    closure.append(candidate)
                }
            }
            result[requestedRole] = closure
        }
        return result
    }
}
