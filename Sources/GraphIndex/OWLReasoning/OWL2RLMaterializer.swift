// OWL2RLMaterializer.swift
// GraphIndex - OWL 2 RL Rule Materialization
//
// Implements forward-chaining materialization of OWL 2 RL rules at write time.
//
// Reference: W3C OWL 2 RL Profile https://www.w3.org/TR/owl2-profiles/#OWL_2_RL

import Foundation
import FoundationDB
import Graph

/// OWL 2 RL Materializer for forward-chaining inference
///
/// Applies OWL 2 RL rules at write time to materialize inferred triples.
/// Uses a hybrid approach where some rules are materialized (hierarchy, inverse)
/// while others are handled via query rewriting (transitive, property chains).
///
/// **Materialized Rules**:
/// - `cax-sco`: Subclass typing (x:C1, C1⊑C2 → x:C2)
/// - `cax-eqc1/2`: Equivalent class typing
/// - `prp-spo1`: Subproperty propagation
/// - `prp-inv1/2`: Inverse property generation
/// - `prp-symp`: Symmetric property propagation
///
/// **Reference**: RDFox (Oxford) for hybrid reasoning strategy
///
/// **Example**:
/// ```swift
/// let materializer = OWL2RLMaterializer(ontologyStore: store)
///
/// // On triple write
/// let inferred = try await materializer.materializeOnWrite(
///     triple: ("ex:Alice", "rdf:type", "ex:Employee"),
///     ontology: ontology,
///     transaction: transaction
/// )
///
/// // Store inferred triples
/// for (triple, provenance) in inferred.inferred {
///     try await storeInferred(triple, provenance, transaction)
/// }
/// ```
public struct OWL2RLMaterializer: Sendable {

    // MARK: - Configuration

    /// Configuration for materialization
    public struct Configuration: Sendable {
        /// Maximum inference depth for recursive rules
        public let maxInferenceDepth: Int

        /// Whether to track provenance for inferred triples
        public let trackProvenance: Bool

        /// Whether to detect inconsistencies during materialization
        public let detectInconsistencies: Bool

        /// Batch size for processing multiple inferences
        public let batchSize: Int

        public init(
            maxInferenceDepth: Int = 10,
            trackProvenance: Bool = true,
            detectInconsistencies: Bool = true,
            batchSize: Int = 100
        ) {
            self.maxInferenceDepth = maxInferenceDepth
            self.trackProvenance = trackProvenance
            self.detectInconsistencies = detectInconsistencies
            self.batchSize = batchSize
        }

        /// Default configuration
        public static let `default` = Configuration()
    }

    // MARK: - Properties

    /// Ontology store for hierarchy lookups
    private let ontologyStore: OntologyStore

    /// Configuration
    private let configuration: Configuration

    // MARK: - Initialization

    public init(ontologyStore: OntologyStore, configuration: Configuration = .default) {
        self.ontologyStore = ontologyStore
        self.configuration = configuration
    }

    // MARK: - Materialization Entry Point

    /// Materialize inferences for a newly written triple
    ///
    /// - Parameters:
    ///   - triple: The triple being written (subject, predicate, object)
    ///   - ontologyIRI: IRI of the ontology to use for reasoning
    ///   - transaction: The active transaction
    /// - Returns: Inference result containing inferred triples and any inconsistencies
    public func materializeOnWrite(
        triple: (subject: String, predicate: String, object: String),
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> InferenceResult {
        var result = InferenceResult()
        let startTime = Date()

        // Create antecedent for tracking
        let baseTriple = TripleKey(triple.subject, triple.predicate, triple.object)

        // Apply rules based on predicate
        switch triple.predicate {
        case WellKnownIRI.rdfType:
            // Instance typing: apply class hierarchy rules
            try await materializeClassHierarchy(
                individual: triple.subject,
                classIRI: triple.object,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )

        case WellKnownIRI.rdfsSubClassOf:
            // Class hierarchy assertion: propagate to existing instances
            try await materializeSubClassAssertion(
                subClass: triple.subject,
                superClass: triple.object,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )

        case WellKnownIRI.rdfsSubPropertyOf:
            // Property hierarchy: apply prp-spo1
            try await materializeSubPropertyAssertion(
                subProperty: triple.subject,
                superProperty: triple.object,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )

        default:
            // Regular property assertion: check for inverse, symmetric, domain/range
            try await materializePropertyAssertion(
                subject: triple.subject,
                predicate: triple.predicate,
                object: triple.object,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )
        }

        result.statistics.inferenceTime = Date().timeIntervalSince(startTime)
        return result
    }

    // MARK: - Class Hierarchy Materialization (cax-sco, cax-eqc1/2)

    /// Materialize class hierarchy for an instance typing
    ///
    /// When x rdf:type C is asserted, for every superclass S of C,
    /// infer x rdf:type S.
    private func materializeClassHierarchy(
        individual: String,
        classIRI: String,
        ontologyIRI: String,
        baseTriple: TripleKey,
        transaction: any TransactionProtocol,
        result: inout InferenceResult
    ) async throws {
        result.statistics.ruleApplications += 1

        // Get all superclasses from the stored hierarchy
        let superClasses = try await ontologyStore.getSuperClasses(
            of: classIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        // For each superclass, infer typing
        for superClass in superClasses {
            let inferredTriple = TripleKey(individual, WellKnownIRI.rdfType, superClass)

            // Create provenance
            let provenance = InferenceProvenance(
                rule: .caxSco,
                antecedents: [
                    baseTriple,
                    TripleKey(classIRI, WellKnownIRI.rdfsSubClassOf, superClass)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // Handle equivalent classes (cax-eqc1/2)
        let equivalentClasses = try await ontologyStore.getEquivalentClasses(
            of: classIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for equivalentClass in equivalentClasses where equivalentClass != classIRI {
            let inferredTriple = TripleKey(individual, WellKnownIRI.rdfType, equivalentClass)

            let provenance = InferenceProvenance(
                rule: .caxEqc1,
                antecedents: [
                    baseTriple,
                    TripleKey(classIRI, WellKnownIRI.owlEquivalentClass, equivalentClass)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }
    }

    /// Materialize when a subClassOf assertion is added
    private func materializeSubClassAssertion(
        subClass: String,
        superClass: String,
        ontologyIRI: String,
        baseTriple: TripleKey,
        transaction: any TransactionProtocol,
        result: inout InferenceResult
    ) async throws {
        // scm-sco: Transitivity of subClassOf
        // If we add C1 rdfs:subClassOf C2, and C2 rdfs:subClassOf C3 exists,
        // then infer C1 rdfs:subClassOf C3
        result.statistics.ruleApplications += 1

        // Get superclasses of the new superclass
        let transitiveSuperClasses = try await ontologyStore.getSuperClasses(
            of: superClass,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for transitiveSuper in transitiveSuperClasses {
            let inferredTriple = TripleKey(subClass, WellKnownIRI.rdfsSubClassOf, transitiveSuper)

            let provenance = InferenceProvenance(
                rule: .scmSco,
                antecedents: [
                    baseTriple,
                    TripleKey(superClass, WellKnownIRI.rdfsSubClassOf, transitiveSuper)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }
    }

    // MARK: - Property Hierarchy Materialization (prp-spo1)

    /// Materialize when a subPropertyOf assertion is added
    private func materializeSubPropertyAssertion(
        subProperty: String,
        superProperty: String,
        ontologyIRI: String,
        baseTriple: TripleKey,
        transaction: any TransactionProtocol,
        result: inout InferenceResult
    ) async throws {
        // scm-spo: Transitivity of subPropertyOf
        result.statistics.ruleApplications += 1

        let transitiveSuperProperties = try await ontologyStore.getSuperProperties(
            of: superProperty,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for transitiveSuper in transitiveSuperProperties {
            let inferredTriple = TripleKey(subProperty, WellKnownIRI.rdfsSubPropertyOf, transitiveSuper)

            let provenance = InferenceProvenance(
                rule: .scmSpo,
                antecedents: [
                    baseTriple,
                    TripleKey(superProperty, WellKnownIRI.rdfsSubPropertyOf, transitiveSuper)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }
    }

    // MARK: - Property Assertion Materialization

    /// Materialize inferences for a property assertion
    private func materializePropertyAssertion(
        subject: String,
        predicate: String,
        object: String,
        ontologyIRI: String,
        baseTriple: TripleKey,
        transaction: any TransactionProtocol,
        result: inout InferenceResult
    ) async throws {
        // prp-spo1: If p1 rdfs:subPropertyOf p2, and x p1 y, then x p2 y
        let superProperties = try await ontologyStore.getSuperProperties(
            of: predicate,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for superProp in superProperties {
            result.statistics.ruleApplications += 1
            let inferredTriple = TripleKey(subject, superProp, object)

            let provenance = InferenceProvenance(
                rule: .prpSpo1,
                antecedents: [
                    baseTriple,
                    TripleKey(predicate, WellKnownIRI.rdfsSubPropertyOf, superProp)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-inv1/2: If p1 owl:inverseOf p2, and x p1 y, then y p2 x
        if let inverseProperty = try await ontologyStore.getInverse(
            of: predicate,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        ) {
            result.statistics.ruleApplications += 1
            let inferredTriple = TripleKey(object, inverseProperty, subject)

            let provenance = InferenceProvenance(
                rule: .prpInv1,
                antecedents: [
                    baseTriple,
                    TripleKey(predicate, WellKnownIRI.owlInverseOf, inverseProperty)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-symp: If p is symmetric, and x p y, then y p x
        if try await ontologyStore.isSymmetric(
            property: predicate,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        ) {
            result.statistics.ruleApplications += 1
            let inferredTriple = TripleKey(object, predicate, subject)

            let provenance = InferenceProvenance(
                rule: .prpSymp,
                antecedents: [
                    baseTriple,
                    TripleKey(predicate, WellKnownIRI.rdfType, WellKnownIRI.owlSymmetricProperty)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-dom: Domain inference
        let domains = try await ontologyStore.getDomains(
            of: predicate,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for domain in domains {
            result.statistics.ruleApplications += 1
            let inferredTriple = TripleKey(subject, WellKnownIRI.rdfType, domain)

            let provenance = InferenceProvenance(
                rule: .prpDom,
                antecedents: [
                    baseTriple,
                    TripleKey(predicate, WellKnownIRI.rdfsDomain, domain)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-rng: Range inference
        let ranges = try await ontologyStore.getRanges(
            of: predicate,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for range in ranges {
            result.statistics.ruleApplications += 1
            let inferredTriple = TripleKey(object, WellKnownIRI.rdfType, range)

            let provenance = InferenceProvenance(
                rule: .prpRng,
                antecedents: [
                    baseTriple,
                    TripleKey(predicate, WellKnownIRI.rdfsRange, range)
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // Detect inconsistencies if configured
        if configuration.detectInconsistencies {
            try await detectInconsistencies(
                subject: subject,
                predicate: predicate,
                object: object,
                ontologyIRI: ontologyIRI,
                transaction: transaction,
                result: &result
            )
        }
    }

    // MARK: - Inconsistency Detection

    /// Detect inconsistencies from a property assertion
    private func detectInconsistencies(
        subject: String,
        predicate: String,
        object: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol,
        result: inout InferenceResult
    ) async throws {
        // prp-irp: Irreflexive property violation
        if subject == object {
            if try await ontologyStore.isIrreflexive(
                property: predicate,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            ) {
                result.inconsistencies.append(InconsistencyReport(
                    rule: .prpIrp,
                    involvedTriples: [TripleKey(subject, predicate, object)],
                    description: "Irreflexive property \(predicate) used reflexively on \(subject)"
                ))
                result.statistics.inconsistenciesDetected += 1
            }
        }

        // prp-asyp: Asymmetric property violation (would need to query existing triples)
        // This is handled at consistency check time rather than materialization
    }
}

// MARK: - Well-Known IRIs

/// Well-known RDF/RDFS/OWL IRIs for materialization
public enum WellKnownIRI {
    // RDF
    public static let rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    // RDFS
    public static let rdfsSubClassOf = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    public static let rdfsSubPropertyOf = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
    public static let rdfsDomain = "http://www.w3.org/2000/01/rdf-schema#domain"
    public static let rdfsRange = "http://www.w3.org/2000/01/rdf-schema#range"

    // OWL
    public static let owlThing = "http://www.w3.org/2002/07/owl#Thing"
    public static let owlNothing = "http://www.w3.org/2002/07/owl#Nothing"
    public static let owlSameAs = "http://www.w3.org/2002/07/owl#sameAs"
    public static let owlDifferentFrom = "http://www.w3.org/2002/07/owl#differentFrom"
    public static let owlEquivalentClass = "http://www.w3.org/2002/07/owl#equivalentClass"
    public static let owlEquivalentProperty = "http://www.w3.org/2002/07/owl#equivalentProperty"
    public static let owlInverseOf = "http://www.w3.org/2002/07/owl#inverseOf"
    public static let owlSymmetricProperty = "http://www.w3.org/2002/07/owl#SymmetricProperty"
    public static let owlTransitiveProperty = "http://www.w3.org/2002/07/owl#TransitiveProperty"
    public static let owlFunctionalProperty = "http://www.w3.org/2002/07/owl#FunctionalProperty"
    public static let owlInverseFunctionalProperty = "http://www.w3.org/2002/07/owl#InverseFunctionalProperty"
    public static let owlIrreflexiveProperty = "http://www.w3.org/2002/07/owl#IrreflexiveProperty"
    public static let owlAsymmetricProperty = "http://www.w3.org/2002/07/owl#AsymmetricProperty"
    public static let owlReflexiveProperty = "http://www.w3.org/2002/07/owl#ReflexiveProperty"
}

// MARK: - Inferred Triple

/// An inferred triple with its provenance
public struct InferredTriple: Sendable {
    /// The inferred triple
    public let triple: TripleKey

    /// Provenance tracking how this triple was derived
    public let provenance: InferenceProvenance

    public init(triple: TripleKey, provenance: InferenceProvenance) {
        self.triple = triple
        self.provenance = provenance
    }
}
