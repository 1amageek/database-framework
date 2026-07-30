// OWL2RLMaterializer.swift
// GraphIndex - OWL 2 RL Rule Materialization
//
// Implements forward-chaining materialization of OWL 2 RL rules at write time.
//
// Reference: W3C OWL 2 RL Profile https://www.w3.org/TR/owl2-profiles/#OWL_2_RL

import DatabaseTypes
import StorageKit
import DatabaseKit

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
        /// Whether to detect inconsistencies during materialization
        public let detectInconsistencies: Bool

        public init(
            detectInconsistencies: Bool = true
        ) {
            self.detectInconsistencies = detectInconsistencies
        }

        /// Default configuration
        public static let `default` = Configuration()
    }

    // MARK: - Properties

    /// Ontology store for hierarchy lookups
    private let ontologyStore: OntologyStore

    /// Configuration
    private let configuration: Configuration

    /// Runtime-provided monotonic time source.
    private let clock: any StorageMonotonicClock

    // MARK: - Initialization

    public init(
        ontologyStore: OntologyStore,
        clock: any StorageMonotonicClock,
        configuration: Configuration = .default
    ) {
        self.ontologyStore = ontologyStore
        self.clock = clock
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
        triple: ReasoningTriple,
        ontologyIRI: String,
        transaction: any TransactionAccess
    ) async throws -> InferenceResult {
        var result = InferenceResult()
        let start = clock.now
        let baseTriple = triple

        // Apply rules based on predicate
        switch triple.predicate.rawValue {
        case WellKnownIRI.rdfType:
            let classIRI = try requireIRI(
                triple.object,
                position: .object,
                rule: .caxSco
            )
            // Instance typing: apply class hierarchy rules
            try await materializeClassHierarchy(
                individual: triple.subject,
                classIRI: classIRI,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )

        case WellKnownIRI.rdfsSubClassOf:
            let subClass = try requireIRI(
                triple.subject,
                position: .subject,
                rule: .scmSco
            )
            let superClass = try requireIRI(
                triple.object,
                position: .object,
                rule: .scmSco
            )
            // Class hierarchy assertion: propagate to existing instances
            try await materializeSubClassAssertion(
                subClass: subClass,
                superClass: superClass,
                ontologyIRI: ontologyIRI,
                baseTriple: baseTriple,
                transaction: transaction,
                result: &result
            )

        case WellKnownIRI.rdfsSubPropertyOf:
            let subProperty = try requireIRI(
                triple.subject,
                position: .subject,
                rule: .scmSpo
            )
            let superProperty = try requireIRI(
                triple.object,
                position: .object,
                rule: .scmSpo
            )
            // Property hierarchy: apply prp-spo1
            try await materializeSubPropertyAssertion(
                subProperty: subProperty,
                superProperty: superProperty,
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

        result.statistics.inferenceTime = start.duration(to: clock.now)
        return result
    }

    // MARK: - Class Hierarchy Materialization (cax-sco, cax-eqc1/2)

    /// Materialize class hierarchy for an instance typing
    ///
    /// When x rdf:type C is asserted, for every superclass S of C,
    /// infer x rdf:type S.
    private func materializeClassHierarchy(
        individual: RDFSubject,
        classIRI: String,
        ontologyIRI: String,
        baseTriple: ReasoningTriple,
        transaction: any TransactionAccess,
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
            let inferredTriple = try reasoningTriple(
                subject: individual,
                predicateIRI: WellKnownIRI.rdfType,
                object: try iriTerm(superClass)
            )

            // Create provenance
            let provenance = InferenceProvenance(
                rule: .caxSco,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: classIRI,
                        predicateIRI: WellKnownIRI.rdfsSubClassOf,
                        objectIRI: superClass
                    )
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
            let inferredTriple = try reasoningTriple(
                subject: individual,
                predicateIRI: WellKnownIRI.rdfType,
                object: try iriTerm(equivalentClass)
            )

            let provenance = InferenceProvenance(
                rule: .caxEqc1,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: classIRI,
                        predicateIRI: WellKnownIRI.owlEquivalentClass,
                        objectIRI: equivalentClass
                    )
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
        baseTriple: ReasoningTriple,
        transaction: any TransactionAccess,
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
            let inferredTriple = try iriTriple(
                subjectIRI: subClass,
                predicateIRI: WellKnownIRI.rdfsSubClassOf,
                objectIRI: transitiveSuper
            )

            let provenance = InferenceProvenance(
                rule: .scmSco,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: superClass,
                        predicateIRI: WellKnownIRI.rdfsSubClassOf,
                        objectIRI: transitiveSuper
                    )
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
        baseTriple: ReasoningTriple,
        transaction: any TransactionAccess,
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
            let inferredTriple = try iriTriple(
                subjectIRI: subProperty,
                predicateIRI: WellKnownIRI.rdfsSubPropertyOf,
                objectIRI: transitiveSuper
            )

            let provenance = InferenceProvenance(
                rule: .scmSpo,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: superProperty,
                        predicateIRI: WellKnownIRI.rdfsSubPropertyOf,
                        objectIRI: transitiveSuper
                    )
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }
    }

    // MARK: - Property Assertion Materialization

    /// Materialize inferences for a property assertion
    private func materializePropertyAssertion(
        subject: RDFSubject,
        predicate: RDFPredicateIRI,
        object: RDFTerm,
        ontologyIRI: String,
        baseTriple: ReasoningTriple,
        transaction: any TransactionAccess,
        result: inout InferenceResult
    ) async throws {
        let predicateIRI = predicate.rawValue
        let propertyDefinition = try await ontologyStore.getProperty(
            predicateIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        // prp-spo1: If p1 rdfs:subPropertyOf p2, and x p1 y, then x p2 y
        let superProperties = try await ontologyStore.getSuperProperties(
            of: predicateIRI,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        for superProp in superProperties {
            result.statistics.ruleApplications += 1
            let inferredTriple = try reasoningTriple(
                subject: subject,
                predicateIRI: superProp,
                object: object
            )

            let provenance = InferenceProvenance(
                rule: .prpSpo1,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: predicateIRI,
                        predicateIRI: WellKnownIRI.rdfsSubPropertyOf,
                        objectIRI: superProp
                    )
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-inv1/2: If p1 owl:inverseOf p2, and x p1 y, then y p2 x
        if let inverseProperty = propertyDefinition?.inverseOf {
            result.statistics.ruleApplications += 1
            let reversedSubject = try requireRDFSubject(
                object,
                rule: .prpInv1
            )
            let inferredTriple = try reasoningTriple(
                subject: reversedSubject,
                predicateIRI: inverseProperty,
                object: subject.term
            )

            let provenance = InferenceProvenance(
                rule: .prpInv1,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: predicateIRI,
                        predicateIRI: WellKnownIRI.owlInverseOf,
                        objectIRI: inverseProperty
                    )
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-symp: If p is symmetric, and x p y, then y p x
        if propertyDefinition?.isSymmetric == true {
            result.statistics.ruleApplications += 1
            let reversedSubject = try requireRDFSubject(
                object,
                rule: .prpSymp
            )
            let inferredTriple = try ReasoningTriple(
                subject: reversedSubject,
                predicate: predicate,
                object: subject.term
            )

            let provenance = InferenceProvenance(
                rule: .prpSymp,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: predicateIRI,
                        predicateIRI: WellKnownIRI.rdfType,
                        objectIRI: WellKnownIRI.owlSymmetricProperty
                    )
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-dom: Domain inference
        let domains = propertyDefinition?.domains ?? []

        for domain in domains {
            result.statistics.ruleApplications += 1
            let inferredTriple = try reasoningTriple(
                subject: subject,
                predicateIRI: WellKnownIRI.rdfType,
                object: try iriTerm(domain)
            )

            let provenance = InferenceProvenance(
                rule: .prpDom,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: predicateIRI,
                        predicateIRI: WellKnownIRI.rdfsDomain,
                        objectIRI: domain
                    )
                ]
            )

            result.inferred.append((inferredTriple, provenance))
            result.statistics.triplesInferred += 1
        }

        // prp-rng: Range inference
        let ranges = propertyDefinition?.type == .objectProperty
            ? propertyDefinition?.ranges ?? []
            : []

        for range in ranges {
            result.statistics.ruleApplications += 1
            let rangeSubject = try requireRDFSubject(
                object,
                rule: .prpRng
            )
            let inferredTriple = try reasoningTriple(
                subject: rangeSubject,
                predicateIRI: WellKnownIRI.rdfType,
                object: try iriTerm(range)
            )

            let provenance = InferenceProvenance(
                rule: .prpRng,
                antecedents: [
                    baseTriple,
                    try iriTriple(
                        subjectIRI: predicateIRI,
                        predicateIRI: WellKnownIRI.rdfsRange,
                        objectIRI: range
                    )
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
                baseTriple: baseTriple,
                propertyDefinition: propertyDefinition,
                result: &result
            )
        }
    }

    // MARK: - Inconsistency Detection

    /// Detect inconsistencies from a property assertion
    private func detectInconsistencies(
        subject: RDFSubject,
        predicate: RDFPredicateIRI,
        object: RDFTerm,
        baseTriple: ReasoningTriple,
        propertyDefinition: StoredPropertyDefinition?,
        result: inout InferenceResult
    ) async throws {
        // prp-irp: Irreflexive property violation
        if subject.term == object && propertyDefinition?.isIrreflexive == true {
            result.inconsistencies.append(InconsistencyReport(
                rule: .prpIrp,
                involvedTriples: [baseTriple],
                description: "Irreflexive property \(predicate.rawValue) used reflexively on \(subject)"
            ))
            result.statistics.inconsistenciesDetected += 1
        }

        // prp-asyp: Asymmetric property violation (would need to query existing triples)
        // This is handled at consistency check time rather than materialization
    }

    private func reasoningTriple(
        subject: RDFSubject,
        predicateIRI: String,
        object: RDFTerm
    ) throws -> ReasoningTriple {
        do {
            return try ReasoningTriple(
                subject: subject,
                predicateIRI: predicateIRI,
                object: object
            )
        } catch let error {
            throw OWL2RLMaterializationError.invalidGeneratedTriple(error)
        }
    }

    private func iriTriple(
        subjectIRI: String,
        predicateIRI: String,
        objectIRI: String
    ) throws -> ReasoningTriple {
        do {
            return try ReasoningTriple(
                subjectIRI: subjectIRI,
                predicateIRI: predicateIRI,
                objectIRI: objectIRI
            )
        } catch let error {
            throw OWL2RLMaterializationError.invalidGeneratedTriple(error)
        }
    }

    private func requireIRI(
        _ term: RDFTerm,
        position: OWL2RLMaterializationPosition,
        rule: OWL2RLRule
    ) throws -> String {
        guard case .iri(let value) = term else {
            throw OWL2RLMaterializationError.expectedIRI(
                rule: rule,
                position: position,
                actual: termKind(term)
            )
        }
        return value.rawValue
    }

    private func requireIRI(
        _ subject: RDFSubject,
        position: OWL2RLMaterializationPosition,
        rule: OWL2RLRule
    ) throws -> String {
        guard case .iri(let value) = subject else {
            throw OWL2RLMaterializationError.expectedIRI(
                rule: rule,
                position: position,
                actual: .blankNode
            )
        }
        return value.rawValue
    }

    private func requireRDFSubject(
        _ term: RDFTerm,
        rule: OWL2RLRule
    ) throws -> RDFSubject {
        switch term {
        case .iri(let iri):
            return .iri(iri)
        case .blankNode(let identifier):
            return .blankNode(identifier)
        case .literal, .tripleTerm:
            throw OWL2RLMaterializationError.expectedRDFSubject(
                rule: rule,
                actual: termKind(term)
            )
        }
    }

    private func termKind(_ term: RDFTerm) -> RDFTermKind {
        switch term {
        case .blankNode: .blankNode
        case .iri: .iri
        case .literal: .literal
        case .tripleTerm: .tripleTerm
        }
    }

    private func iriTerm(_ value: String) throws -> RDFTerm {
        do {
            return .iri(try RDFIRI(value))
        } catch let error {
            throw OWL2RLMaterializationError.invalidIRI(error)
        }
    }

}

public enum OWL2RLMaterializationPosition: Sendable, Equatable {
    case subject
    case object
}

public enum OWL2RLMaterializationError: Error, Sendable, Equatable {
    case expectedIRI(
        rule: OWL2RLRule,
        position: OWL2RLMaterializationPosition,
        actual: RDFTermKind
    )
    case expectedRDFSubject(
        rule: OWL2RLRule,
        actual: RDFTermKind
    )
    case invalidGeneratedTriple(ReasoningTripleError)
    case invalidIRI(RDFIRIError)
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
    public let triple: ReasoningTriple

    /// Provenance tracking how this triple was derived
    public let provenance: InferenceProvenance

    public init(triple: ReasoningTriple, provenance: InferenceProvenance) {
        self.triple = triple
        self.provenance = provenance
    }
}
