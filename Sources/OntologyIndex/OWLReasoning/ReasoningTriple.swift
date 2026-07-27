import DatabaseTypes
import DatabaseKit

/// A validated RDF triple used as the identity of an ontology inference.
public struct ReasoningTriple: Sendable, Hashable, Comparable {
    public let subject: RDFSubject
    public let predicate: RDFPredicateIRI
    public let object: RDFTerm

    public init(
        subject: RDFSubject,
        predicate: RDFPredicateIRI,
        object: RDFTerm
    ) throws(ReasoningTripleError) {
        do {
            try RDFTermValidation.validate(subject.term, role: .subject)
        } catch let error {
            throw .invalidSubject(error)
        }
        do {
            try RDFTermValidation.validate(predicate.term, role: .predicate)
        } catch let error {
            throw .invalidPredicate(error)
        }
        do {
            try RDFTermValidation.validate(object, role: .object)
        } catch let error {
            throw .invalidObject(error)
        }
        self.subject = subject
        self.predicate = predicate
        self.object = object
    }

    public init(
        subject: RDFSubject,
        predicateIRI: String,
        object: RDFTerm
    ) throws(ReasoningTripleError) {
        let predicate: RDFPredicateIRI
        do {
            predicate = try RDFPredicateIRI(predicateIRI)
        } catch let error {
            throw .invalidPredicateIRI(error)
        }
        try self.init(
            subject: subject,
            predicate: predicate,
            object: object
        )
    }

    public init(
        subjectIRI: String,
        predicateIRI: String,
        objectIRI: String
    ) throws(ReasoningTripleError) {
        let subject: RDFSubject
        do {
            subject = .iri(try RDFIRI(subjectIRI))
        } catch let error {
            throw .invalidSubjectIRI(error)
        }

        let object: RDFTerm
        do {
            object = .iri(try RDFIRI(objectIRI))
        } catch let error {
            throw .invalidObjectIRI(error)
        }

        try self.init(
            subject: subject,
            predicateIRI: predicateIRI,
            object: object
        )
    }

    public init(
        _ subjectIRI: String,
        _ predicateIRI: String,
        _ objectIRI: String
    ) throws(ReasoningTripleError) {
        try self.init(
            subjectIRI: subjectIRI,
            predicateIRI: predicateIRI,
            objectIRI: objectIRI
        )
    }

    public static func < (
        lhs: ReasoningTriple,
        rhs: ReasoningTriple
    ) -> Bool {
        if lhs.subject != rhs.subject {
            return lhs.subject < rhs.subject
        }
        if lhs.predicate != rhs.predicate {
            return lhs.predicate < rhs.predicate
        }
        return lhs.object < rhs.object
    }
}

public enum ReasoningTripleError: Error, Sendable, Equatable {
    case invalidSubject(RDFTermValidationError)
    case invalidPredicate(RDFTermValidationError)
    case invalidObject(RDFTermValidationError)
    case invalidSubjectIRI(RDFIRIError)
    case invalidPredicateIRI(RDFIRIError)
    case invalidObjectIRI(RDFIRIError)
}

extension ReasoningTriple: CustomStringConvertible {
    public var description: String {
        "(\(subject), \(predicate.rawValue), \(object))"
    }
}
