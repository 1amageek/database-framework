import DatabaseValue

/// A validated RDF triple used as the identity of an ontology inference.
public struct ReasoningTriple: Sendable, Hashable, Comparable {
    public let subject: DatabaseRDFTerm
    public let predicate: DatabaseRDFPredicateIRI
    public let object: DatabaseRDFTerm

    public init(
        subject: DatabaseRDFTerm,
        predicate: DatabaseRDFPredicateIRI,
        object: DatabaseRDFTerm
    ) throws(ReasoningTripleError) {
        do {
            try DatabaseRDFTermCodec.validate(subject, role: .subject)
        } catch let error {
            throw .invalidSubject(error)
        }
        do {
            try DatabaseRDFTermCodec.validate(object, role: .object)
        } catch let error {
            throw .invalidObject(error)
        }
        self.subject = subject
        self.predicate = predicate
        self.object = object
    }

    public init(
        subject: DatabaseRDFTerm,
        predicateIRI: String,
        object: DatabaseRDFTerm
    ) throws(ReasoningTripleError) {
        let predicate: DatabaseRDFPredicateIRI
        do {
            predicate = try DatabaseRDFPredicateIRI(predicateIRI)
        } catch let error {
            throw .invalidPredicate(error)
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
        try self.init(
            subject: .iri(subjectIRI),
            predicateIRI: predicateIRI,
            object: .iri(objectIRI)
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
    case invalidSubject(DatabaseRDFTermCodecError)
    case invalidPredicate(DatabaseRDFPredicateIRIError)
    case invalidObject(DatabaseRDFTermCodecError)
}

extension ReasoningTriple: CustomStringConvertible {
    public var description: String {
        "(\(subject), \(predicate.rawValue), \(object))"
    }
}
