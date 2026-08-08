import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

/// Maintains the canonical RDF projection of an OWL-bound entity.
public struct OWLClassRDFIndexMaintainer: IndexMaintainer {
    public typealias Item = PersistedModel

    private let subspace: Subspace
    private let physicalCodec: RDFQuadIndexPhysicalCodec
    private let entityName: String
    private let classIRI: String
    private let individualIRIBase: String
    private let graph: RDFGraphName?
    private let properties: [OWLDataPropertyDescriptor]

    public init(
        subspace: Subspace,
        entityName: String,
        classIRI: String,
        individualIRIBase: String,
        graph: RDFGraphName?,
        properties: [OWLDataPropertyDescriptor]
    ) {
        self.subspace = subspace
        self.physicalCodec = RDFQuadIndexPhysicalCodec(baseSubspace: subspace)
        self.entityName = entityName
        self.classIRI = classIRI
        self.individualIRIBase = individualIRIBase
        self.graph = graph
        self.properties = properties
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem {
            for key in try buildAllKeys(for: oldItem) {
                try transaction.clear(key: key)
            }
        }
        if let newItem {
            for key in try buildAllKeys(for: newItem) {
                try transaction.setValue([], for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        for key in try buildAllKeys(for: item) {
            try transaction.setValue([], for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        try buildAllKeys(for: item)
    }

    private func buildAllKeys(for item: Item) throws -> [ByteString] {
        let quads = try projectedQuads(for: item)
        var keys: [ByteString] = []
        keys.reserveCapacity(quads.count * 6)

        for quad in quads {
            let writePlan = try RDFQuadIndexWritePlan(quad: quad)
            try writePlan.forEachEntry { entry in
                let key = try physicalCodec.encode(entry)
                try validateKeySize(key)
                keys.append(key)
            }
        }
        return keys
    }

    package func projectedQuads(for item: Item) throws -> [RDFQuad] {
        guard item.entity == entityName else {
            throw OWLClassRDFIndexError.entityMismatch(
                expected: entityName,
                actual: item.entity
            )
        }
        guard let identifier = item.value(forFieldNamed: "id") else {
            throw OWLClassRDFIndexError.missingIdentifier(entity: entityName)
        }
        let subject = try OWLIndividualIRIBuilder.subject(
            baseIRI: individualIRIBase,
            persistableType: entityName,
            identifier: identifier
        )
        let rdfType: RDFPredicateIRI
        do {
            rdfType = try OWLRDFVocabulary.rdfType
        } catch let error {
            throw OWLProjectionError.invalidVocabularyIRI(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                error
            )
        }
        let classTerm: RDFTerm
        do {
            classTerm = try .iri(validating: classIRI)
        } catch let error {
            throw OWLProjectionError.invalidClassIRI(classIRI, error)
        }
        var quads = [
            RDFQuad(
                subject: subject,
                predicate: rdfType,
                object: classTerm,
                graph: graph
            )
        ]
        for property in properties {
            guard let value = item.value(forFieldNamed: property.fieldName) else {
                throw OWLClassRDFIndexError.missingPropertyField(
                    entity: entityName,
                    field: property.fieldName
                )
            }
            let objects: [RDFTerm]
            if let targetTypeName = property.targetTypeName {
                objects = try OWLIndividualIRIBuilder.terms(
                    baseIRI: individualIRIBase,
                    persistableType: targetTypeName,
                    value: value
                )
            } else {
                objects = try OWLCanonicalDataPropertyProjection.terms(from: value)
            }
            let predicate: RDFPredicateIRI
            do {
                predicate = try RDFPredicateIRI(property.iri)
            } catch let error {
                throw OWLProjectionError.invalidPropertyIRI(property.iri, error)
            }
            for object in objects {
                quads.append(
                    RDFQuad(
                        subject: subject,
                        predicate: predicate,
                        object: object,
                        graph: graph
                    )
                )
            }
        }
        for quad in quads {
            try quad.validate()
        }
        return quads
    }
}
