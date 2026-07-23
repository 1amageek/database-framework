import Core
import DatabaseEngine
import DatabaseValue
import Graph
import StorageKit

/// Maintains the canonical RDF dataset projections for a Persistable entity.
public struct RDFQuadIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let subjectField: String
    private let predicateField: String
    private let objectField: String
    private let graphField: String?
    private let physicalCodec: RDFQuadIndexPhysicalCodec

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        subjectField: String,
        predicateField: String,
        objectField: String,
        graphField: String?
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.subjectField = subjectField
        self.predicateField = predicateField
        self.objectField = objectField
        self.graphField = graphField
        self.physicalCodec = RDFQuadIndexPhysicalCodec(baseSubspace: subspace)
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem {
            for key in try buildIndexKeys(for: oldItem) {
                try transaction.clear(key: key)
            }
        }
        if let newItem {
            let value = try CoveringValueBuilder.build(
                for: newItem,
                index: index
            )
            for key in try buildIndexKeys(for: newItem) {
                try transaction.setValue(value, for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        let value = try CoveringValueBuilder.build(
            for: item,
            index: index
        )
        for key in try buildIndexKeys(for: item) {
            try transaction.setValue(value, for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        try buildIndexKeys(for: item)
    }

    private func buildIndexKeys(for item: Item) throws -> [Bytes] {
        let subject = try requiredTerm(from: item, fieldName: subjectField)
        let predicate = try requiredTerm(from: item, fieldName: predicateField)
        let object = try requiredTerm(from: item, fieldName: objectField)
        let graph = try graphTerm(from: item)
        let quad = RDFQuad(
            subject: subject,
            predicate: predicate,
            object: object,
            graph: graph
        )
        let writePlan = try RDFQuadIndexWritePlan(quad: quad)
        var keys: [Bytes] = []
        keys.reserveCapacity(6)
        try writePlan.forEachEntry { entry in
            let key = try physicalCodec.encode(entry)
            try validateKeySize(key)
            keys.append(key)
        }
        return keys
    }

    private func requiredTerm(
        from item: Item,
        fieldName: String
    ) throws -> DatabaseRDFTerm {
        guard let value = item[dynamicMember: fieldName] else {
            throw DataAccessError.nilValueCannotBeIndexed
        }
        guard let term = value as? DatabaseRDFTerm else {
            throw GraphIndexError.invalidFieldType(
                fieldName: fieldName,
                expectedType: "DatabaseRDFTerm",
                actualType: String(describing: type(of: value))
            )
        }
        return term
    }

    private func graphTerm(from item: Item) throws -> DatabaseRDFTerm? {
        guard let graphField else { return nil }
        guard let value = item[dynamicMember: graphField] else { return nil }
        guard let term = value as? DatabaseRDFTerm else {
            throw GraphIndexError.invalidFieldType(
                fieldName: graphField,
                expectedType: "DatabaseRDFTerm?",
                actualType: String(describing: type(of: value))
            )
        }
        return term
    }
}
