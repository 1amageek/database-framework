import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Maintains the canonical RDF dataset projections for a Persistable entity.
public struct RDFQuadIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let subjectField: FieldIdentity
    private let predicateField: FieldIdentity
    private let objectField: FieldIdentity
    private let graphField: FieldIdentity?
    private let physicalCodec: RDFQuadIndexPhysicalCodec

    package init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        subjectField: String,
        predicateField: String,
        objectField: String,
        graphField: String?
    ) throws {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.subjectField = try Self.requireField(named: subjectField)
        self.predicateField = try Self.requireField(named: predicateField)
        self.objectField = try Self.requireField(named: objectField)
        self.graphField = try graphField.map {
            try Self.requireField(named: $0)
        }
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
        let subject = try requiredTerm(from: item, field: subjectField)
        let predicate = try requiredTerm(from: item, field: predicateField)
        let object = try requiredTerm(from: item, field: objectField)
        let graph = try graphTerm(from: item)
        let quad = try RDFQuad(
            validatingSubject: subject,
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
        field: FieldIdentity
    ) throws -> RDFTerm {
        guard let value = try item.persistedFieldValue(for: field) else {
            throw GraphIndexError.fieldNotFound(
                fieldName: field.name,
                itemType: Item.persistableType
            )
        }
        guard value != .null else {
            throw DataAccessError.nilValueCannotBeIndexed
        }
        guard case .rdfTerm(let term) = value else {
            throw GraphIndexError.invalidFieldType(
                fieldName: field.name,
                expectedType: "RDFTerm",
                actualType: String(describing: value)
            )
        }
        return term
    }

    private func graphTerm(from item: Item) throws -> RDFTerm? {
        guard let graphField else { return nil }
        guard let value = try item.persistedFieldValue(for: graphField) else {
            throw GraphIndexError.fieldNotFound(
                fieldName: graphField.name,
                itemType: Item.persistableType
            )
        }
        guard value != .null else { return nil }
        guard case .rdfTerm(let term) = value else {
            throw GraphIndexError.invalidFieldType(
                fieldName: graphField.name,
                expectedType: "RDFTerm?",
                actualType: String(describing: value)
            )
        }
        return term
    }

    private static func requireField(
        named fieldName: String
    ) throws -> FieldIdentity {
        guard let fieldNumber = Item.fieldNumber(for: fieldName) else {
            throw GraphIndexError.fieldNotFound(
                fieldName: fieldName,
                itemType: Item.persistableType
            )
        }
        return FieldIdentity(name: fieldName, number: fieldNumber)
    }
}
