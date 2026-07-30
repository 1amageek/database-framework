import DatabaseKit
import StorageKit

/// The canonical physical identifier for a row in a polymorphic projection.
///
/// A concrete row is keyed by its model identifier. A polymorphic projection
/// adds the stable concrete-type code in front of that identifier so equal IDs
/// from different member types remain distinct:
///
/// ```text
/// (typeCode, canonicalModelIdentifier)
/// ```
///
/// Construction and validation live here so projection rows, derived indexes,
/// and read-side result matching cannot drift to different tuple layouts.
public enum PolymorphicIdentifierKey {
    public static func tuple(
        for entity: Schema.Entity,
        identifier: Tuple
    ) throws(PolymorphicIdentifierKeyError) -> Tuple {
        guard entity.polymorphicMembership != nil else {
            throw .modelIsNotPolymorphic(typeName: entity.name)
        }
        do {
            _ = try PersistableIdentifierKeyCodec.value(
                from: identifier,
                expectedType: entity.identifierType
            )
        } catch let error {
            throw .invalidModelIdentifier(error)
        }
        return Tuple(
            PolymorphicTypeCode.value(for: entity.name)
        ).appending(identifier)
    }

    public static func tuple<Model: Persistable>(
        for modelType: Model.Type,
        identifier: Tuple
    ) throws(PolymorphicIdentifierKeyError) -> Tuple {
        try requirePolymorphicMembership(modelType)
        try validateModelIdentifier(identifier, for: modelType)
        return Tuple(
            PolymorphicTypeCode.value(for: modelType.persistableType)
        ).appending(identifier)
    }

    public static func validate<Model: Persistable>(
        _ tuple: Tuple,
        for modelType: Model.Type
    ) throws(PolymorphicIdentifierKeyError) {
        guard tuple.count == 2 else {
            throw .invalidElementCount(actual: tuple.count)
        }

        try requirePolymorphicMembership(modelType)
        let typeCodeValue: TupleValue
        do {
            typeCodeValue = try tuple.value(at: 0)
        } catch {
            throw .invalidTypeCode
        }
        guard case .signedInteger(let actualTypeCode) = typeCodeValue else {
            throw .invalidTypeCode
        }

        let expectedTypeCode = PolymorphicTypeCode.value(
            for: modelType.persistableType
        )
        guard actualTypeCode == expectedTypeCode else {
            throw .typeCodeMismatch(
                expected: expectedTypeCode,
                actual: actualTypeCode
            )
        }

        let identifierElement: any TupleElement
        do {
            identifierElement = try tuple.element(at: 1)
        } catch {
            throw .invalidElementCount(actual: tuple.count)
        }
        try validateModelIdentifier(
            Tuple(identifierElement),
            for: modelType
        )
    }

    private static func requirePolymorphicMembership<Model: Persistable>(
        _ modelType: Model.Type
    ) throws(PolymorphicIdentifierKeyError) {
        guard modelType.polymorphicMembership != nil else {
            throw .modelIsNotPolymorphic(typeName: modelType.persistableType)
        }
    }

    private static func validateModelIdentifier<Model: Persistable>(
        _ identifier: Tuple,
        for modelType: Model.Type
    ) throws(PolymorphicIdentifierKeyError) {
        do {
            _ = try PersistableIdentifierKeyCodec.value(
                from: identifier,
                expectedType: modelType.persistableIdentifierType
            )
        } catch let error {
            throw .invalidModelIdentifier(error)
        }
    }
}

public enum PolymorphicIdentifierKeyError: Error, Sendable, Equatable {
    case modelIsNotPolymorphic(typeName: String)
    case invalidElementCount(actual: Int)
    case invalidTypeCode
    case typeCodeMismatch(expected: Int64, actual: Int64)
    case invalidModelIdentifier(PersistableIdentifierKeyError)
}
