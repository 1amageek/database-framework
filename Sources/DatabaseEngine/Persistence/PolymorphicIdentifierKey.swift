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
        for modelType: any Persistable.Type,
        identifier: Tuple
    ) throws(PolymorphicIdentifierKeyError) -> Tuple {
        let polymorphicType = try requirePolymorphicType(modelType)
        try validateModelIdentifier(identifier, for: modelType)
        return Tuple(
            polymorphicType.typeCode(for: modelType.persistableType)
        ).appending(identifier)
    }

    public static func validate(
        _ tuple: Tuple,
        for modelType: any Persistable.Type
    ) throws(PolymorphicIdentifierKeyError) {
        guard tuple.count == 2 else {
            throw .invalidElementCount(actual: tuple.count)
        }

        let polymorphicType = try requirePolymorphicType(modelType)
        let actualTypeCode: Int64
        do {
            guard let value = try tuple.element(at: 0) as? Int64 else {
                throw PolymorphicIdentifierKeyError.invalidTypeCode
            }
            actualTypeCode = value
        } catch let error as PolymorphicIdentifierKeyError {
            throw error
        } catch {
            throw .invalidTypeCode
        }

        let expectedTypeCode = polymorphicType.typeCode(
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

    private static func requirePolymorphicType(
        _ modelType: any Persistable.Type
    ) throws(PolymorphicIdentifierKeyError) -> any Polymorphable.Type {
        guard let polymorphicType = modelType as? any Polymorphable.Type else {
            throw .modelIsNotPolymorphic(typeName: modelType.persistableType)
        }
        return polymorphicType
    }

    private static func validateModelIdentifier(
        _ identifier: Tuple,
        for modelType: any Persistable.Type
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
