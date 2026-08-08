import DatabaseKit
import DatabaseTypes
import StorageKit

/// Resolves every model and wire identifier through one canonical storage-key path.
public enum PersistableIdentifierKeyCodec {
    /// Encodes an identifier already represented as a canonical persisted
    /// field value. This is the type-independent counterpart of
    /// `tuple(for model:)`; both produce the same physical storage key.
    public static func tuple(
        forPersistedIdentifier value: FieldValue,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        let identifier = try persistedIdentifier(from: value)
        return try tuple(
            for: identifier.value,
            expectedType: identifier.type,
            limits: limits
        )
    }

    public static func tuple<ID: PersistableIdentifier>(
        for identifier: ID,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try tuple(
            for: identifier.persistableIdentifierValue,
            expectedType: ID.persistableIdentifierType,
            limits: limits
        )
    }

    public static func tuple<Model: Persistable>(
        for model: Model,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try tuple(
            for: model.persistableIdentifierValue,
            expectedType: Model.persistableIdentifierType,
            limits: limits
        )
    }

    public static func tuple(
        for identity: EntityReference,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try tuple(
            for: identity.id,
            expectedType: expectedType,
            limits: limits
        )
    }

    public static func tuple(
        for value: ReferenceIdentifier,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try validate(
            value,
            expectedType: expectedType,
            limits: limits
        )
        return Tuple(PersistableIdentifierTupleElement(value: value))
    }

    public static func validate(
        _ value: ReferenceIdentifier,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) {
        try validateResourceLimits(value, limits: limits)
        do {
            try PersistableIdentifierValidator.validate(
                value,
                as: expectedType
            )
        } catch let error {
            throw .invalidIdentifier(error)
        }
    }

    /// Recovers the logical identifier carried by a canonical storage tuple.
    ///
    /// Integer decoding is guided by the schema because FoundationDB uses the
    /// same physical tuple spelling for nonnegative `Int64` and small `UInt64`
    /// values. Byte values retain the tuple's storage owner without copying.
    public static func value(
        from identifier: Tuple,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> ReferenceIdentifier {
        try validateTypeResourceLimits(expectedType, limits: limits)
        guard identifier.count == 1 else {
            throw .invalidTupleElementCount(actual: identifier.count)
        }

        let value: TupleValue
        do {
            value = try identifier.value(at: 0)
        } catch {
            throw .invalidTupleElementCount(actual: identifier.count)
        }

        var componentCount = 0
        return try decodeNode(
            value,
            expectedType: expectedType,
            depth: 0,
            componentCount: &componentCount,
            limits: limits
        )
    }

    private static func decodeNode(
        _ value: TupleValue,
        expectedType: PersistableIdentifierType,
        depth: Int,
        componentCount: inout Int,
        limits: PersistableIdentifierKeyLimits
    ) throws(PersistableIdentifierKeyError) -> ReferenceIdentifier {
        let (nextCount, overflow) = componentCount.addingReportingOverflow(1)
        guard !overflow, nextCount <= limits.maximumComponentCount else {
            throw .componentCountExceeded(
                actual: overflow ? Int.max : nextCount,
                maximum: limits.maximumComponentCount
            )
        }
        componentCount = nextCount

        switch expectedType {
        case .bool:
            guard case .boolean(let value) = value else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .bool(value)

        case .int8:
            return .int8(
                try signedInteger(
                    value,
                    expectedType: expectedType,
                    as: Int8.self
                )
            )
        case .int16:
            return .int16(
                try signedInteger(
                    value,
                    expectedType: expectedType,
                    as: Int16.self
                )
            )
        case .int32:
            return .int32(
                try signedInteger(
                    value,
                    expectedType: expectedType,
                    as: Int32.self
                )
            )
        case .int64:
            guard case .signedInteger(let value) = value else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .int64(value)

        case .uint8:
            return .uint8(
                try unsignedInteger(
                    value,
                    expectedType: expectedType,
                    as: UInt8.self
                )
            )
        case .uint16:
            return .uint16(
                try unsignedInteger(
                    value,
                    expectedType: expectedType,
                    as: UInt16.self
                )
            )
        case .uint32:
            return .uint32(
                try unsignedInteger(
                    value,
                    expectedType: expectedType,
                    as: UInt32.self
                )
            )
        case .uint64:
            if case .unsignedInteger(let value) = value {
                return .uint64(value)
            }
            guard case .signedInteger(let value) = value, value >= 0 else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .uint64(UInt64(value))

        case .string:
            guard case .string(let value) = value else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .string(value)

        case .bytes:
            guard case .bytes(let value) = value else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .bytes(value)

        case .uuid:
            guard case .uuid(let value) = value else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .uuid(value)

        case .composite(let expectedComponents):
            guard !expectedComponents.isEmpty else {
                throw .invalidTupleValue(expected: expectedType)
            }
            guard depth < limits.maximumCompositeDepth else {
                throw .compositeDepthExceeded(
                    actual: depth + 1,
                    maximum: limits.maximumCompositeDepth
                )
            }
            guard case .nested(let tuple) = value,
                  tuple.count == expectedComponents.count else {
                throw .invalidTupleValue(expected: expectedType)
            }

            var components: [ReferenceIdentifier] = []
            components.reserveCapacity(expectedComponents.count)
            for index in expectedComponents.indices {
                let component: TupleValue
                do {
                    component = try tuple.value(at: index)
                } catch {
                    throw .invalidTupleValue(expected: expectedType)
                }
                components.append(
                    try decodeNode(
                        component,
                        expectedType: expectedComponents[index],
                        depth: depth + 1,
                        componentCount: &componentCount,
                        limits: limits
                    )
                )
            }
            return .composite(components)
        }
    }

    private static func signedInteger<Value: FixedWidthInteger & SignedInteger>(
        _ element: TupleValue,
        expectedType: PersistableIdentifierType,
        as type: Value.Type
    ) throws(PersistableIdentifierKeyError) -> Value {
        guard case .signedInteger(let encoded) = element,
              let value = Value(exactly: encoded) else {
            throw .invalidTupleValue(expected: expectedType)
        }
        return value
    }

    private static func persistedIdentifier(
        from value: FieldValue
    ) throws(PersistableIdentifierKeyError) -> (
        value: ReferenceIdentifier,
        type: PersistableIdentifierType
    ) {
        switch value {
        case .bool(let value):
            return (.bool(value), .bool)
        case .int8(let value):
            return (.int8(value), .int8)
        case .int16(let value):
            return (.int16(value), .int16)
        case .int32(let value):
            return (.int32(value), .int32)
        case .int64(let value):
            return (.int64(value), .int64)
        case .uint8(let value):
            return (.uint8(value), .uint8)
        case .uint16(let value):
            return (.uint16(value), .uint16)
        case .uint32(let value):
            return (.uint32(value), .uint32)
        case .uint64(let value):
            return (.uint64(value), .uint64)
        case .string(let value):
            return (.string(value), .string)
        case .bytes(let value):
            return (.bytes(value), .bytes)
        case .uuid(let value):
            return (.uuid(value), .uuid)
        case .array(let values):
            guard !values.isEmpty else {
                throw .unsupportedPersistedIdentifierValue
            }
            var components: [ReferenceIdentifier] = []
            var componentTypes: [PersistableIdentifierType] = []
            components.reserveCapacity(values.count)
            componentTypes.reserveCapacity(values.count)
            for value in values {
                let component = try persistedIdentifier(from: value)
                components.append(component.value)
                componentTypes.append(component.type)
            }
            return (
                .composite(components),
                .composite(componentTypes)
            )
        default:
            throw .unsupportedPersistedIdentifierValue
        }
    }

    private static func unsignedInteger<
        Value: FixedWidthInteger & UnsignedInteger
    >(
        _ element: TupleValue,
        expectedType: PersistableIdentifierType,
        as type: Value.Type
    ) throws(PersistableIdentifierKeyError) -> Value {
        if case .unsignedInteger(let encoded) = element,
           let value = Value(exactly: encoded) {
            return value
        }
        guard case .signedInteger(let encoded) = element,
              encoded >= 0,
              let value = Value(exactly: encoded) else {
            throw .invalidTupleValue(expected: expectedType)
        }
        return value
    }

    private static func validateResourceLimits(
        _ value: ReferenceIdentifier,
        limits: PersistableIdentifierKeyLimits
    ) throws(PersistableIdentifierKeyError) {
        var pending: [(value: ReferenceIdentifier, depth: Int)] = [(value, 0)]
        var count = 0
        while let node = pending.popLast() {
            let nextCount = count.addingReportingOverflow(1)
            guard !nextCount.overflow,
                  nextCount.partialValue <= limits.maximumComponentCount else {
                throw .componentCountExceeded(
                    actual: nextCount.overflow ? Int.max : nextCount.partialValue,
                    maximum: limits.maximumComponentCount
                )
            }
            count = nextCount.partialValue
            guard case .composite(let components) = node.value else {
                continue
            }
            let nextDepth = node.depth + 1
            guard nextDepth <= limits.maximumCompositeDepth else {
                throw .compositeDepthExceeded(
                    actual: nextDepth,
                    maximum: limits.maximumCompositeDepth
                )
            }
            for component in components.reversed() {
                pending.append((component, nextDepth))
            }
        }
    }

    private static func validateTypeResourceLimits(
        _ type: PersistableIdentifierType,
        limits: PersistableIdentifierKeyLimits
    ) throws(PersistableIdentifierKeyError) {
        var pending: [(type: PersistableIdentifierType, depth: Int)] = [
            (type, 0)
        ]
        var count = 0
        while let node = pending.popLast() {
            let nextCount = count.addingReportingOverflow(1)
            guard !nextCount.overflow,
                  nextCount.partialValue <= limits.maximumComponentCount else {
                throw .componentCountExceeded(
                    actual: nextCount.overflow ? Int.max : nextCount.partialValue,
                    maximum: limits.maximumComponentCount
                )
            }
            count = nextCount.partialValue
            guard case .composite(let components) = node.type else {
                continue
            }
            let nextDepth = node.depth + 1
            guard nextDepth <= limits.maximumCompositeDepth else {
                throw .compositeDepthExceeded(
                    actual: nextDepth,
                    maximum: limits.maximumCompositeDepth
                )
            }
            for component in components.reversed() {
                pending.append((component, nextDepth))
            }
        }
    }
}
