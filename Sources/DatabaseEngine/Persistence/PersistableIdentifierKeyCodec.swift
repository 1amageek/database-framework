import DatabaseKit
import DatabaseTypes
import StorageKit
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

#if canImport(FoundationEssentials)
private typealias TupleUUID = FoundationEssentials.UUID
#else
private typealias TupleUUID = Foundation.UUID
#endif

/// Resolves every model and wire identifier through one canonical storage-key path.
public enum PersistableIdentifierKeyCodec {
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

    public static func tuple(
        for model: any Persistable,
        limits: PersistableIdentifierKeyLimits = .default
    ) throws(PersistableIdentifierKeyError) -> Tuple {
        try tuple(
            for: model.persistableIdentifierValue,
            expectedType: type(of: model).persistableIdentifierType,
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

        let element: any TupleElement
        do {
            element = try identifier.element(at: 0)
        } catch {
            throw .invalidTupleElementCount(actual: identifier.count)
        }

        var componentCount = 0
        return try decodeNode(
            element,
            expectedType: expectedType,
            depth: 0,
            componentCount: &componentCount,
            limits: limits
        )
    }

    private static func decodeNode(
        _ element: any TupleElement,
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

        if let canonical = element as? PersistableIdentifierTupleElement {
            try validate(
                canonical.value,
                expectedType: expectedType,
                limits: limits
            )
            return canonical.value
        }

        switch expectedType {
        case .bool:
            guard let value = element as? Bool else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .bool(value)

        case .int8:
            return .int8(
                try signedInteger(
                    element,
                    expectedType: expectedType,
                    as: Int8.self
                )
            )
        case .int16:
            return .int16(
                try signedInteger(
                    element,
                    expectedType: expectedType,
                    as: Int16.self
                )
            )
        case .int32:
            return .int32(
                try signedInteger(
                    element,
                    expectedType: expectedType,
                    as: Int32.self
                )
            )
        case .int64:
            guard let value = element as? Int64 else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .int64(value)

        case .uint8:
            return .uint8(
                try unsignedInteger(
                    element,
                    expectedType: expectedType,
                    as: UInt8.self
                )
            )
        case .uint16:
            return .uint16(
                try unsignedInteger(
                    element,
                    expectedType: expectedType,
                    as: UInt16.self
                )
            )
        case .uint32:
            return .uint32(
                try unsignedInteger(
                    element,
                    expectedType: expectedType,
                    as: UInt32.self
                )
            )
        case .uint64:
            if let value = element as? UInt64 {
                return .uint64(value)
            }
            guard let value = element as? Int64, value >= 0 else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .uint64(UInt64(value))

        case .string:
            guard let value = element as? String else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .string(value)

        case .bytes:
            guard let value = element as? Bytes else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .bytes(ByteString(retaining: value))

        case .uuid:
            guard let value = element as? TupleUUID else {
                throw .invalidTupleValue(expected: expectedType)
            }
            let bytes = value.uuid
            var high = UInt64(bytes.0) << 56
            high |= UInt64(bytes.1) << 48
            high |= UInt64(bytes.2) << 40
            high |= UInt64(bytes.3) << 32
            high |= UInt64(bytes.4) << 24
            high |= UInt64(bytes.5) << 16
            high |= UInt64(bytes.6) << 8
            high |= UInt64(bytes.7)
            var low = UInt64(bytes.8) << 56
            low |= UInt64(bytes.9) << 48
            low |= UInt64(bytes.10) << 40
            low |= UInt64(bytes.11) << 32
            low |= UInt64(bytes.12) << 24
            low |= UInt64(bytes.13) << 16
            low |= UInt64(bytes.14) << 8
            low |= UInt64(bytes.15)
            return .uuid(DatabaseTypes.UUID(high: high, low: low))

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
            guard let tuple = element as? Tuple,
                  tuple.count == expectedComponents.count else {
                throw .invalidTupleValue(expected: expectedType)
            }

            var components: [ReferenceIdentifier] = []
            components.reserveCapacity(expectedComponents.count)
            for index in expectedComponents.indices {
                let component: any TupleElement
                do {
                    component = try tuple.element(at: index)
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
        _ element: any TupleElement,
        expectedType: PersistableIdentifierType,
        as type: Value.Type
    ) throws(PersistableIdentifierKeyError) -> Value {
        guard let encoded = element as? Int64,
              let value = Value(exactly: encoded) else {
            throw .invalidTupleValue(expected: expectedType)
        }
        return value
    }

    private static func unsignedInteger<
        Value: FixedWidthInteger & UnsignedInteger
    >(
        _ element: any TupleElement,
        expectedType: PersistableIdentifierType,
        as type: Value.Type
    ) throws(PersistableIdentifierKeyError) -> Value {
        if let encoded = element as? UInt64,
           let value = Value(exactly: encoded) {
            return value
        }
        guard let encoded = element as? Int64,
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
