import Core
import DatabaseValue
import StorageKit
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Resolves every model and wire identifier through one canonical storage-key path.
public enum PersistableIdentifierKeyCodec {
    public static func tuple<ID: PersistableIdentifier>(
        for identifier: ID,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) -> Tuple {
        try tuple(
            for: identifier.persistableIdentifierValue,
            expectedType: ID.persistableIdentifierType,
            limits: limits
        )
    }

    public static func tuple(
        for model: any Persistable,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) -> Tuple {
        try tuple(
            for: model.persistableIdentifierValue,
            expectedType: type(of: model).persistableIdentifierType,
            limits: limits
        )
    }

    public static func tuple(
        for identity: PersistableIdentity,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) -> Tuple {
        try tuple(
            for: identity.id,
            expectedType: expectedType,
            limits: limits
        )
    }

    public static func tuple(
        for value: PersistableIdentifierValue,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) -> Tuple {
        try validate(
            value,
            expectedType: expectedType,
            limits: limits
        )
        return Tuple(PersistableIdentifierTupleElement(value: value))
    }

    public static func validate(
        _ value: PersistableIdentifierValue,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierValidationError) {
        try PersistableIdentifierValidator.validate(
            value,
            as: expectedType,
            limits: limits
        )
    }

    /// Recovers the logical identifier carried by a canonical storage tuple.
    ///
    /// Integer decoding is guided by the schema because FoundationDB uses the
    /// same physical tuple spelling for nonnegative `Int64` and small `UInt64`
    /// values. Byte values retain the tuple's storage owner without copying.
    public static func value(
        from identifier: Tuple,
        expectedType: PersistableIdentifierType,
        limits: PersistableIdentifierLimits = .default
    ) throws(PersistableIdentifierKeyError) -> PersistableIdentifierValue {
        do {
            try PersistableIdentifierValidator.validate(expectedType, limits: limits)
        } catch let error {
            throw .invalidIdentifier(error)
        }
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
        limits: PersistableIdentifierLimits
    ) throws(PersistableIdentifierKeyError) -> PersistableIdentifierValue {
        let (nextCount, overflow) = componentCount.addingReportingOverflow(1)
        guard !overflow, nextCount <= limits.maximumComponentCount else {
            throw .invalidIdentifier(
                .componentCountExceeded(
                    actual: overflow ? Int.max : nextCount,
                    maximum: limits.maximumComponentCount
                )
            )
        }
        componentCount = nextCount

        if let canonical = element as? PersistableIdentifierTupleElement {
            do {
                try validate(
                    canonical.value,
                    expectedType: expectedType,
                    limits: limits
                )
            } catch let error {
                throw .invalidIdentifier(error)
            }
            return canonical.value
        }

        switch expectedType {
        case .bool:
            guard let value = element as? Bool else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .bool(value)

        case .int64:
            guard let value = element as? Int64 else {
                throw .invalidTupleValue(expected: expectedType)
            }
            return .int64(value)

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
            return .bytes(DatabaseBytes(retaining: value))

        case .uuid:
            guard let value = element as? UUID else {
                throw .invalidTupleValue(expected: expectedType)
            }
            let bytes = value.uuid
            let high = UInt64(bytes.0) << 56
                | UInt64(bytes.1) << 48
                | UInt64(bytes.2) << 40
                | UInt64(bytes.3) << 32
                | UInt64(bytes.4) << 24
                | UInt64(bytes.5) << 16
                | UInt64(bytes.6) << 8
                | UInt64(bytes.7)
            let low = UInt64(bytes.8) << 56
                | UInt64(bytes.9) << 48
                | UInt64(bytes.10) << 40
                | UInt64(bytes.11) << 32
                | UInt64(bytes.12) << 24
                | UInt64(bytes.13) << 16
                | UInt64(bytes.14) << 8
                | UInt64(bytes.15)
            return .uuid(DatabaseUUID(high: high, low: low))

        case .composite(let expectedComponents):
            guard !expectedComponents.isEmpty else {
                throw .invalidIdentifier(.emptyComposite)
            }
            guard depth < limits.maximumCompositeDepth else {
                throw .invalidIdentifier(
                    .compositeDepthExceeded(
                        actual: depth + 1,
                        maximum: limits.maximumCompositeDepth
                    )
                )
            }
            guard let tuple = element as? Tuple,
                  tuple.count == expectedComponents.count else {
                throw .invalidTupleValue(expected: expectedType)
            }

            var components: [PersistableIdentifierValue] = []
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
}
