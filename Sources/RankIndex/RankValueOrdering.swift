import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

enum RankValueDirection: Sendable {
    case ascending
    case descending
}

struct RankValueEntry<Item> {
    let item: Item
    let value: FieldValue
    let identifierKey: ByteString
}

enum RankValueOrdering {
    static func numericValue(
        from fieldValue: FieldValue?,
        fieldName: String
    ) throws -> FieldValue {
        guard let fieldValue else {
            throw RankValueError.missingField(fieldName)
        }

        switch fieldValue {
        case .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64:
            return fieldValue
        case .float32(let value):
            guard value.isFinite else {
                throw RankValueError.unorderedFloatingPoint(fieldName: fieldName)
            }
            return fieldValue
        case .float64(let value):
            guard value.isFinite else {
                throw RankValueError.unorderedFloatingPoint(fieldName: fieldName)
            }
            return fieldValue
        case .null:
            throw RankValueError.missingField(fieldName)
        default:
            throw RankValueError.nonNumericField(
                fieldName: fieldName,
                actualType: fieldTypeName(fieldValue)
            )
        }
    }

    /// Encodes the identifier once and retains the owned bytes throughout sort.
    /// The byte order is the same order used by rank index keys.
    static func identifierKey<ID: PersistableIdentifier>(
        for identifier: ID
    ) throws -> ByteString {
        do {
            return try PersistableIdentifierKeyCodec.tuple(
                for: identifier
            ).pack()
        } catch {
            throw RankValueError.invalidIdentifier(
                actualType: identifierTypeName(
                    ID.persistableIdentifierType
                )
            )
        }
    }

    static func sorted<Item>(
        _ entries: consuming [RankValueEntry<Item>],
        direction: RankValueDirection
    ) throws -> [RankValueEntry<Item>] {
        var result = consume entries
        var identifiers: Set<ByteString> = []
        identifiers.reserveCapacity(result.count)
        for entry in result {
            guard identifiers.insert(entry.identifierKey).inserted else {
                throw RankValueError.duplicateIdentifier
            }
        }

        try result.sort { lhs, rhs in
            let left = try numericValue(
                from: lhs.value,
                fieldName: "rank"
            )
            let right = try numericValue(
                from: rhs.value,
                fieldName: "rank"
            )
            guard let comparison = left.compare(to: right) else {
                throw RankValueError.nonNumericField(
                    fieldName: "rank",
                    actualType: "\(fieldTypeName(left)), \(fieldTypeName(right))"
                )
            }

            if comparison == .equal {
                switch direction {
                case .ascending:
                    return lhs.identifierKey.lexicographicallyPrecedes(rhs.identifierKey)
                case .descending:
                    return rhs.identifierKey.lexicographicallyPrecedes(lhs.identifierKey)
                }
            }

            switch direction {
            case .ascending:
                return comparison == .lessThan
            case .descending:
                return comparison == .greaterThan
            }
        }
        return result
    }

    private static func identifierTypeName(
        _ type: PersistableIdentifierType
    ) -> String {
        switch type {
        case .bool: return "Bool"
        case .int8: return "Int8"
        case .int16: return "Int16"
        case .int32: return "Int32"
        case .int64: return "Int64"
        case .uint8: return "UInt8"
        case .uint16: return "UInt16"
        case .uint32: return "UInt32"
        case .uint64: return "UInt64"
        case .string: return "String"
        case .bytes: return "ByteString"
        case .uuid: return "UUID"
        case .composite: return "CompositeIdentifier"
        }
    }

    private static func fieldTypeName(_ value: FieldValue) -> String {
        switch value {
        case .null: return "Null"
        case .bool: return "Bool"
        case .int8: return "Int8"
        case .int16: return "Int16"
        case .int32: return "Int32"
        case .int64: return "Int64"
        case .uint8: return "UInt8"
        case .uint16: return "UInt16"
        case .uint32: return "UInt32"
        case .uint64: return "UInt64"
        case .float32: return "Float32"
        case .float64: return "Float64"
        case .decimal: return "Decimal"
        case .string: return "String"
        case .bytes: return "ByteString"
        case .date: return "CivilDate"
        case .time: return "CivilTime"
        case .dateTime: return "CivilDateTime"
        case .timestamp: return "Timestamp"
        case .timeSpan: return "TimeSpan"
        case .calendarPeriod: return "CalendarPeriod"
        case .geographicPoint: return "GeographicPoint"
        case .geographicPosition: return "GeographicPosition"
        case .vector: return "Vector"
        case .uuid: return "UUID"
        case .array: return "Array"
        case .object: return "FieldObject"
        case .reference: return "EntityReference"
        case .rdfTerm: return "RDFTerm"
        }
    }
}
