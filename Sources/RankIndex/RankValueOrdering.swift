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
                actualType: String(reflecting: fieldValue)
            )
        }
    }

    /// Encodes the identifier once and retains the owned bytes throughout sort.
    /// The byte order is the same order used by rank index keys.
    static func identifierKey<ID>(for identifier: ID) throws -> ByteString {
        do {
            let element = try TupleEncoder.encode(identifier)
            return Tuple(element).pack()
        } catch {
            throw RankValueError.invalidIdentifier(
                actualType: String(reflecting: ID.self)
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
                    actualType: "\(String(reflecting: left)), \(String(reflecting: right))"
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
}
