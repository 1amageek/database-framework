import DatabaseTypes
import DatabaseEngine
import StorageKit

enum RankValueDirection: Sendable {
    case ascending
    case descending
}

struct RankValueEntry<Item> {
    let item: Item
    let value: FieldValue
    let identifierKey: Bytes
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
    static func identifierKey<ID>(for identifier: ID) throws -> Bytes {
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
        _ entries: [RankValueEntry<Item>],
        direction: RankValueDirection
    ) throws -> [RankValueEntry<Item>] {
        var identifiers: Set<Bytes> = []
        identifiers.reserveCapacity(entries.count)
        for entry in entries {
            guard identifiers.insert(entry.identifierKey).inserted else {
                throw RankValueError.duplicateIdentifier
            }
        }

        return entries.sorted { lhs, rhs in
            if lhs.value == rhs.value {
                switch direction {
                case .ascending:
                    return lhs.identifierKey.lexicographicallyPrecedes(rhs.identifierKey)
                case .descending:
                    return rhs.identifierKey.lexicographicallyPrecedes(lhs.identifierKey)
                }
            }

            switch direction {
            case .ascending:
                return lhs.value < rhs.value
            case .descending:
                return rhs.value < lhs.value
            }
        }
    }
}
