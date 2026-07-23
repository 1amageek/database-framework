import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire

enum DatabaseQueryRowEncoder {
    static func encodeFields(
        _ values: [String: DatabaseValue]
    ) throws -> [DatabaseObjectField] {
        let sortedValues = values.sorted { $0.key < $1.key }
        var fields: [DatabaseObjectField] = []
        fields.reserveCapacity(sortedValues.count)
        for (offset, field) in sortedValues.enumerated() {
            guard let number = UInt32(exactly: offset + 1) else {
                throw DatabaseQueryRowEncodingError.fieldCountExceeded(values.count)
            }
            fields.append(
                DatabaseObjectField(
                    number: number,
                    name: field.key,
                    value: field.value
                )
            )
        }
        return fields
    }

    static func encode(_ row: QueryRow) throws -> QueryExecuteOperation.Row {
        let version: DatabaseBytes?
        if let token = row.version {
            version = try PersistableVersionTokenCodec.digest(from: token)
        } else {
            version = nil
        }
        return QueryExecuteOperation.Row(
            values: try encodeFields(row.fields),
            annotations: try encodeFields(row.annotations),
            version: version
        )
    }
}

enum DatabaseQueryRowEncodingError: Error, Sendable, Equatable {
    case fieldCountExceeded(Int)
}
