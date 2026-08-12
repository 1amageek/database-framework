import DatabaseOperationCore
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package enum DatabaseQueryRowEncoder {
    package static func encodeFields(
        _ values: [String: FieldValue]
    ) throws -> FieldObject {
        let sortedValues = values.sorted { $0.key < $1.key }
        return try FieldObject(
            sortedValues.map { field in
                (key: field.key, value: field.value)
            }
        )
    }

    package static func encode(
        _ row: DatabaseEngine.QueryRow,
        columnNames: [String]
    ) throws -> DatabaseWire.QueryRow {
        let version: ByteString?
        if let token = row.version {
            version = try PersistableVersionTokenCodec.digest(from: token)
        } else {
            version = nil
        }
        return DatabaseWire.QueryRow(
            values: columnNames.map { row.fields[$0] ?? .null },
            annotations: try encodeFields(row.annotations),
            version: version
        )
    }
}
