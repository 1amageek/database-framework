import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine

/// Adapts execution metadata at the canonical result boundary.
@_spi(DatabaseExecution)
public enum QueryRowProjection {
    public static func fields(_ values: [String: FieldValue]) throws -> FieldObject {
        // One destination array is required when a runtime dictionary becomes a
        // canonical FieldObject. Payload owners remain shared; sorting is in place.
        try FieldObject(values.map { (key: $0.key, value: $0.value) })
    }

    public static func encode(_ row: ExecutionQueryRow,
                              columns: [QueryColumn]) throws -> DatabaseKit.QueryRow {
        try row.validate(columns: columns)
        let version: ByteString?
        if let token = row.version { version = try PersistableVersionTokenCodec.digest(from: token) }
        else { version = nil }
        return DatabaseKit.QueryRow(values: row.values,
            annotations: try fields(row.annotations), version: version)
    }
}
