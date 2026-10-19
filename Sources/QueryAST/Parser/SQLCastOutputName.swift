import DatabaseKit

/// Names the output column of an unaliased projection item that is a CAST.
///
/// PostgreSQL names such a column after the internal name of its target type.
/// The parser applies this name when it folds a constant cast to a literal,
/// and the type binder applies it to casts that remain structural, so both
/// paths share one mapping.
public enum SQLCastOutputName {
    /// PostgreSQL's internal type name as written by a CAST target.
    public static func name(for type: DataType) -> String {
        switch type {
        case .boolean: "bool"
        case .smallint: "int2"
        case .integer: "int4"
        case .bigint: "int8"
        case .real: "float4"
        case .doublePrecision: "float8"
        case .decimal: "numeric"
        case .char: "bpchar"
        case .varchar: "varchar"
        case .text: "text"
        case .date: "date"
        case .time(let zone, _): zone ? "timetz" : "time"
        case .timestamp(let zone, _): zone ? "timestamptz" : "timestamp"
        case .interval: "interval"
        case .binary, .varbinary, .blob: "bytea"
        case .json: "json"
        case .jsonb: "jsonb"
        case .uuid: "uuid"
        case .array(let element): name(for: element)
        case .custom(let name, _): name
        case .composite: "record"
        }
    }
}
