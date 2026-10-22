import DatabaseKit

/// Intrinsic output names for function families whose width is independent of argument types.
enum SQLTableFunctionOutput {
    static let ordinalityName = "ordinality"

    static func names(for call: FunctionCall, withOrdinality: Bool) -> [String]? {
        guard call.schema == nil || call.schema == "pg_catalog", !call.distinct else { return nil }
        let names: [String]
        switch call.name {
        case "generate_series", "generate_subscripts", "regexp_matches", "regexp_split_to_table",
             "json_object_keys", "jsonb_object_keys": names = [call.name]
        case "json_each", "jsonb_each", "json_each_text", "jsonb_each_text": names = ["key", "value"]
        case "json_array_elements", "jsonb_array_elements", "json_array_elements_text", "jsonb_array_elements_text": names = ["value"]
        default: return nil
        }
        return withOrdinality ? names + [ordinalityName] : names
    }
}
