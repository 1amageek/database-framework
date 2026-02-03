/// FindCommands - Fetch with optional in-memory filter/sort using dynamic codec

import Foundation
import DatabaseEngine

public struct FindCommands {

    private let dataAccess: CatalogDataAccess
    private let output: OutputFormatter

    public init(dataAccess: CatalogDataAccess, output: OutputFormatter) {
        self.dataAccess = dataAccess
        self.output = output
    }

    /// Usage: find <TypeName> [--where field op value] [--sort field [desc]] [--limit N] [--partition field=value ...]
    public func execute(_ args: [String]) async throws {
        guard !args.isEmpty else {
            throw CLIError.invalidArguments("Usage: find <TypeName> [--where field op value] [--sort field [desc]] [--limit N] [--partition field=value ...]")
        }

        let typeName = args[0]
        let partitionValues = DataCommands.parsePartitionValues(from: args)
        let options = try parseOptions(Array(args.dropFirst()))

        // Fetch: if no filter, apply limit at scan level; otherwise fetch all and filter
        let fetchLimit = options.whereClause == nil ? options.limit : nil
        var records = try await dataAccess.findAll(typeName: typeName, limit: fetchLimit, partitionValues: partitionValues)

        // In-memory filter
        if let whereClause = options.whereClause {
            records = records.filter { dict in
                guard let fieldValue = dict[whereClause.field] else { return false }
                return compareValue(fieldValue, whereClause.op, whereClause.value)
            }
        }

        // In-memory sort
        if let sortField = options.sortField {
            let descending = options.sortDescending
            records.sort { a, b in
                compareForSort(a[sortField], b[sortField], descending: descending)
            }
        }

        // Apply limit after filter
        if let limit = options.limit, options.whereClause != nil {
            records = Array(records.prefix(limit))
        }

        // Output
        if records.isEmpty {
            output.info("(no results)")
        } else {
            output.info("Found \(records.count) record(s):")
            for dict in records {
                let jsonString = try JSONParser.stringify(dict)
                output.line(jsonString)
            }
        }
    }

    // MARK: - Option Parsing

    private struct QueryOptions {
        var whereClause: WhereClause?
        var sortField: String?
        var sortDescending: Bool = false
        var limit: Int?
    }

    private struct WhereClause {
        let field: String
        let op: String
        let value: String
    }

    private func parseOptions(_ args: [String]) throws -> QueryOptions {
        var options = QueryOptions()
        var i = 0

        while i < args.count {
            let arg = args[i].lowercased()
            switch arg {
            case "--where", "where":
                guard i + 3 < args.count else {
                    throw CLIError.invalidArguments("--where requires: field op value")
                }
                let field = args[i + 1]
                let op = args[i + 2]
                var value = args[i + 3]
                if value.hasPrefix("\"") && value.hasSuffix("\"") && value.count >= 2 {
                    value = String(value.dropFirst().dropLast())
                }
                options.whereClause = WhereClause(field: field, op: op, value: value)
                i += 4

            case "--sort":
                guard i + 1 < args.count else {
                    throw CLIError.invalidArguments("--sort requires a field name")
                }
                options.sortField = args[i + 1]
                i += 2
                if i < args.count && args[i].lowercased() == "desc" {
                    options.sortDescending = true
                    i += 1
                }

            case "--limit":
                guard i + 1 < args.count, let n = Int(args[i + 1]) else {
                    throw CLIError.invalidArguments("--limit requires a number")
                }
                options.limit = n
                i += 2

            default:
                i += 1
            }
        }

        return options
    }

    // MARK: - Comparison

    private func compareValue(_ fieldValue: Any, _ op: String, _ targetValue: String) -> Bool {
        // String comparison
        if let strValue = fieldValue as? String {
            switch op {
            case "==", "=": return strValue == targetValue
            case "!=": return strValue != targetValue
            case ">": return strValue > targetValue
            case "<": return strValue < targetValue
            case ">=": return strValue >= targetValue
            case "<=": return strValue <= targetValue
            default: return false
            }
        }

        // Numeric comparison
        if let numTarget = Double(targetValue) {
            let numField: Double?
            if let d = fieldValue as? Double { numField = d }
            else if let i = fieldValue as? Int { numField = Double(i) }
            else if let i = fieldValue as? Int64 { numField = Double(i) }
            else if let u = fieldValue as? UInt64 { numField = Double(u) }
            else if let f = fieldValue as? Float { numField = Double(f) }
            else { numField = nil }

            if let numField {
                switch op {
                case "==", "=": return numField == numTarget
                case "!=": return numField != numTarget
                case ">": return numField > numTarget
                case "<": return numField < numTarget
                case ">=": return numField >= numTarget
                case "<=": return numField <= numTarget
                default: return false
                }
            }
        }

        // Bool comparison
        if let boolValue = fieldValue as? Bool {
            let targetBool = targetValue.lowercased() == "true"
            switch op {
            case "==", "=": return boolValue == targetBool
            case "!=": return boolValue != targetBool
            default: return false
            }
        }

        return false
    }

    private func compareForSort(_ a: Any?, _ b: Any?, descending: Bool) -> Bool {
        guard let a else { return !descending }
        guard let b else { return descending }

        let result: Bool
        if let sa = a as? String, let sb = b as? String {
            result = sa < sb
        } else if let da = toDouble(a), let db = toDouble(b) {
            result = da < db
        } else {
            result = String(describing: a) < String(describing: b)
        }

        return descending ? !result : result
    }

    private func toDouble(_ value: Any) -> Double? {
        if let d = value as? Double { return d }
        if let i = value as? Int { return Double(i) }
        if let i = value as? Int64 { return Double(i) }
        if let u = value as? UInt64 { return Double(u) }
        if let f = value as? Float { return Double(f) }
        return nil
    }
}

// MARK: - Help

extension FindCommands {
    static var helpText: String {
        """
        Find Commands:
          find <TypeName>                              List all records
          find <TypeName> --limit N                    List with limit
          find <TypeName> --where field op value       Filter records
          find <TypeName> --sort field [desc]          Sort records
          find <TypeName> --where age > 30 --sort name --limit 10

        Options:
          --partition field=value      Specify partition value for dynamic directory types

        Operators: ==, !=, >, <, >=, <=

        Examples:
          find User --limit 5
          find User --where name == "Alice"
          find User --where age > 30 --sort age desc --limit 10
          find Order --limit 10 --partition tenantId=tenant_123
        """
    }
}
