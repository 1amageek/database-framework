import DatabaseKit
import QueryAST
import DatabaseTypes

/// Reversible SQL invocation policy; prepared names deliberately live elsewhere.
struct SQLBlockSettings: Sendable {
    var searchPath: SQLSearchPath
    var temporal: SQLTemporalConfiguration
    var timeZoneName: String
    var statementTimeoutMilliseconds: UInt64 = 0
    private var searchPathClaim: DatabaseIntermediateReservation?
    private var timeZoneClaim: DatabaseIntermediateReservation?

    init(searchPath: SQLSearchPath, temporal: SQLTemporalConfiguration) {
        self.searchPath = searchPath
        self.temporal = temporal
        switch temporal.timeZone {
        case .utc: timeZoneName = "UTC"
        case .fixed(let seconds): timeZoneName = String(seconds)
        case .named(let name, _): timeZoneName = name
        }
    }

    mutating func set(_ name: SQLSettingName, value: QueryValue,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) throws {
        switch name {
        case .searchPath:
            switch value {
            case .primitive(.string(let text)):
                let parsed = try Self.parseSearchPath(text, workMeter: workMeter)
                searchPath = parsed.path
                searchPathClaim = parsed.claim
            case .array(let array):
                var payload = DatabaseIntermediateFootprint()
                for value in array.elements {
                    guard case .primitive(.string(let name)) = value else {
                        throw SQLBlockExecutionError.invalidSettingValue(.searchPath)
                    }
                    payload = try payload.adding(DatabaseIntermediateFootprint(bytes: UInt64(name.utf8.count)))
                }
                var entries = try SQLArrayBuffer<SQLSearchPathEntry>(count: array.elements.count,
                    extraBytes: payload.bytes, meter: workMeter)
                for value in array.elements {
                    guard case .primitive(.string(let name)) = value else {
                        throw SQLBlockExecutionError.invalidSettingValue(.searchPath)
                    }
                    try entries.append(name == "$user" ? .currentUser : .schema(name))
                }
                let finished = entries.finish()
                searchPath = try SQLSearchPath(entries: finished.values)
                searchPathClaim = finished.reservation
            default: throw SQLBlockExecutionError.invalidSettingValue(name)
            }
        case .timeZone:
            guard case .primitive(.string(let text)) = value else {
                throw SQLBlockExecutionError.invalidSettingValue(name)
            }
            let claim = try workMeter.reserveIntermediate(bytes: UInt64(text.utf8.count), at: .bindingCandidate)
            let zone = try CanonicalSQLTemporalEvaluator.zone(text[...], posix: false,
                environment: environment, workMeter: workMeter)
            temporal = temporal.replacing(timeZone: zone)
            timeZoneName = text
            timeZoneClaim = claim
        case .statementTimeout:
            switch value {
            case .primitive(.int64(let value)) where value >= 0:
                statementTimeoutMilliseconds = UInt64(value)
            case .primitive(.string(let text)):
                statementTimeoutMilliseconds = try Self.parseTimeout(text)
            default: throw SQLBlockExecutionError.invalidSettingValue(name)
            }
        }
    }

    mutating func reset(_ name: SQLSettingName?, defaults: Self) {
        guard let name else { self = defaults; return }
        switch name {
        case .searchPath: searchPath = defaults.searchPath; searchPathClaim = defaults.searchPathClaim
        case .timeZone: temporal = defaults.temporal; timeZoneName = defaults.timeZoneName; timeZoneClaim = defaults.timeZoneClaim
        case .statementTimeout: statementTimeoutMilliseconds = defaults.statementTimeoutMilliseconds
        }
    }

    func text(_ name: SQLSettingName) -> String {
        switch name {
        case .searchPath:
            return searchPath.entries.map { entry in
                switch entry {
                case .currentUser: return "\"$user\""
                case .schema(let name): return SQLParser.displayIdentifier(name)
                }
            }.joined(separator: ", ")
        case .timeZone: return timeZoneName
        case .statementTimeout: return String(statementTimeoutMilliseconds)
        }
    }

    static let names: [SQLSettingName] = [.searchPath, .timeZone, .statementTimeout]

    static func label(_ name: SQLSettingName) -> String {
        switch name {
        case .searchPath: return "search_path"
        case .timeZone: return "TimeZone"
        case .statementTimeout: return "statement_timeout"
        }
    }

    private static func parseTimeout(_ input: String) throws -> UInt64 {
        let text = String(SQLTemporalInput.trimmed(input[...])).lowercased()
        let units: [(String, UInt64)] = [("min", 60_000), ("ms", 1), ("s", 1_000), ("h", 3_600_000), ("d", 86_400_000)]
        for (suffix, multiplier) in units where text.hasSuffix(suffix) {
            let number = String(SQLTemporalInput.trimmed(text.dropLast(suffix.count)))
            guard let value = UInt64(number) else { throw SQLBlockExecutionError.invalidSettingValue(.statementTimeout) }
            let result = value.multipliedReportingOverflow(by: multiplier)
            guard !result.overflow, result.partialValue <= UInt64(Int64.max) else {
                throw SQLBlockExecutionError.invalidSettingValue(.statementTimeout)
            }
            return result.partialValue
        }
        guard let value = UInt64(text), value <= UInt64(Int64.max) else {
            throw SQLBlockExecutionError.invalidSettingValue(.statementTimeout)
        }
        return value
    }

    private static func parseSearchPath(_ input: String, workMeter: DatabaseWorkMeter) throws
        -> (path: SQLSearchPath, claim: DatabaseIntermediateReservation) {
        var entries = try SQLArrayBuffer<SQLSearchPathEntry>(extraBytes: UInt64(input.utf8.count), meter: workMeter)
        var remaining = input[...]
        while true {
            remaining = SQLTemporalInput.trimmed(remaining)
            guard !remaining.isEmpty else {
                if entries.values.isEmpty {
                    let finished = entries.finish()
                    return (try SQLSearchPath(entries: finished.values), finished.reservation)
                }
                throw SQLBlockExecutionError.invalidSettingValue(.searchPath)
            }
            var name = ""
            let quoted = remaining.first == "\""
            if quoted {
                remaining = remaining.dropFirst()
                var closed = false
                while let character = remaining.first {
                    remaining = remaining.dropFirst()
                    if character == "\"" {
                        if remaining.first == "\"" { name.append(character); remaining = remaining.dropFirst() }
                        else { closed = true; break }
                    } else { name.append(character) }
                }
                guard closed else { throw SQLBlockExecutionError.invalidSettingValue(.searchPath) }
                remaining = SQLTemporalInput.trimmed(remaining)
                guard remaining.isEmpty || remaining.first == "," else {
                    throw SQLBlockExecutionError.invalidSettingValue(.searchPath)
                }
            } else {
                let end = remaining.firstIndex(of: ",") ?? remaining.endIndex
                let value = SQLTemporalInput.trimmed(remaining[..<end])
                guard !value.contains(where: { $0.isWhitespace || $0 == "\"" }) else {
                    throw SQLBlockExecutionError.invalidSettingValue(.searchPath)
                }
                name = value.lowercased()
                remaining = remaining[end...]
            }
            guard !name.isEmpty else { throw SQLBlockExecutionError.invalidSettingValue(.searchPath) }
            try entries.append(name == "$user" ? .currentUser : .schema(name))
            if remaining.isEmpty {
                let finished = entries.finish()
                return (try SQLSearchPath(entries: finished.values), finished.reservation)
            }
            remaining = remaining.dropFirst()
        }
    }
}
