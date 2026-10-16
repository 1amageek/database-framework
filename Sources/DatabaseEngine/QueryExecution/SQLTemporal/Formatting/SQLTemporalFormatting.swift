import DatabaseKit
import DatabaseTypes

/// Selected temporal template calls and the temporal scalar-text boundary.
enum SQLTemporalFormatting {
    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard case .function(let call) = bound.expression, operands.count == 2,
              bound.children.count == 2 else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal formatting")
        }
        let selected: Bool
        switch (call.name, bound.children[0].type, bound.children[1].type, bound.type) {
        case ("to_char", .sql(.timestamp), .sql(.text), .sql(.text)),
             ("to_char", .sql(.interval), .sql(.text), .sql(.text)),
             ("to_date", .sql(.text), .sql(.text), .sql(.date)),
             ("to_timestamp", .sql(.text), .sql(.text), .sql(.timestamp(true, _))): selected = true
        default: selected = false
        }
        guard selected else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal formatting signature")
        }
        for index in operands.indices {
            guard QueryValueValidator.accepts(operands[index], type: bound.children[index].type) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected temporal formatting input")
            }
        }
        try workMeter.consume(at: .expressionEvaluation)
        if operands.contains(where: { $0.isNull }) { return (.primitive(.null), nil) }
        guard case .primitive(.string(let template)) = operands[1] else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal template")
        }
        switch call.name {
        case "to_char":
            if infinity(operands[0]) != nil || template.isEmpty { return (.primitive(.null), nil) }
            let parts: SQLTemporalFormat.Parts
            if case .interval(let value) = operands[0] { parts = SQLTemporalFormat.Parts(value) }
            else {
                parts = SQLTemporalFormat.Parts(try CanonicalSQLTemporalEvaluator.calendarFields(of: operands[0],
                    type: bound.children[0].type, environment: environment, workMeter: workMeter))
            }
            let designation = try designation(operands[0], environment: environment, workMeter: workMeter)
            let count = try measured(workMeter: workMeter) {
                try SQLTemporalFormat.render(template[...], parts: parts, designation: designation,
                    workMeter: workMeter, emit: $0)
            }
            try QueryValueFootprint(nodeCount: 1, collectionElementCount: 0,
                retainedByteCount: count, maximumNestingDepth: 1).validate(limits: limits)
            let output = try CanonicalSQLScalarText.produceText(count: count, workMeter: workMeter) {
                try materialize(count: count, workMeter: workMeter) {
                    try SQLTemporalFormat.render(template[...], parts: parts, designation: designation,
                        workMeter: workMeter, emit: $0)
                }
            }
            return (output.withValue { .primitive(copy $0) },
                output.retainedReservation.map { DatabaseQueryPayloadLease(retaining: $0) })
        case "to_date", "to_timestamp":
            guard case .primitive(.string(let input)) = operands[0] else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal template input")
            }
            let parsed = try SQLTemporalFormatParser.parse(input[...], template: template[...],
                environment: environment, workMeter: workMeter)
            if call.name == "to_date" { return (try CanonicalSQLTemporalEvaluator.date(parsed.fields), nil) }
            return (try CanonicalSQLTemporalEvaluator.assemble(parsed.fields, withTimeZone: true,
                explicitOffsetSeconds: parsed.offsetSeconds, precision: parsed.precision,
                environment: environment, workMeter: workMeter), nil)
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(call.name)
        }
    }

    static func withText<Result>(of value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter,
        _ body: (borrowing Substring) throws -> Result) throws -> Result {
        guard !value.isNull, QueryValueValidator.accepts(value, type: type) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal scalar text")
        }
        if let sign = infinity(value) {
            try workMeter.consume(at: .expressionEvaluation)
            return try body(sign == .negative ? "-infinity"[...] : "infinity"[...])
        }
        let parts: SQLTemporalFormat.Parts
        var offset: Int32? = nil
        var date = false, time = false
        switch value {
        case .interval(let interval): parts = SQLTemporalFormat.Parts(interval)
        case .time(let wall): parts = timeParts(wall.microsecondsSinceMidnight); time = true
        case .timeWithTimeZone(let wall):
            parts = timeParts(wall.time.microsecondsSinceMidnight); time = true; offset = wall.utcOffsetSeconds
        case .primitive(.time(let wall)):
            parts = timeParts(Int64(wall.hour) * 3_600_000_000 + Int64(wall.minute) * 60_000_000 +
                Int64(wall.second) * 1_000_000 + Int64(wall.nanoseconds / 1000)); time = true
        case .primitive(.date):
            parts = SQLTemporalFormat.Parts(try CanonicalSQLTemporalEvaluator.calendarFields(of: value,
                type: type, environment: environment, workMeter: workMeter)); date = true
        case .primitive(.dateTime), .primitive(.timestamp):
            parts = SQLTemporalFormat.Parts(try CanonicalSQLTemporalEvaluator.calendarFields(of: value,
                type: type, environment: environment, workMeter: workMeter)); date = true; time = true
            offset = try designation(value, environment: environment, workMeter: workMeter)?.offsetSeconds
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "temporal scalar text")
        }
        let count = try measured(workMeter: workMeter) {
            try plain(parts, date: date, time: time, offset: offset, emit: $0)
        }
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: count)
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        defer { claim.release() }
        let text = try materialize(count: count, workMeter: workMeter) {
            try plain(parts, date: date, time: time, offset: offset, emit: $0)
        }
        return try body(text[...])
    }

    static func infinity(_ value: QueryValue) -> QueryTemporalInfinity? {
        switch value {
        case .dateInfinity(let sign), .intervalInfinity(let sign), .timestampInfinity(let sign, _): sign
        default: nil
        }
    }

    private static func timeParts(_ microseconds: Int64) -> SQLTemporalFormat.Parts {
        SQLTemporalFormat.Parts(SQLGregorianCalendar.Fields(year: 0, month: 1, day: 1,
            hour: Int32(microseconds / 3_600_000_000), minute: Int32(microseconds / 60_000_000 % 60),
            second: Int32(microseconds / 1_000_000 % 60), microsecond: Int32(microseconds % 1_000_000)))
    }

    private static func designation(_ value: QueryValue, environment: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules.Designation? {
        if case .primitive(.timestamp(let instant)) = value {
            return try CanonicalSQLTemporalEvaluator.designation(at: instant,
                environment: environment, workMeter: workMeter)
        }
        return nil
    }

    private static func measured(workMeter: DatabaseWorkMeter,
        _ render: (_ emit: (UInt8) throws -> Void) throws -> Void) throws -> UInt64 {
        var count: UInt64 = 0
        try render { _ in
            if count & 255 == 0 { try workMeter.consume(at: .expressionEvaluation) }
            count = try CanonicalSQLScalarText.checkedCount(count, addition: 1)
        }
        return count
    }

    private static func materialize(count: UInt64, workMeter: DatabaseWorkMeter,
        _ render: (_ emit: (UInt8) throws -> Void) throws -> Void) throws -> String {
        try CanonicalSQLScalarText.makeText(count: count, workMeter: workMeter) { buffer in
            var position = 0
            // The final String owns the initialized storage; its synchronous borrow never escapes.
            try render { byte in
                try CanonicalSQLScalarText.checkpoint(position, workMeter)
                guard position < buffer.count else { throw SQLTemporalFormat.rangeFailure }
                buffer[position] = byte; position += 1
            }
            guard position == buffer.count else { throw SQLTemporalFormat.rangeFailure }
        }
    }

    private static func plain(_ parts: SQLTemporalFormat.Parts, date: Bool, time: Bool,
        offset: Int32?, emit: (UInt8) throws -> Void) throws {
        func text(_ value: String) throws { for byte in value.utf8 { try emit(byte) } }
        func number(_ value: Int64, _ width: Int = 0) throws { try SQLTemporalFormat.integer(value, width: width, emit: emit) }
        func fraction(_ value: Int64) throws {
            if value == 0 { return }
            try emit(46)
            var trailing: Int64 = 1, digits = 6
            while value % (trailing * 10) == 0 { trailing *= 10; digits -= 1 }
            try number(value / trailing, digits)
        }
        if parts.interval {
            var zero = true, before = false
            func component(_ value: Int64, unit: String) throws {
                if value == 0 { return }
                if !zero { try emit(32) }
                if before && value > 0 { try emit(43) }
                try number(value); try emit(32); try text(unit)
                if value != 1 { try emit(115) }
                before = value < 0; zero = false
            }
            try component(parts.year, unit: "year")
            try component(parts.month, unit: "mon")
            try component(parts.day, unit: "day")
            if zero || parts.hour != 0 || parts.minute != 0 || parts.second != 0 || parts.microsecond != 0 {
                if !zero { try emit(32) }
                let negative = parts.hour < 0 || parts.minute < 0 || parts.second < 0 || parts.microsecond < 0
                if negative { try emit(45) } else if before { try emit(43) }
                try number(Int64(parts.hour.magnitude), 2); try emit(58)
                try number(Int64(parts.minute.magnitude), 2); try emit(58)
                try number(Int64(parts.second.magnitude), 2); try fraction(Int64(parts.microsecond.magnitude))
            }
            return
        }
        if date {
            try number(parts.year > 0 ? parts.year : 1 - parts.year, 4); try emit(45)
            try number(parts.month, 2); try emit(45); try number(parts.day, 2)
        }
        if time {
            if date { try emit(32) }
            try number(parts.hour, 2); try emit(58); try number(parts.minute, 2); try emit(58)
            try number(parts.second, 2); try fraction(parts.microsecond)
        }
        if let offset {
            let magnitude = Int64(offset).magnitude
            try emit(offset < 0 ? 45 : 43); try number(Int64(magnitude / 3600), 2)
            if magnitude % 3600 != 0 {
                try emit(58); try number(Int64(magnitude % 3600 / 60), 2)
                if magnitude % 60 != 0 { try emit(58); try number(Int64(magnitude % 60), 2) }
            }
        }
        if date && parts.year <= 0 { try text(" BC") }
    }
}
