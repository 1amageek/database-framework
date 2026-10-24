import DatabaseKit
import DatabaseTypes

/// Renders one selected tree; it never chooses operators or executes expressions.
enum SQLExplainRenderer {
    static func render(_ plan: SQLExplainPlanNode, options: SQLExplainOptions,
                       measurement: SQLExplainMeasurement?, meter: DatabaseWorkMeter) throws -> SQLBlockCollectedRows {
        var buffer = try SQLExplainTextBuffer(meter: meter)
        switch options.format {
        case .text:
            try text(plan, depth: 0, verbose: options.verbose, into: &buffer)
            if let measurement {
                try buffer.append("Actual Rows: \(measurement.rows)\nWork Units: \(measurement.workUnits)\nExecution Time (ms): \(milliseconds(measurement.elapsed))\n")
            }
        case .json:
            try buffer.append("[{\"Plan\":")
            try json(plan, verbose: options.verbose, into: &buffer)
            if let measurement {
                try buffer.append(",\"Actual Rows\":\(measurement.rows),\"Work Units\":\(measurement.workUnits),\"Execution Time ms\":\(milliseconds(measurement.elapsed))")
            }
            try buffer.append("}]")
        }
        switch options.format {
        case .text: return try textRows(buffer, meter: meter)
        case .json: return try jsonRow(buffer, meter: meter)
        }
    }

    private static func textRows(_ buffer: borrowing SQLExplainTextBuffer,
                                 meter: DatabaseWorkMeter) throws -> SQLBlockCollectedRows {
        let columns = [try QueryColumn(number: 1, name: "QUERY PLAN", type: .primitive(.string), isNullable: false)]
        var rows = try DatabaseRetainedQueryRowsBuilder(columns: columns, workMeter: meter,
            stage: .resultMaterialization)
        var start = buffer.text.startIndex
        while start < buffer.text.endIndex {
            let end = buffer.text[start...].firstIndex(of: "\n") ?? buffer.text.endIndex
            let slice = buffer.text[start..<end]
            let footprint = try CanonicalRelationalFootprintMeter.queryRowBaseFootprint()
                .adding(DatabaseIntermediateFootprint(bytes: UInt64(slice.utf8.count)).multiplied(by: 8))
            // Each caller-visible TEXT row requires its own canonical String owner.
            try rows.append(footprint: footprint) {
                try ExecutionQueryRow(values: [.primitive(.string(String(slice)))], columns: columns)
            }
            try meter.recordOutputRows()
            start = end == buffer.text.endIndex ? end : buffer.text.index(after: end)
        }
        return SQLBlockCollectedRows(rows: try rows.finish().moveToSharedOwnership(at: .resultMaterialization),
            affectedRows: nil)
    }

    private static func jsonRow(_ buffer: borrowing SQLExplainTextBuffer,
                                meter: DatabaseWorkMeter) throws -> SQLBlockCollectedRows {
        let columns = [try QueryColumn(number: 1, name: "QUERY PLAN", type: .sql(.json), isNullable: false)]
        // The typed JSON boundary requires immutable UTF-8 bytes; the renderer's String is copied once.
        let byteCount = UInt64(buffer.text.utf8.count)
        try DatabaseByteProcessingMeter.consume(byteCount: byteCount, passes: 2, workMeter: meter,
            stage: .resultMaterialization)
        var rows = try DatabaseRetainedQueryRowsBuilder(columns: columns, workMeter: meter,
            stage: .resultMaterialization, expectedCount: 1)
        let footprint = try CanonicalRelationalFootprintMeter.queryRowBaseFootprint()
            .adding(DatabaseIntermediateFootprint(bytes: byteCount))
            .adding(DatabaseIntermediateFootprint(bytes: 128))
        try rows.append(footprint: footprint) {
            let json = try QueryJSONValue(bytes: ByteString(utf8: buffer.text), limits: CanonicalBindingStorage.limits(meter))
            return try ExecutionQueryRow(values: [.json(json)], columns: columns)
        }
        try meter.recordOutputRows()
        return SQLBlockCollectedRows(rows: try rows.finish().moveToSharedOwnership(at: .resultMaterialization),
            affectedRows: nil)
    }

    private static func text(_ plan: SQLExplainPlanNode, depth: Int, verbose: Bool,
                             into buffer: inout SQLExplainTextBuffer) throws {
        try buffer.meter.consume(at: .resultMaterialization)
        for _ in 0..<depth { try buffer.append("  ") }
        try buffer.append(plan.operation)
        try buffer.append("\n")
        for detail in plan.details where verbose || !detail.verboseOnly {
            for _ in 0...depth { try buffer.append("  ") }
            try buffer.append(detail.name)
            try buffer.append(": ")
            try buffer.append(detail.value)
            try buffer.append("\n")
        }
        if verbose {
            for output in plan.outputs {
                for _ in 0...depth { try buffer.append("  ") }
                try buffer.append("Output: \(output.number) ")
                try buffer.append(output.name)
                try buffer.append(" (nullable=\(output.isNullable))\n")
            }
        }
        for child in plan.children { try text(child, depth: depth + 1, verbose: verbose, into: &buffer) }
    }

    private static func json(_ plan: SQLExplainPlanNode, verbose: Bool,
                             into buffer: inout SQLExplainTextBuffer) throws {
        try buffer.meter.consume(at: .resultMaterialization)
        try buffer.append("{\"Node Type\":")
        try buffer.quoted(plan.operation)
        // Detail objects prevent a source name from colliding with structural keys.
        try buffer.append(",\"Details\":[")
        var first = true
        for detail in plan.details where verbose || !detail.verboseOnly {
            if !first { try buffer.append(",") }; first = false
            try buffer.append("{\"Name\":"); try buffer.quoted(detail.name)
            try buffer.append(",\"Value\":"); try buffer.quoted(detail.value)
            try buffer.append("}")
        }
        try buffer.append("]")
        if verbose {
            try buffer.append(",\"Output\":[")
            for (index, output) in plan.outputs.enumerated() {
                if index > 0 { try buffer.append(",") }
                try buffer.append("{\"Number\":\(output.number),\"Name\":")
                try buffer.quoted(output.name)
                try buffer.append(",\"Nullable\":\(output.isNullable)}")
            }
            try buffer.append("]")
        }
        try buffer.append(",\"Plans\":[")
        for (index, child) in plan.children.enumerated() {
            if index > 0 { try buffer.append(",") }
            try json(child, verbose: verbose, into: &buffer)
        }
        try buffer.append("]}")
    }

    private static func milliseconds(_ duration: Duration) -> Double {
        let components = duration.components
        return Double(components.seconds) * 1_000 + Double(components.attoseconds) / 1_000_000_000_000_000
    }
}
