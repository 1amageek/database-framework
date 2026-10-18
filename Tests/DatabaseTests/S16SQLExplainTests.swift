import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL EXPLAIN rendering", .timeLimit(.minutes(1)))
struct S16SQLExplainTests {
    @Test("TEXT and JSON preserve the same selected operator hierarchy")
    func formatsShareSelectedTree() throws {
        let plan = SQLExplainPlanNode(operation: "Aggregate", children: [
            SQLExplainPlanNode(operation: "Hash Join", children: [
                SQLExplainPlanNode(operation: "Index Scan", details: [.init(name: "Index", value: "by_id")]),
                SQLExplainPlanNode(operation: "Table Scan")])])
        let text = try rendered(plan, options: .init())
        let json = try rendered(plan, options: .init(format: .json))
        #expect(text.contains("Aggregate\n  Hash Join\n    Index Scan\n      Index: by_id\n    Table Scan"))
        let result = try #require(JSONSerialization.jsonObject(with: Data(json.utf8)) as? [[String: Any]])
        let root = try #require(result.first?["Plan"] as? [String: Any])
        #expect(root["Node Type"] as? String == "Aggregate")
        let children = try #require(root["Plans"] as? [[String: Any]])
        #expect(children.first?["Node Type"] as? String == "Hash Join")
        let inputs = try #require(children.first?["Plans"] as? [[String: Any]])
        #expect(inputs.compactMap { $0["Node Type"] as? String } == ["Index Scan", "Table Scan"])
        #expect(!json.contains("Cost"))
        #expect(!json.contains("Buffers"))
    }

    @Test("VERBOSE exposes bound output descriptors and private source details")
    func verboseControlsDetails() throws {
        let column = try QueryColumn(number: 1, name: "projected", type: .primitive(.string), isNullable: true)
        let plan = SQLExplainPlanNode(operation: "Projection", details: [
            .init(name: "Source", value: "bound_alias", verboseOnly: true)], outputs: [column])
        let basic = try rendered(plan, options: .init())
        #expect(!basic.contains("projected"))
        let verbose = try rendered(plan, options: .init(verbose: true))
        #expect(verbose.contains("Source: bound_alias"))
        #expect(verbose.contains("Output: 1 projected (nullable=true)"))
        let json = try rendered(plan, options: .init(verbose: true, format: .json))
        let result = try #require(JSONSerialization.jsonObject(with: Data(json.utf8)) as? [[String: Any]])
        let root = try #require(result.first?["Plan"] as? [String: Any])
        let outputs = try #require(root["Output"] as? [[String: Any]])
        #expect(outputs.first?["Name"] as? String == "projected")
    }

    @Test("JSON quotes control characters and preserves Unicode source names")
    func jsonEscaping() throws {
        let name = "table\"\\\n\u{0000}日本語"
        let plan = SQLExplainPlanNode(operation: name, details: [.init(name: "Relation", value: name)])
        let json = try rendered(plan, options: .init(format: .json))
        let result = try #require(JSONSerialization.jsonObject(with: Data(json.utf8)) as? [[String: Any]])
        let root = try #require(result.first?["Plan"] as? [String: Any])
        #expect(root["Node Type"] as? String == name)
        let details = try #require(root["Details"] as? [[String: Any]])
        #expect(details.first?["Value"] as? String == name)
    }

    @Test("ANALYZE measurements describe statement observations only")
    func measuredValues() throws {
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())
        let output = try SQLExplainRenderer.render(.init(operation: "Insert"),
            options: .init(analyze: true, format: .json),
            measurement: .init(rows: 3, workUnits: 17, elapsed: .milliseconds(125)), meter: meter)
        #expect(output.rows.columns.first?.type == .sql(.json))
        let json = try string(output)
        let result = try #require(JSONSerialization.jsonObject(with: Data(json.utf8)) as? [[String: Any]])
        #expect(result.first?["Actual Rows"] as? Int == 3)
        #expect(result.first?["Work Units"] as? Int == 17)
        #expect(result.first?["Execution Time ms"] as? Double == 125)
        #expect(meter.consumedRows == 1)
        #expect(meter.retainedIntermediateBytes > UInt64(json.utf8.count))
    }

    @Test("Rendering rejects exhausted work and byte budgets")
    func resourceFailures() throws {
        let work = DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 0),
            monotonicClock: TestProcessMonotonicClock())
        #expect(throws: DatabaseWorkLimitError.self) {
            try SQLExplainRenderer.render(.init(operation: "Table Scan"), options: .init(), measurement: nil, meter: work)
        }
        let bytes = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: 1),
            monotonicClock: TestProcessMonotonicClock())
        #expect(throws: DatabaseWorkLimitError.self) {
            try SQLExplainRenderer.render(.init(operation: "Table Scan"), options: .init(), measurement: nil, meter: bytes)
        }
    }

    @Test("Cancellation rejects rendering before producing rows")
    func cancellation() async throws {
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())
            return try SQLExplainRenderer.render(.init(operation: "Table Scan"), options: .init(), measurement: nil, meter: meter)
        }
        await #expect(throws: CancellationError.self) { try await task.value }
    }

    private func rendered(_ plan: SQLExplainPlanNode, options: SQLExplainOptions) throws -> String {
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: TestProcessMonotonicClock())
        return try string(SQLExplainRenderer.render(plan, options: options, measurement: nil, meter: meter))
    }

    private func string(_ rows: SQLBlockCollectedRows) throws -> String {
        var output = ""
        for index in rows.visibleRange {
            if index > rows.visibleRange.lowerBound { output.append("\n") }
            try rows.rows.withElement(at: index) { row in
                switch row.values[0] {
                case .primitive(.string(let value)): output.append(value)
                case .json(let value): output.append(String(decoding: value.bytes, as: UTF8.self))
                default: throw ExplainTestFailure.expectedString
                }
            }
        }
        return output
    }
}

private enum ExplainTestFailure: Error { case expectedString }
