import DatabaseKit
import DatabaseTypes
import StorageKitSystemClock
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL BYTEA kernels")
struct CanonicalSQLBinaryEvaluatorTests {
    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 1_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work, maximumIntermediateBytes: bytes),
            monotonicClock: SystemStorageClock())
    }
    private func run(_ name: String, types: [DataType], values: [QueryValue],
        result: DataType = .blob, workMeter: DatabaseWorkMeter? = nil) throws -> DatabaseQueryScopedFieldValue {
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name,
            arguments: types.map { _ in .null })), type: .sql(result), isNullable: true,
            children: types.map { CanonicalBoundExpression(expression: .null, type: .sql($0), isNullable: true) })
        return try CanonicalSQLBinaryEvaluator.evaluate(bound, operands: values, workMeter: workMeter ?? meter())
    }
    private func bytes(_ input: [UInt8]) -> QueryValue { .primitive(.bytes(ByteString(input))) }
    private func integer(_ input: Int32) -> QueryValue { .primitive(.int32(input)) }
    private func text(_ input: String) -> QueryValue { .primitive(.string(input)) }

    @Test("Hex, escape and base64 encode/decode match retained reference including full-line newline")
    func encodings() throws {
        let cases: [(String, [UInt8], String)] = [
            ("hex", [0,255,92], "00ff5c"), ("escape", [0,255,92,65], "\\000\\377\\\\A"),
            ("base64", [0,1,2,255], "AAEC/w==")]
        for (format, input, expected) in cases {
            let encoded = try run("encode", types: [.blob,.text], values: [bytes(input),text(format)], result: .text)
            encoded.withValue { #expect($0 == .string(expected)) }
            let decoded = try run("decode", types: [.text,.text], values: [text(expected),text(format)])
            decoded.withValue { #expect($0 == .bytes(ByteString(input))) }
        }
        let spaced = try run("decode", types: [.text,.text], values: [text(" AAEC\n/w== "),text("BASE64")])
        spaced.withValue { #expect($0 == .bytes(ByteString([0,1,2,255]))) }
        let hex = try run("decode", types: [.text,.text], values: [text("00 ff"),text("HEX")])
        hex.withValue { #expect($0 == .bytes(ByteString([0,255]))) }
        let nestedMeter = meter()
        do {
            let decoded = try run("decode", types: [.text,.text], values: [text("AP9c"),text("base64")], workMeter: nestedMeter)
            try decoded.withValue { bytes in
                let encoded = try run("encode", types: [.blob,.text], values: [.primitive(copy bytes),text("hex")],
                    result: .text, workMeter: nestedMeter)
                encoded.withValue { #expect($0 == .string("00ff5c")) }
            }
        }
        #expect(nestedMeter.retainedIntermediateBytes == 0)
        for count in [56,57,58] {
            let result = try run("encode", types: [.blob,.text],
                values: [.primitive(.bytes(ByteString(repeating: 97, count: count))),text("base64")], result: .text)
            result.withValue { value in
                guard case .string(let encoded) = value else { Issue.record("Expected encoded text"); return }
                #expect(encoded.utf8.count == ((count + 2) / 3) * 4 + count / 57)
                #expect(encoded.contains("\n") == (count >= 57))
                #expect(encoded.hasSuffix("\n") == (count == 57))
            }
        }
    }

    @Test("Invalid encoding, hex pairs, escape and base64 input remain typed failures")
    func decodingFailures() throws {
        let inputs: [(String,String)] = [("abc","hex"),("0 0","hex"),("xy","hex"),
            ("AA?=","base64"),("AAA","base64"),("=AAA","base64"),("\\08x","escape"),("\\","escape")]
        for (input, format) in inputs {
            let workMeter = meter()
            #expect(throws: DatabaseExpressionEvaluationError.self) {
                try run("decode", types: [.text,.text], values: [text(input),text(format)], workMeter: workMeter)
            }
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "encode", reason: "unrecognized encoding")) {
            try run("encode", types: [.blob,.text], values: [bytes([1]),text("rot13")], result: .text)
        }
    }

    @Test("Byte/bit access uses zero offsets and low bit first; setters keep immutable source and wrap byte values")
    func byteAndBitAccess() throws {
        let original = bytes([1,128])
        for (index, expected): (Int64, Int32) in [(0,1),(7,0),(8,0),(15,1)] {
            let result = try run("get_bit", types: [.blob,.bigint],
                values: [original,.primitive(.int64(index))], result: .integer)
            result.withValue { #expect($0 == .int32(expected)) }
        }
        let get = try run("get_byte", types: [.blob,.integer], values: [original,integer(1)], result: .integer)
        get.withValue { #expect($0 == .int32(128)) }
        for (number, expected): (Int32, UInt8) in [(-1,255),(256,0),(511,255)] {
            let changed = try run("set_byte", types: [.blob,.integer,.integer], values: [original,integer(1),integer(number)])
            changed.withValue { #expect($0 == .bytes(ByteString([1,expected]))) }
            #expect(original == bytes([1,128]))
        }
        let changed = try run("set_bit", types: [.blob,.bigint,.integer], values: [bytes([0,0]),.primitive(.int64(9)),integer(1)])
        changed.withValue { #expect($0 == .bytes(ByteString([0,2]))) }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "get_byte", reason: "offset outside the input")) {
            try run("get_byte", types: [.blob,.integer], values: [original,integer(2)], result: .integer)
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "get_bit", reason: "offset outside the input")) {
            try run("get_bit", types: [.blob,.bigint], values: [original,.primitive(.int64(-1))], result: .integer)
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "set_bit", reason: "bit must be zero or one")) {
            try run("set_bit", types: [.blob,.bigint,.integer], values: [original,.primitive(.int64(0)),integer(2)])
        }
    }

    @Test("BYTEA length, subsequence and concatenation preserve exact bytes and slice owner")
    func byteSequences() throws {
        let input = bytes([0,1,2,255])
        for (name, count) in [("length",4),("octet_length",4),("bit_length",32)] {
            let result = try run(name, types: [.blob], values: [input], result: .integer)
            result.withValue { #expect($0 == .int32(Int32(count))) }
        }
        for (start, length, expected): (Int32, Int32, [UInt8]) in [(2,2,[1,2]),(-1,3,[0]),(10,1,[]),(1,0,[]),(2,.max,[1,2,255])] {
            let result = try run("substring", types: [.blob,.integer,.integer], values: [input,integer(start),integer(length)])
            result.withValue { value in
                #expect(value == .bytes(ByteString(expected)))
                if !expected.isEmpty, case .bytes(let slice) = value { #expect(slice.retainedByteCount == 4) }
            }
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "substr", reason: "negative substring length")) {
            try run("substr", types: [.blob,.integer,.integer], values: [input,integer(1),integer(-1)])
        }
        let bound = CanonicalBoundExpression(expression: .concatenate(.null,.null), type: .sql(.blob), isNullable: true,
            children: [.init(expression: .null, type: .sql(.blob), isNullable: true),
                       .init(expression: .null, type: .sql(.blob), isNullable: true)])
        let joined = try CanonicalSQLBinaryEvaluator.evaluate(bound, operands: [bytes([0]),bytes([255])], workMeter: meter())
        joined.withValue { #expect($0 == .bytes(ByteString([0,255]))) }
    }

    @Test("Strict NULL propagates after selected carrier validation")
    func nullAndSignature() throws {
        let result = try run("encode", types: [.blob,.text], values: [.primitive(.null),text("invalid")], result: .text)
        result.withValue { #expect($0 == .null) }
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "binary selected input")) {
            try run("get_byte", types: [.blob,.integer], values: [text("wrong carrier"),integer(0)], result: .integer)
        }
    }

    @Test("Binary output keeps its reservation, rejects one byte short and releases all failed state")
    func retainedResources() throws {
        let input = QueryValue.primitive(.bytes(ByteString(repeating: 255, count: 1024)))
        let probe = meter()
        do {
            let result = try run("encode", types: [.blob,.text], values: [input,text("hex")], result: .text, workMeter: probe)
            #expect(probe.retainedIntermediateBytes > 0)
            result.withValue { value in
                guard case .string(let text) = value else { Issue.record("Expected hex output"); return }
                #expect(text.utf8.count == 2048)
            }
        }
        #expect(probe.retainedIntermediateBytes == 0)
        let exact = meter(bytes: probe.peakIntermediateBytes)
        _ = try run("encode", types: [.blob,.text], values: [input,text("hex")], result: .text, workMeter: exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let short = meter(bytes: probe.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) {
            try run("encode", types: [.blob,.text], values: [input,text("hex")], result: .text, workMeter: short)
        }
        #expect(short.retainedIntermediateBytes == 0)
        let noWork = meter(work: 0)
        #expect(throws: DatabaseWorkLimitError.self) {
            try run("set_byte", types: [.blob,.integer,.integer], values: [input,integer(0),integer(1)], workMeter: noWork)
        }
        #expect(noWork.retainedIntermediateBytes == 0)
    }
}
