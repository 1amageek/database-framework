import DatabaseKit
import DatabaseTypes
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("S05 selected string binding and identity")
struct CanonicalSQLStringBindingTests {
    @Test("Static string and binary overloads preserve result declarations and CHAR length inputs")
    func staticOverloads() throws {
        let character = try QueryColumn(number: 1, name: "c", type: .sql(.char(length: 4)), isNullable: true)
        let bytes = try QueryColumn(number: 2, name: "b", type: .sql(.blob), isNullable: false)
        let binder = binder(columns: [character, bytes])
        let length = try binder.bind(.function(FunctionCall(name: "length", arguments: [.column(ColumnRef("c"))])))
        #expect(length.type == .sql(.integer))
        #expect(length.children[0].type == character.type)
        let predicate = try binder.bind(.patternMatch(subject: .column(ColumnRef("c")),
            pattern: .literal(.sqlUnknownString("a")), escape: nil, kind: .like))
        #expect(predicate.children[0].type == character.type)
        #expect(predicate.children[1].type == .sql(.text))
        let byteLength = try binder.bind(.function(FunctionCall(name: "length", arguments: [.column(ColumnRef("b"))])))
        #expect(byteLength.type == .sql(.integer))
        #expect(byteLength.children[0].type == .sql(.blob))
        let substring = try binder.bind(.function(FunctionCall(name: "substring", arguments: [.column(ColumnRef("b")), .literal(.queryValue(.primitive(.int32(1)), type: .sql(.integer)))])))
        #expect(substring.type == .sql(.blob))
        #expect(substring.children[1].type == .sql(.integer))
        let bit = try binder.bind(.function(FunctionCall(name: "get_bit", arguments: [.column(ColumnRef("b")), .literal(.int(0))])))
        #expect(bit.children[1].type == .sql(.bigint))
        let captures = try binder.bind(.function(FunctionCall(name: "regexp_match", arguments: [.literal(.null), .literal(.string("(a)"))])))
        #expect(captures.type == .sql(.array(.text)))
        #expect(captures.isNullable)
        let concat = try binder.bind(.function(FunctionCall(name: "concat", arguments: [.literal(.null), .literal(.null)])))
        #expect(concat.type == .sql(.text))
        #expect(!concat.isNullable)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder.bind(.function(FunctionCall(name: "length", arguments: [.cast(.literal(.null), targetType: .integer)])))
        }
    }

    @Test("Explicit and assignment CHAR casts retain separate contexts before NULL propagation")
    func castContexts() async throws {
        let binder = binder()
        let explicit = try binder.bind(.cast(.literal(.sqlUnknownString("abcd")), targetType: .char(length: 2)))
        #expect(explicit.type == .sql(.char(length: 2)))
        #expect(explicit.children[0].type == .sql(.text))
        #expect(explicit.castContext == .explicit)
        let assignment = try binder.bindAssignment(.literal(.sqlUnknownString("abcd")), to: .sql(.char(length: 2)))
        #expect(assignment.children[0].type == .sql(.text))
        #expect(assignment.castContext == .assignment)
        let numeric = try binder.bindAssignment(.literal(.sqlUnknownString("1.2350")), to: .sql(.decimal(precision: 4, scale: 2)))
        #expect(numeric.children[0].type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(numeric.castContext == .assignment)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1000,
            timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
        let row = CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: []))
        do {
            let result = try await DatabaseContext.evaluateCanonicalScalar(explicit, on: row, workMeter: meter)
            #expect(result.value == .primitive(.string("ab")))
            #expect(!result.leases.isEmpty)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        await #expect(throws: CanonicalReadError.self) {
            _ = try await DatabaseContext.evaluateCanonicalScalar(assignment, on: row, workMeter: meter)
        }
        #expect(meter.retainedIntermediateBytes == 0)
        do {
            let rounded = try await DatabaseContext.evaluateCanonicalScalar(numeric, on: row, workMeter: meter)
            #expect(rounded.value == .numeric(try QueryNumericValue(sqlLexicalForm: "1.24", maximumUTF8Count: 4)))
        }
        let nested = try binder.bind(.function(FunctionCall(name: "lower", arguments: [
            .cast(.literal(.sqlUnknownString("ÄAB")), targetType: .char(length: 5))])))
        do {
            let result = try await DatabaseContext.evaluateCanonicalScalar(nested, on: row, workMeter: meter)
            #expect(result.value == .primitive(.string("Äab")))
        }
        #expect(meter.retainedIntermediateBytes == 0)
        let commonCharacter = Expression.coalesce([
            .cast(.literal(.sqlUnknownString("abc")), targetType: .char(length: 3)),
            .cast(.literal(.sqlUnknownString("abc")), targetType: .char(length: 4))])
        let bareCharacter = try binder.bind(.cast(commonCharacter, targetType: .char(length: nil)))
        #expect(bareCharacter.children[0].type == .sql(.char(length: nil)))
        #expect(bareCharacter.castContext == .explicit)
        do {
            let result = try await DatabaseContext.evaluateCanonicalScalar(bareCharacter, on: row, workMeter: meter)
            #expect(result.value == .primitive(.string("a")))
        }
        for (value, type) in [(FieldValue.uint8(.max), QueryColumnType.primitive(.uint8)),
            (.uint16(.max), .primitive(.uint16)), (.uint32(.max), .primitive(.uint32)),
            (.uint64(.max), .primitive(.uint64))] {
            let identity = try binder.bindAssignment(.literal(.queryValue(.primitive(value), type: type)), to: type)
            let result = try await DatabaseContext.evaluateCanonicalScalar(identity, on: row, workMeter: meter)
            #expect(result.value == .primitive(value))
        }
        let maximum = try QueryNumericValue(sqlLexicalForm: "18446744073709551615", maximumUTF8Count: 20)
        let numericUnsigned = try binder.bindAssignment(.literal(.queryValue(.numeric(maximum),
            type: .sql(.decimal(precision: nil, scale: nil)))), to: .primitive(.uint64))
        let unsignedResult = try await DatabaseContext.evaluateCanonicalScalar(numericUnsigned, on: row, workMeter: meter)
        #expect(unsignedResult.value == .primitive(.uint64(.max)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try CanonicalQueryTypeBinder.normalize(.numeric(QueryNumericValue(
                value: ExactDecimal(coefficient: -1, scale: 0), displayScale: 0)), to: .primitive(.uint64))
        }
        let typedNull = try binder.bind(.cast(.literal(.queryValue(.primitive(.null), type: .sql(.text))),
            targetType: .decimal(precision: 4, scale: 2)))
        #expect(typedNull.type == .sql(.decimal(precision: 4, scale: 2)))
        #expect(typedNull.isNullable)
        #expect(throws: DatabaseExpressionEvaluationError.self) {
            try binder.bind(.cast(.literal(.queryValue(.primitive(.null), type: .sql(.uuid))), targetType: .integer))
        }
    }

    @Test("CHAR stored keys are rejected recursively while exact text keys remain admitted")
    func storedKeySemantics() throws {
        let character = try QueryColumn(number: 1, name: "c", type: .sql(.char(length: 4)), isNullable: false)
        let text = try QueryColumn(number: 2, name: "t", type: .sql(.text), isNullable: false)
        let binder = binder(columns: [character, text])
        let comparison = Expression.equal(.column(ColumnRef("c")), .literal(.sqlUnknownString("a")))
        let selected = try binder.bind(comparison)
        #expect(selected.children.allSatisfy { if case .sql(.char) = $0.type { return true }; return false })
        #expect(!CanonicalSQLValueSemantics.permitsStoredKey(selected))
        #expect(!CanonicalSQLValueSemantics.permitsStoredKey(try binder.bind(.and(.literal(.bool(true)), .not(comparison)))))
        #expect(CanonicalSQLValueSemantics.permitsStoredKey(try binder.bind(.equal(.column(ColumnRef("t")), .literal(.sqlUnknownString("a"))))))
    }

    @Test("Typed identity trims only CHAR U+0020 and leaves UTF-8 and primitive values exact")
    func typedIdentity() throws {
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: 1000,
            timeoutMilliseconds: 30_000), monotonicClock: SystemStorageClock())
        let character = QueryColumnType.sql(.char(length: nil)), text = QueryColumnType.sql(.text)
        #expect(try CanonicalSQLValueSemantics.equal(.string("a"), .string("a   "), leftType: character, rightType: character))
        #expect(try !CanonicalSQLValueSemantics.equal(.string("a"), .string("a\t"), leftType: character, rightType: character))
        #expect(try !CanonicalSQLValueSemantics.equal(.string("é"), .string("é"), leftType: text, rightType: text))
        #expect(try CanonicalSQLValueSemantics.identity(.string("a   "), type: character, workMeter: meter) == .string("a"))
        #expect(try CanonicalSQLValueSemantics.identity(.string("a   "), type: .primitive(.string), workMeter: meter) == .string("a   "))
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func binder(columns: [QueryColumn] = []) -> CanonicalQueryTypeBinder {
        CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard let slot = columns.firstIndex(where: { $0.name == reference.column }) else {
                throw DatabaseExpressionEvaluationError.missingColumn(reference.column)
            }
            return slot
        }, wildcardSlots: { _ in Array(columns.indices) })
    }
}
