import DatabaseKit
import DatabaseTypes

/// Linear synchronous iteration; each returned row independently retains its claims.
struct SQLJSONRowCursor: ~Copyable {
    private struct State {
        let function: String
        let text: Bool
        let keysOnly: Bool
        let input: QueryValue
        let inputLeases: [DatabaseQueryPayloadLease]
        let lexical: SQLJSONLexicalInput?
        let columns: [QueryColumn]
        let schemaLease: DatabaseQueryPayloadLease
        let memory: SQLJSONMemory
        let limits: QueryValueLimits
        var index: Int
    }
    private enum Storage {
        case active(State)
        case closed
    }
    private var storage: Storage

    init(bound: CanonicalBoundExpression, operands: [QueryValue], inputLeases: [DatabaseQueryPayloadLease],
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws {
        guard case .function(let call) = bound.expression, operands.count == 1, bound.children.count == 1,
              inputLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw SQLJSONMemory.failure("JSON cursor selected signature or meter")
        }
        try SQLJSONMemory.validate(operands[0], type: bound.children[0].type, meter: workMeter)
        let name = call.name
        let each = name == "json_each" || name == "jsonb_each" || name == "json_each_text" || name == "jsonb_each_text"
        let elements = name == "json_array_elements" || name == "jsonb_array_elements" || name == "json_array_elements_text" || name == "jsonb_array_elements_text"
        let keys = name == "json_object_keys" || name == "jsonb_object_keys"
        guard each || elements || keys else { throw DatabaseExpressionEvaluationError.unsupportedFunction(name) }
        let memory = try SQLJSONMemory(workMeter)
        let input = operands[0]
        let lexical: SQLJSONLexicalInput?
        if case .json(let json) = input {
            let indexed = try SQLJSONLexicalInput(json, memory: memory)
            guard elements ? indexed.nodes[0].kind == .array : indexed.nodes[0].kind == .object else {
                throw CanonicalSQLJSONEvaluator.failure(elements ? .jsonArrayRootRequired(function: name) : .jsonObjectRootRequired(function: name))
            }
            lexical = indexed
        } else if case .jsonb(let json) = input {
            let correct: Bool
            switch json.root { case .array: correct = elements; case .object: correct = !elements; default: correct = false }
            guard correct else {
                throw CanonicalSQLJSONEvaluator.failure(elements ? .jsonArrayRootRequired(function: name) : .jsonObjectRootRequired(function: name))
            }
            try memory.bytes(try json.measure().retainedByteCount)
            lexical = nil
        } else if input.isNull { lexical = nil }
        else { throw SQLJSONMemory.failure("JSON cursor input") }
        let text = name.hasSuffix("_text")
        let type: QueryColumnType = text || keys ? .sql(.text) : (name.hasPrefix("jsonb_") ? .sql(.jsonb) : .sql(.json))
        var columns: [QueryColumn] = []
        try memory.prepare(&columns, adding: each ? 2 : 1)
        if each { columns.append(try QueryColumn(number: 1, name: "key", type: .sql(.text), isNullable: false)) }
        columns.append(try QueryColumn(number: each ? 2 : 1, name: keys ? name : "value", type: type, isNullable: text))
        var schemaBytes = try SQLJSONMemory.product(columns.capacity, MemoryLayout<QueryColumn>.stride)
        schemaBytes = try SQLJSONMemory.add(schemaBytes, UInt64(MemoryLayout<[QueryColumn]>.stride))
        for column in columns { schemaBytes = try SQLJSONMemory.add(schemaBytes, UInt64(column.name.utf8.count)) }
        let schema = try workMeter.reserveIntermediate(bytes: schemaBytes, at: .expressionEvaluation)
        let prepared = State(function: name, text: text, keysOnly: keys, input: input, inputLeases: inputLeases,
            lexical: lexical, columns: columns, schemaLease: DatabaseQueryPayloadLease(retaining: schema),
            memory: memory, limits: limits, index: lexical == nil ? 0 : 1)
        // No stored default initialization or partially initialized self crosses a throw.
        self.storage = .active(prepared)
    }

    borrowing func metadata() throws -> (columns: [QueryColumn], lease: DatabaseQueryPayloadLease) {
        guard case .active(let state) = storage else { throw SQLJSONMemory.failure("closed JSON cursor metadata") }
        return (state.columns, state.schemaLease)
    }

    mutating func next() throws -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease])? {
        do {
            guard case .active(var current) = storage else { return nil }
            try current.memory.work()
            if current.input.isNull { close(); return nil }
            let rowMemory = try SQLJSONMemory(current.memory.meter)
            var values: [QueryValue] = []
            try rowMemory.prepare(&values, adding: current.columns.count)
            if let lexical = current.lexical {
                guard current.index < lexical.nodes[0].end else { close(); return nil }
                let node = lexical.nodes[current.index]
                if let key = node.key {
                    values.append(.primitive(.string(try lexical.decodedString(key, limits: current.limits, outputMemory: rowMemory))))
                }
                if !current.keysOnly {
                    values.append(try lexical.value(current.index, text: current.text, limits: current.limits, outputMemory: rowMemory))
                }
                current.index = node.end
            } else {
                let json = try CanonicalSQLJSONEvaluator.jsonb(current.input)
                let selected: QueryJSONBNode
                switch json.root {
                case .object(let members):
                    guard current.index < members.count else { close(); return nil }
                    let member = members[current.index]
                    values.append(.primitive(.string(member.key))); selected = member.value
                case .array(let elements):
                    guard current.index < elements.count else { close(); return nil }
                    selected = elements[current.index]
                default: throw SQLJSONMemory.failure("admitted JSON cursor root")
                }
                if !current.keysOnly {
                    var plan = SQLJSONPlan(memory: rowMemory, limits: current.limits, environment: nil)
                    values.append(try CanonicalSQLJSONEvaluator.selected(selected, text: current.text, plan: &plan))
                }
                current.index += 1
            }
            var rowBytes = try SQLJSONMemory.product(values.capacity, MemoryLayout<QueryValue>.stride)
            rowBytes = try SQLJSONMemory.add(rowBytes, UInt64(MemoryLayout<[QueryValue]>.stride))
            for value in values {
                rowBytes = try SQLJSONMemory.add(rowBytes, CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                    workMeter: current.memory.meter, stage: .expressionEvaluation).bytes)
            }
            let rowClaim = try current.memory.meter.reserveIntermediate(rows: 1, bytes: rowBytes, at: .expressionEvaluation)
            var leases: [DatabaseQueryPayloadLease] = []
            try rowMemory.prepare(&leases, adding: current.inputLeases.count + 1)
            leases.append(contentsOf: current.inputLeases)
            leases.append(DatabaseQueryPayloadLease(retaining: rowClaim))
            // The returned lease-array storage must survive this row's transient builder.
            let leaseBytes = try SQLJSONMemory.add(UInt64(MemoryLayout<[DatabaseQueryPayloadLease]>.stride),
                SQLJSONMemory.product(leases.capacity, MemoryLayout<DatabaseQueryPayloadLease>.stride))
            try rowClaim.reserveAdditional(bytes: leaseBytes, at: .expressionEvaluation)
            storage = .active(current)
            return (values, leases)
        } catch { close(); throw error }
    }

    mutating func close() { storage = .closed }
}
