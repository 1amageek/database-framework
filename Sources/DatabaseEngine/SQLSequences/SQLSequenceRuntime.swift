import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// Request effect binding shared by selected SQL and mutation column policies.
package enum SQLSequenceRuntime {
    @TaskLocal package static var request: SQLSequenceRequest?
    @TaskLocal package static var statementIndex: Int = 0
    @TaskLocal static var defaultBindings: SQLSequenceDefaultBindings?

    package static func evaluate(_ name: String, arguments: [FieldValue],
                                 occurrence: Expression) async throws -> FieldValue? {
        let function = name
        guard ["nextval", "currval", "lastval", "setval"].contains(function) else { return nil }
        guard let request else { throw SQLSequenceExecutionError.requestRequired }
        if function == "lastval" {
            guard arguments.isEmpty else { throw SQLSequenceExecutionError.invalidArguments(function) }
            return .int64(try await request.lastValue())
        }
        guard (function == "setval" && (arguments.count == 2 || arguments.count == 3))
            || (function != "setval" && arguments.count == 1),
              let target = arguments.first else { throw SQLSequenceExecutionError.invalidArguments(function) }
        // PostgreSQL's selected sequence functions are strict, including SETVAL's arguments.
        if arguments.contains(.null) { return .null }
        guard case .string(let name) = target else { throw SQLSequenceExecutionError.invalidArguments(function) }
        let sequence: SQLSequence
        if let literal = SQLSequenceDefaultBindings.literalTarget(occurrence), let bindings = defaultBindings {
            guard let id = bindings.sequenceIDsByLiteral[SQLCatalogName(literal)] else {
                throw SQLSequenceExecutionError.invalidPersistedState
            }
            let catalog = try request.context.readPolicy().sqlCatalog.catalog
            guard let bound = catalog.sequences.first(where: { $0.id.utf8.elementsEqual(id.value.utf8) }) else {
                throw SQLSequenceExecutionError.unknownSequence(id.value)
            }
            sequence = bound
        } else { sequence = try resolve(name: name, request: request) }
        switch function {
        case "currval":
            guard arguments.count == 1 else { throw SQLSequenceExecutionError.invalidArguments(function) }
            return .int64(try await request.currentValue(sequence: sequence))
        case "nextval":
            guard arguments.count == 1 else { throw SQLSequenceExecutionError.invalidArguments(function) }
            let identity = try expressionIdentity(occurrence, workMeter: request.meter)
            defer { withExtendedLifetime(identity.reservation) {} }
            return .int64(try await effect(sequence, operation: .next,
                occurrence: .expression(identity.bytes), request: request))
        default:
            guard arguments.count == 2 || arguments.count == 3,
                  case .int64(let value) = arguments[1] else {
                throw SQLSequenceExecutionError.invalidArguments(function)
            }
            let called: Bool
            if arguments.count == 2 { called = true }
            else if case .bool(let value) = arguments[2] { called = value }
            else { throw SQLSequenceExecutionError.invalidArguments(function) }
            guard value >= sequence.minimum, value <= sequence.maximum else {
                throw SQLSequenceExecutionError.invalidArguments("SETVAL value is outside sequence bounds")
            }
            let identity = try expressionIdentity(occurrence, workMeter: request.meter)
            defer { withExtendedLifetime(identity.reservation) {} }
            return .int64(try await effect(sequence, operation: .set(value, called),
                occurrence: .expression(identity.bytes), request: request))
        }
    }

    package static func nextValue(sequenceID: String, occurrence: String) async throws -> Int64 {
        guard let request else { throw SQLSequenceExecutionError.requestRequired }
        let catalog = try request.context.makeSQLReadResolution(searchPath: ActiveSQLSession.binding?.resolution.searchPath ?? request.searchPath,
            workMeter: request.meter).snapshot.catalog
        guard let sequence = catalog.sequences.first(where: { $0.id.utf8.elementsEqual(sequenceID.utf8) }) else {
            throw SQLSequenceExecutionError.unknownSequence(sequenceID)
        }
        return try await effect(sequence, operation: .next,
            occurrence: .identity(ByteString(utf8: occurrence)), request: request)
    }

    private static func effect(_ sequence: SQLSequence, operation: SQLSequenceRequest.Operation,
        occurrence: SQLSequenceRequest.Occurrence, request: SQLSequenceRequest) async throws -> Int64 {
        guard let binding = ActiveDatabaseTransactionContext.binding,
              binding.container === request.context.container else { throw SQLSequenceExecutionError.requestRequired }
        return try await request.effect(sequence: sequence, operation: operation, occurrence: occurrence,
            statement: statementIndex, transaction: binding.transaction, root: binding.systemRoot)
    }

    static func resolve(name: String, request: SQLSequenceRequest) throws -> SQLSequence {
        let resolution = try request.context.makeSQLReadResolution(searchPath: ActiveSQLSession.binding?.resolution.searchPath ?? request.searchPath,
            workMeter: request.meter)
        let parts = try identifiers(name)
        let namespaces: [String]
        if parts.count == 2 { namespaces = [parts[0]] }
        else {
            namespaces = resolution.searchPath.entries.compactMap { entry in
                switch entry {
                case .schema(let value): return value
                case .currentUser: return resolution.currentUser
                }
            }
        }
        for namespace in namespaces {
            guard let namespace = resolution.snapshot.catalog.namespaces.first(where: {
                $0.name.utf8.elementsEqual(namespace.utf8)
            }) else { continue }
            if let sequence = resolution.snapshot.catalog.sequences.first(where: {
                $0.namespaceID.utf8.elementsEqual(namespace.id.utf8)
                    && $0.name.utf8.elementsEqual(parts.last!.utf8)
            }) { return sequence }
        }
        throw SQLSequenceExecutionError.unknownSequence(name)
    }

    private static func expressionIdentity(_ expression: Expression, workMeter meter: DatabaseWorkMeter) throws
        -> (bytes: ByteString, reservation: DatabaseIntermediateReservation?) {
        var reservation: DatabaseIntermediateReservation?
        var bytes: [UInt8] = []
        // Canonical QueryIR bytes preserve exact UTF-8 literals and every argument;
        // an Expression hash alone cannot prove collision-free occurrence identity.
        let envelope = QueryStatement.select(SelectQuery(projection: [.expression(expression)], source: .values([])))
        try QueryIRWireFormat.emitCanonicalEncoding(envelope, prepare: { count in
            reservation = try meter.reserveIntermediate(bytes: UInt64(count), at: .expressionEvaluation)
            try DatabaseByteProcessingMeter.consume(byteCount: UInt64(count), workMeter: meter,
                stage: .expressionEvaluation)
            bytes.reserveCapacity(count)
        }, consume: { buffer in
            bytes.append(contentsOf: buffer.bindMemory(to: UInt8.self))
        })
        return (ByteString(bytes), reservation)
    }

    static func identifiers(_ name: String) throws -> [String] {
        let bytes = Array(name.utf8)
        var index = 0
        var result: [String] = []
        while index < bytes.count {
            var value: [UInt8] = []
            if bytes[index] == 34 {
                index += 1
                var closed = false
                while index < bytes.count {
                    let byte = bytes[index]; index += 1
                    if byte == 34 {
                        if index < bytes.count, bytes[index] == 34 { value.append(34); index += 1 }
                        else { closed = true; break }
                    } else { value.append(byte) }
                }
                guard closed else { throw SQLSequenceExecutionError.invalidArguments("sequence identifier") }
            } else {
                while index < bytes.count, bytes[index] != 46 {
                    let byte = bytes[index]; index += 1
                    value.append(byte >= 65 && byte <= 90 ? byte + 32 : byte)
                }
            }
            guard !value.isEmpty else { throw SQLSequenceExecutionError.invalidArguments("sequence identifier") }
            result.append(String(decoding: value, as: UTF8.self))
            if index < bytes.count {
                guard bytes[index] == 46, index + 1 < bytes.count else {
                    throw SQLSequenceExecutionError.invalidArguments("sequence identifier")
                }
                index += 1
            }
        }
        guard result.count == 1 || result.count == 2 else {
            throw SQLSequenceExecutionError.invalidArguments("sequence identifier")
        }
        return result
    }
}
