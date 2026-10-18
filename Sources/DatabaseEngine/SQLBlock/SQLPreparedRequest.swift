import DatabaseKit
import DatabaseTypes
import Synchronization

/// Attempt-owned registry, deliberately outside the savepoint journal.
final class SQLPreparedRequest: Sendable {
    private struct Definition: Sendable {
        let name: String
        let statement: QueryStatement
        let parameters: [QueryParameter]
        let generation: UInt64
        let schemaIdentity: ByteString
        let searchPath: SQLSearchPath
        let claim: DatabaseIntermediateReservation
    }
    private let state = Mutex<[Definition]>([])

    func prepare(_ definition: SQLPrepare, context: DatabaseContext,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil) async throws {
        guard !state.withLock({ $0.contains(where: { Self.equal($0.name, definition.name) }) }) else {
            throw SQLPreparedExecutionError.duplicateName(definition.name)
        }
        let initialBytes = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: definition.parameterTypes.count, element: QueryParameter.self).bytes
        let claim = try workMeter.reserveIntermediate(bytes: initialBytes + UInt64(definition.name.utf8.count) + 256,
            at: .bindingCandidate)
        let typeBinder = CanonicalQueryTypeBinder(columns: [],
            resolveColumn: { _ in throw DatabaseExpressionEvaluationError.unboundParameter },
            wildcardSlots: { _ in [] }, workMeter: workMeter)
        for type in definition.parameterTypes {
            try workMeter.consume(at: .bindingCandidate)
            _ = try typeBinder.bindScalarAssignment(.literal(.null), to: .sql(type))
        }
        var parameters = definition.parameterTypes.enumerated().map {
            QueryParameter(position: UInt32($0.offset + 1), value: .primitive(.null), type: .sql($0.element))
        }
        // The canonical structural traversal finds parameters in every nested
        // query, including unused definitions, without a second AST walker.
        while true {
            try workMeter.consume(at: .bindingCandidate)
            do {
                try QueryParameterBinder(parameters: parameters, structuralLimits: options.structuralLimits)
                    .validate(definition.statement)
                break
            } catch QueryParameterBindingError.missingPosition(let position) {
                try claim.reserveAdditional(bytes: UInt64(MemoryLayout<QueryParameter>.stride), at: .bindingCandidate)
                parameters.append(QueryParameter(position: position, value: .primitive(.null)))
            } catch QueryParameterBindingError.missingName(let name) {
                throw SQLPreparedExecutionError.namedParameter(name)
            }
        }
        parameters.sort { $0.position < $1.position }
        for (index, parameter) in parameters.enumerated() where parameter.position != UInt32(index + 1) {
            throw SQLPreparedExecutionError.indeterminateParameter(UInt32(index + 1))
        }
        let preparedParameters = parameters
        let inference = try CanonicalParameterBindingState(parameters: preparedParameters, workMeter: workMeter)
        _ = try await SQLPreparedAnalysisRuntime.$parameters.withValue(inference) {
            try await SQLPreparedAnalysis.analyze(definition.statement, parameters: preparedParameters,
                context: context, transaction: transaction, options: options, temporal: temporal, workMeter: workMeter,
                occurrences: occurrences, occurrence: occurrence)
        }
        try inference.finalize()
        let resolved = inference.resolvedParameters
        for parameter in resolved where parameter.type == nil {
            throw SQLPreparedExecutionError.indeterminateParameter(parameter.position)
        }
        let schemaLease = await transaction.currentSQLSchemaLease()
        let entry = Definition(name: definition.name, statement: definition.statement,
            parameters: resolved, generation: schemaLease.generation, schemaIdentity: schemaLease.sqlCatalog.digest, searchPath: options.searchPath, claim: claim)
        try state.withLock { entries in
            guard !entries.contains(where: { Self.equal($0.name, definition.name) }) else {
                throw SQLPreparedExecutionError.duplicateName(definition.name)
            }
            entries.append(entry)
        }
    }

    func execution(_ call: SQLExecute, arguments: [QueryValue], schemaGeneration: UInt64,
        searchPath: SQLSearchPath, schemaIdentity: ByteString, temporal: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter) async throws -> SQLPreparedInvocation {
        let entry = try state.withLock { entries in
            guard let entry = entries.first(where: { Self.equal($0.name, call.name) }) else {
                throw SQLPreparedExecutionError.unknownName(call.name)
            }
            return entry
        }
        guard entry.generation == schemaGeneration, entry.schemaIdentity == schemaIdentity,
              Self.equal(entry.searchPath, searchPath) else {
            throw SQLPreparedExecutionError.staleBinding(call.name)
        }
        guard arguments.count == entry.parameters.count else {
            throw SQLPreparedExecutionError.arity(name: call.name,
                expected: entry.parameters.count, actual: arguments.count)
        }
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: arguments.count, element: QueryParameter.self).adding(
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: arguments.count, element: DatabaseQueryPayloadLease.self))
        let reservation = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        var parameters: [QueryParameter] = []
        var payloads: [DatabaseQueryPayloadLease] = []
        parameters.reserveCapacity(arguments.count)
        payloads.reserveCapacity(arguments.count)
        let row = CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: []))
        for (value, parameter) in zip(arguments, entry.parameters) {
            guard let type = parameter.type else { throw SQLPreparedExecutionError.indeterminateParameter(parameter.position) }
            let binder = CanonicalQueryTypeBinder(columns: [],
                resolveColumn: { _ in throw DatabaseExpressionEvaluationError.unboundParameter },
                wildcardSlots: { _ in [] }, workMeter: workMeter)
            let selected: CanonicalBoundExpression
            do {
                selected = try binder.bindScalarAssignment(.literal(.queryValue(value, type: nil)), to: type)
            } catch DatabaseExpressionEvaluationError.typeMismatch,
                    DatabaseExpressionEvaluationError.invalidCast {
                throw SQLPreparedExecutionError.argumentType(position: parameter.position)
            }
            let converted = try await DatabaseContext.evaluateCanonicalScalar(selected, on: row,
                temporalEnvironment: temporal, workMeter: workMeter)
            parameters.append(QueryParameter(position: parameter.position, value: converted.value, type: type))
            for lease in converted.leases {
                if payloads.count >= arguments.count {
                    try reservation.reserveAdditional(bytes: UInt64(MemoryLayout<DatabaseQueryPayloadLease>.stride), at: .bindingCandidate)
                }
                payloads.append(lease)
            }
        }
        return SQLPreparedInvocation(statement: entry.statement, parameters: parameters, payloadLeases: payloads, reservation: reservation)
    }

    func deallocate(_ name: String?) throws {
        try state.withLock { entries in
            guard let name else { entries.removeAll(); return }
            guard let index = entries.firstIndex(where: { Self.equal($0.name, name) }) else {
                throw SQLPreparedExecutionError.unknownName(name)
            }
            entries.remove(at: index)
        }
    }

    private static func equal(_ lhs: SQLSearchPath, _ rhs: SQLSearchPath) -> Bool {
        guard lhs.entries.count == rhs.entries.count else { return false }
        return zip(lhs.entries, rhs.entries).allSatisfy { left, right in
            switch (left, right) {
            case (.currentUser, .currentUser): return true
            case (.schema(let a), .schema(let b)): return equal(a, b)
            default: return false
            }
        }
    }

    private static func equal(_ lhs: String, _ rhs: String) -> Bool {
        lhs.utf8.elementsEqual(rhs.utf8)
    }
}
