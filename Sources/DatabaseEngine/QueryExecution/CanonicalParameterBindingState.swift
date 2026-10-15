import DatabaseKit
import Synchronization

/// One invocation's ordered parameter inference. Finalization closes inference
/// before storage or graph execution can consume the resolved declarations.
final class CanonicalParameterBindingState: Sendable {
    enum Context { case output, unconstrained, required }

    private struct Unresolved {
        let context: Context
        let occurrence: QueryOccurrenceID?
        let root: QueryOccurrenceID?
    }

    private struct Entry {
        let parameter: QueryParameter
        var type: QueryColumnType?
        var unresolved: [Unresolved] = []
    }

    private struct State {
        var entries: [Entry]
        var finalized = false
    }

    private let state: Mutex<State>
    private let reservation: DatabaseIntermediateReservation?

    init(parameters: [QueryParameter]) {
        state = Mutex(State(entries: parameters.map { Entry(parameter: $0, type: $0.type) }))
        reservation = nil
    }

    init(parameters: [QueryParameter], workMeter: DatabaseWorkMeter) throws {
        let perEntry = UInt64(MemoryLayout<Entry>.stride + MemoryLayout<QueryParameter>.stride)
        let entries = UInt64(parameters.count).multipliedReportingOverflow(by: perEntry)
        let bytes = entries.partialValue.addingReportingOverflow(128)
        guard !entries.overflow, !bytes.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        reservation = try workMeter.reserveIntermediate(bytes: bytes.partialValue, at: .bindingCandidate)
        state = Mutex(State(entries: parameters.map { Entry(parameter: $0, type: $0.type) }))
    }

    func resolve(
        _ reference: QueryParameterReference,
        contextualType: QueryColumnType?,
        context: Context,
        occurrence: QueryOccurrenceID? = nil,
        root: QueryOccurrenceID? = nil
    ) throws -> QueryParameter {
        try state.withLock { state in
            let indices = state.entries.indices.filter { index in
                let parameter = state.entries[index].parameter
                switch reference {
                case .position(let position): return parameter.position == position
                case .name(let name): return parameter.name == name
                }
            }
            guard indices.count == 1, let index = indices.first else {
                throw DatabaseExpressionEvaluationError.unboundParameter
            }
            let parameter = state.entries[index].parameter
            if state.entries[index].type == nil, !parameter.value.isNull {
                if case .primitive(.array(let values)) = parameter.value, values.isEmpty {
                    // An empty carrier has no intrinsic element declaration.
                } else {
                    state.entries[index].type = try CanonicalQueryTypeBinder.type(of: parameter.value)
                }
            }
            if state.entries[index].type == nil, let contextualType {
                guard !state.finalized else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "unresolved parameter")
                }
                state.entries[index].type = contextualType
            } else if state.entries[index].type == nil, !state.finalized {
                state.entries[index].unresolved.append(Unresolved(context: context, occurrence: occurrence, root: root))
            }
            return QueryParameter(position: parameter.position, name: parameter.name,
                                  value: parameter.value, type: state.entries[index].type)
        }
    }

    func declaredType(_ reference: QueryParameterReference) throws -> QueryColumnType? {
        try state.withLock { state in
            let matches = state.entries.filter { entry in
                switch reference {
                case .position(let position): return entry.parameter.position == position
                case .name(let name): return entry.parameter.name == name
                }
            }
            guard matches.count == 1, let entry = matches.first else {
                throw DatabaseExpressionEvaluationError.unboundParameter
            }
            if let type = entry.type { return type }
            if entry.parameter.value.isNull { return nil }
            if case .primitive(.array(let values)) = entry.parameter.value, values.isEmpty { return nil }
            return try CanonicalQueryTypeBinder.type(of: entry.parameter.value)
        }
    }

    func finalize() throws {
        try state.withLock { state in
            guard !state.finalized else { return }
            for index in state.entries.indices {
                let unresolved = state.entries[index].unresolved
                if !unresolved.isEmpty {
                    func failure(_ operation: String) -> any Error {
                        let cause = DatabaseExpressionEvaluationError.typeMismatch(operation: operation)
                        if let first = unresolved.first, first.occurrence != nil {
                            return CanonicalSQLBindingFailure(cause: .expression(cause), occurrence: first.occurrence, root: first.root)
                        }
                        return cause
                    }
                    if state.entries[index].type != nil {
                        throw failure("ambiguous parameter type")
                    }
                    guard unresolved.allSatisfy({ if case .output = $0.context { return true }; return false }) else {
                        throw failure(unresolved.count == 1 ? "indeterminate parameter type" : "ambiguous parameter type")
                    }
                    state.entries[index].type = .sql(.text)
                }
            }
            state.finalized = true
        }
    }

    var resolvedParameters: [QueryParameter] {
        state.withLock { state in
            state.entries.map { entry in
                QueryParameter(position: entry.parameter.position, name: entry.parameter.name,
                               value: entry.parameter.value, type: entry.type)
            }
        }
    }
}
