import DatabaseKit
import DatabaseTypes
import StorageKit

/// One request's retry-stable sequence effect history and durable recovery scope.
package actor SQLSequenceRequest {
    enum Occurrence: Sendable, Hashable {
        case expression(ByteString)
        case identity(ByteString)
    }
    enum Operation: Sendable, Equatable {
        case next
        case set(Int64, Bool)
    }
    private struct Event: Sendable {
        let occurrence: Occurrence
        let statement: Int
        let sequence: SQLSequence
        let head: ByteString
        let operation: Operation
        let reservation: DatabaseIntermediateReservation
        var value: Int64?
    }

    nonisolated let context: DatabaseContext
    nonisolated let searchPath: SQLSearchPath
    nonisolated let meter: DatabaseWorkMeter
    private let configuration: TransactionConfiguration
    private let deadline: TransactionExecutionDeadline
    private let nonce: ByteString
    private var hasExclusiveAdmission: Bool
    private var events: [Event] = []
    private var cursor = 0
    private var current: [ByteString: Int64] = [:]
    private var last: ByteString?

    init(context: DatabaseContext, searchPath: SQLSearchPath, meter: DatabaseWorkMeter,
         configuration: TransactionConfiguration, deadline: TransactionExecutionDeadline, exclusiveAdmission: Bool) {
        self.hasExclusiveAdmission = exclusiveAdmission
        self.context = context
        self.searchPath = searchPath
        self.meter = meter
        self.configuration = configuration
        self.deadline = deadline
        var random = SystemRandomNumberGenerator()
        self.nonce = ByteString((0..<16).map { _ in UInt8.random(in: .min ... .max, using: &random) })
    }

    var nonceIdentity: String {
        let digits = Array("0123456789abcdef".utf8)
        return String(decoding: nonce.flatMap { [digits[Int($0 >> 4)], digits[Int($0 & 15)]] }, as: UTF8.self)
    }

    func promoteAdmission() throws {
        guard events.isEmpty else { throw SQLSequenceExecutionError.replayDiverged }
        hasExclusiveAdmission = true
    }

    func beginAttempt() {
        cursor = 0
        current.removeAll(keepingCapacity: true)
        last = nil
    }

    func forkHead(step: Int, start: Int64) -> ByteString {
        // The request nonce and step retain the same restart/create incarnation across replay.
        ByteString(Array(nonce) + Array(SQLSequenceStore.encode(Int64(step), called: false))
            + Array(SQLSequenceStore.encode(start, called: false)))
    }

    func effect(sequence: SQLSequence, operation: Operation, occurrence: Occurrence,
                statement: Int, transaction: any TransactionAccess, root: Subspace) async throws -> Int64 {
        guard hasExclusiveAdmission else { throw SQLExclusiveAdmissionDemand() }
        try meter.consume(at: .mutationPlanning)
        let store = SQLSequenceStore(root: root)
        let head = try await transaction.getValue(for: store.headKey(sequence.id), snapshot: false,
            maximumByteCount: 34) ?? ByteString()
        guard head.isEmpty || head.count == 34 else { throw SQLSequenceExecutionError.invalidPersistedState }
        let eventIndex = cursor
        cursor += 1
        if eventIndex < events.count {
            let prior = events[eventIndex]
            guard prior.occurrence == occurrence, prior.statement == statement,
                  prior.sequence == sequence, prior.head == head, prior.operation == operation else {
                throw SQLSequenceExecutionError.replayDiverged
            }
        } else {
            let occurrenceBytes: Int
            switch occurrence {
            case .expression(let bytes), .identity(let bytes): occurrenceBytes = bytes.count
            }
            let reservation = try meter.reserveIntermediate(bytes:
                512 + UInt64(sequence.id.utf8.count + sequence.name.utf8.count + head.count + occurrenceBytes), at: .mutationPlanning)
            events.append(Event(occurrence: occurrence, statement: statement, sequence: sequence,
                head: head, operation: operation, reservation: reservation, value: nil))
        }
        guard let value = events[eventIndex].value else { throw SQLSequenceAllocationDemand(event: eventIndex) }
        let id = ByteString(utf8: sequence.id)
        switch operation {
        case .next: current[id] = value; last = id
        case .set(_, true): current[id] = value
        case .set(_, false): break
        }
        return value
    }

    func currentValue(sequence: SQLSequence) throws -> Int64 {
        try meter.consume(at: .expressionEvaluation)
        guard let value = current[ByteString(utf8: sequence.id)] else {
            throw SQLSequenceExecutionError.currentValueUnavailable(sequence.name)
        }
        return value
    }

    func lastValue() throws -> Int64 {
        try meter.consume(at: .expressionEvaluation)
        guard let last, let value = current[last] else { throw SQLSequenceExecutionError.lastValueUnavailable }
        let catalog = try context.readPolicy().sqlCatalog.catalog
        guard catalog.sequences.contains(where: { ByteString(utf8: $0.id) == last }) else {
            throw SQLSequenceExecutionError.lastValueUnavailable
        }
        return value
    }

    /// Called only after the demand's data attempt authoritatively rolled back and closed.
    func allocate(_ demand: SQLSequenceAllocationDemand) async throws {
        guard events.indices.contains(demand.event), events[demand.event].value == nil else {
            throw SQLSequenceExecutionError.replayDiverged
        }
        try meter.consume(at: .mutationPlanning)
        let event = events[demand.event]
        let storage = try context.executionStorage()
        let store = SQLSequenceStore(root: storage.systemRoot)
        let marker = store.markerKey(request: nonce, event: demand.event)
        let cell = store.cellKey(event.sequence.id, head: event.head)
        let authority = try partitionAuthority()
        let runner = TransactionRunner(transactionExecutor: storage.transactionExecutor,
            clock: context.container.monotonicClock)
        let value = try await runner.run(configuration: configuration, executionDeadline: deadline,
            operationDescription: "SQL sequence allocation", partitionAuthority: authority) { transaction in
            #if DATABASE_MULTI_BASE
            let lease = try self.context.requireOperationDataRoot()
            try await DatabaseGrantStore(resource: lease.resource, root: lease.systemRoot).require(.write,
                authorization: self.context.authorization, transaction: transaction)
            #endif
            if let recovered = try await transaction.getValue(for: marker, snapshot: false, maximumByteCount: 9) {
                return try SQLSequenceStore.decode(recovered).value
            }
            let initial: Int64
            if event.head.isEmpty { initial = event.sequence.start }
            else { initial = try SQLSequenceStore.decode(event.head.suffix(9)).value }
            let state: (value: Int64, called: Bool)
            if let stored = try await transaction.getValue(for: cell, snapshot: false, maximumByteCount: 9) {
                state = try SQLSequenceStore.decode(stored)
            } else { state = (initial, false) }
            let value: Int64
            let called: Bool
            switch event.operation {
            case .next:
                value = try SQLSequenceStore.next(event.sequence, state: state)
                called = true
            case .set(let requested, let isCalled):
                guard requested >= event.sequence.minimum, requested <= event.sequence.maximum else {
                    throw SQLSequenceExecutionError.invalidArguments("SETVAL value is outside sequence bounds")
                }
                value = requested
                called = isCalled
            }
            try transaction.setValue(SQLSequenceStore.encode(value, called: called), for: cell)
            try transaction.setValue(SQLSequenceStore.encode(value, called: true), for: marker)
            return value
        }
        events[demand.event].value = value
    }

    /// Recovery markers are complete at allocation commit; removing them is mandatory close work.
    func close() async throws {
        guard !events.isEmpty else { return }
        let storage = try context.executionStorage()
        let range = SQLSequenceStore(root: storage.systemRoot).recoveryRange(request: nonce)
        let authority = try partitionAuthority()
        let configuration = self.configuration
        let clock = context.container.monotonicClock
        // An unstructured task does not inherit cancellation. Cleanup remains bounded by
        // transaction configuration and closes the recovery transaction before returning.
        try await Task {
            let runner = TransactionRunner(transactionExecutor: storage.transactionExecutor, clock: clock)
            try await runner.run(configuration: configuration, operationDescription: "SQL sequence recovery close",
                partitionAuthority: authority) { transaction in
                try transaction.clearRange(beginKey: range.begin, endKey: range.end)
            }
        }.value
    }

    private func partitionAuthority() throws -> DatabasePartitionAuthority {
        #if DATABASE_MULTI_BASE
        return try context.requireOperationDataRoot().partitionAuthority
        #else
        return context.container.defaultPartitionAuthority
        #endif
    }
}
