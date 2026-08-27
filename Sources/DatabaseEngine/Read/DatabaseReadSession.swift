import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

/// Copyable task-local bridge for one noncopyable result.
///
/// The value crosses only the synchronous mutex boundary used to transfer it
/// out of an API whose generic result is currently constrained to Copyable.
final class DatabaseReadResultBox<
    Value: ~Copyable & Sendable
>: Sendable {
    private let storage: Mutex<Value?>

    init(_ value: consuming sending Value) {
        self.storage = Mutex(.some(consume value))
    }

    func take() -> sending Value {
        storage.withLock { stored in
            guard let value = stored.take() else {
                preconditionFailure(
                    "A read result may be transferred exactly once"
                )
            }
            return value
        }
    }
}

/// Construction authority for one Fusion index child capability.
///
/// The production initializer is file-private so only the parent read session
/// can issue it. The DEBUG factory exists solely for DatabaseEngine's direct
/// low-level lifecycle tests and is not visible to feature modules.
final class FusionIndexReadAdmission: Sendable {
    let transaction: any TransactionReadAccess
    let snapshot: Bool
    let workMeter: DatabaseWorkMeter

    fileprivate init(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) {
        self.transaction = transaction
        self.snapshot = snapshot
        self.workMeter = workMeter
    }

    #if DEBUG
    static func testing(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) -> FusionIndexReadAdmission {
        FusionIndexReadAdmission(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
    }
    #endif
}

/// One context-bound, transaction-bound capability for feature read execution.
///
/// A feature executor receives this facade instead of `DatabaseContext`. The
/// facade exposes the admitted storage snapshot and the exact read operations
/// owned by DatabaseEngine, but it cannot create transactions, mutate storage,
/// change runtime configuration, or retain container lifecycle authority.
public struct DatabaseReadSession: Sendable {
    package static let scopeCursorRegistryContainerByteCount = UInt64(
        MemoryLayout<[
            UInt64: @Sendable () async throws -> Void
        ]>.stride
    )

    /// Non-forgeable proof that this session admitted one polymorphic read.
    package struct PolymorphicReadAdmission: Sendable {
        package enum Operation: Sendable, Equatable {
            case scan
            case orderedPointFetch
        }

        private let groupIdentifier: String
        private let operation: Operation
        package let workMeter: DatabaseWorkMeter

        fileprivate init(
            groupIdentifier: String,
            operation: Operation,
            workMeter: DatabaseWorkMeter
        ) {
            self.groupIdentifier = groupIdentifier
            self.operation = operation
            self.workMeter = workMeter
        }

        package func require(
            groupIdentifier: String,
            operation: Operation
        ) throws {
            guard self.groupIdentifier == groupIdentifier,
                  self.operation == operation else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
        }
    }

    /// Linear proof that one retained-model point-fetch batch was admitted by
    /// this session before QueryExecution can form a storage key.
    package struct RetainedModelFetchAdmission: ~Copyable, Sendable {
        package let entity: Schema.Entity
        package let transaction: DatabaseReadTransaction
        package let admittedFieldNames: Set<String>
        package let snapshot: Bool
        package let workMeter: DatabaseWorkMeter

        // Retaining the exact evidence also retains its schema lease. The
        // initializer is file-private so only DatabaseReadSession can bind
        // these execution values into one admission.
        private let authorization: DatabaseReadAuthorization

        fileprivate init(
            entity: Schema.Entity,
            transaction: DatabaseReadTransaction,
            admittedFieldNames: Set<String>,
            snapshot: Bool,
            workMeter: DatabaseWorkMeter,
            authorization: DatabaseReadAuthorization
        ) {
            self.entity = entity
            self.transaction = transaction
            self.admittedFieldNames = admittedFieldNames
            self.snapshot = snapshot
            self.workMeter = workMeter
            self.authorization = authorization
        }
    }

    private final class Scope: Sendable {
        private typealias CursorCleanup = @Sendable () async throws -> Void

        private struct CursorRecord: Sendable {
            let drainEligibility: DatabaseReadCursorDrainEligibility
            let cleanup: CursorCleanup
        }

        private enum Lifecycle: Sendable {
            case active
            case closing
            case inactive
        }

        struct Owners: Sendable {
            let context: DatabaseContext
            let binding: DatabaseTransactionExecutionBinding
            let storageAccess: ReadAuthorizedTransactionAccess
            let policy: DatabaseReadPolicy
        }

        private struct State: Sendable {
            var lifecycle = Lifecycle.active
            var activeOperationCount = 0
            var invalidationWaiters: [CheckedContinuation<Void, Never>] = []
            var nextCursorIdentifier: UInt64 = 1
            var cursors: [UInt64: CursorRecord] = [:]
            var accountedCursorCapacity = 0
            var owners: Owners?
        }

        private final class CursorRegistration: Sendable {
            private let onRelease: @Sendable () -> Void
            private let reservation: DatabaseIntermediateReservation
            private let operation: DatabaseReadScopeOperationLease

            init(
                reservation: DatabaseIntermediateReservation,
                operation: DatabaseReadScopeOperationLease,
                onRelease: @escaping @Sendable () -> Void
            ) {
                self.reservation = reservation
                self.operation = operation
                self.onRelease = onRelease
            }

            deinit {
                onRelease()
                operation.end()
            }
        }

        private let state: Mutex<State>
        private let workMeter: DatabaseWorkMeter
        private let cursorRegistryLayout: DatabaseRetainedHashTableLayout
        private let cursorRegistryReservation: DatabaseIntermediateReservation

        init(
            context: DatabaseContext,
            binding: DatabaseTransactionExecutionBinding,
            storageAccess: ReadAuthorizedTransactionAccess,
            policy: DatabaseReadPolicy,
            workMeter: DatabaseWorkMeter
        ) throws {
            let cursorRegistryLayout = try DatabaseRetainedHashTableLayout
                .validated(
                    containerByteCount: UInt64(
                        DatabaseReadSession
                            .scopeCursorRegistryContainerByteCount
                    ),
                    elementCapacitySlotByteCount: UInt64(
                        max(
                            1,
                            MemoryLayout<(UInt64, CursorRecord)>.stride
                        )
                    )
                )
            self.workMeter = workMeter
            self.cursorRegistryLayout = cursorRegistryLayout
            self.cursorRegistryReservation = try workMeter
                .reserveIntermediate(
                    bytes: cursorRegistryLayout.containerByteCount,
                    at: .indexScan
                )
            self.state = Mutex(
                State(
                    owners: Owners(
                        context: context,
                        binding: binding,
                        storageAccess: storageAccess,
                        policy: policy
                    )
                )
            )
        }

        func beginOperation() throws -> (
            owners: Owners,
            lease: DatabaseReadScopeOperationLease
        ) {
            if let admitted = DatabaseReadScopeOperationLease.current {
                let lease = try admitted.borrowed(
                    by: ObjectIdentifier(self)
                )
                let owners = try state.withLock { state in
                    guard state.lifecycle != .inactive,
                          let owners = state.owners else {
                        throw DatabaseTransactionError
                            .invalidOperationContext
                    }
                    try validateBinding(owners)
                    return owners
                }
                return (owners, lease)
            }

            let owners = try state.withLock { state in
                guard state.lifecycle == .active,
                      let owners = state.owners else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                try validateBinding(owners)
                let (count, overflow) = state.activeOperationCount
                    .addingReportingOverflow(1)
                guard !overflow else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                state.activeOperationCount = count
                return owners
            }
            return (
                owners,
                DatabaseReadScopeOperationLease(
                    scopeIdentity: ObjectIdentifier(self),
                    validateOperation: { [self] in
                        try validateAdmittedOperation()
                    },
                    rootDidEnd: { [self] in rootOperationDidEnd() },
                    endOperation: { [self] in endOperation() }
                )
            )
        }

        func beginStorageOperation() throws
            -> DatabaseReadScopeOperationLease {
            return try beginOperation().lease
        }

        func withOperation<Result: Sendable>(
            _ body: @Sendable (
                Owners,
                DatabaseReadScopeOperationLease
            ) async throws -> Result
        ) async throws -> Result {
            let (owners, operation) = try beginOperation()
            defer { operation.end() }
            return try await DatabaseReadScopeOperationLease.$current
                .withValue(operation) {
                    try await body(owners, operation)
                }
        }

        func withNoncopyableOperation<Result: ~Copyable & Sendable>(
            _ body: @Sendable (
                Owners,
                DatabaseReadScopeOperationLease
            ) async throws -> sending Result
        ) async throws -> sending Result {
            let (owners, operation) = try beginOperation()
            defer { operation.end() }

            let output = try await DatabaseReadScopeOperationLease.$current
                .withValue(operation) {
                    DatabaseReadResultBox(
                        try await body(owners, operation)
                    )
                }
            return output.take()
        }

        func storageAccess() throws -> ReadAuthorizedTransactionAccess {
            try state.withLock { state in
                guard state.lifecycle != .inactive,
                      let owners = state.owners else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                try validateBinding(owners)
                return owners.storageAccess
            }
        }

        func validate() throws {
            try state.withLock { state in
                guard state.lifecycle == .active,
                      let owners = state.owners else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                try validateBinding(owners)
            }
        }

        func requireWorkMeter(_ candidate: DatabaseWorkMeter) throws {
            guard workMeter === candidate else {
                throw DatabaseReadSessionError.workMeterMismatch
            }
        }

        func operationWorkMeter() -> DatabaseWorkMeter {
            workMeter
        }

        func makeCursor(
            _ factory: @Sendable (
                DatabaseReadScopeOperationLease
            ) -> KeyValueCursor,
            operation: DatabaseReadScopeOperationLease
        ) -> KeyValueCursor {
            // The admitted operation remains counted across the synchronous
            // factory boundary. Closing therefore cannot make the scope
            // inactive before this cursor is registered, and no second
            // pending-registration lifecycle is required.
            let cursorOperation: DatabaseReadScopeOperationLease
            do {
                cursorOperation = try operation.borrowed(
                    by: ObjectIdentifier(self)
                )
                operation.end()
            } catch {
                let admissionError = error
                operation.end()
                return KeyValueCursor(validatingScope: {
                    throw admissionError
                })
            }
            let reservation: DatabaseIntermediateReservation
            let identifier: UInt64
            do {
                reservation = try workMeter.reserveIntermediate(
                    rows: 1,
                    bytes: UInt64(MemoryLayout<KeyValueCursor>.stride + 128),
                    at: .indexScan
                )
                let admission = try state.withLock { state in
                    guard state.lifecycle != .inactive,
                          state.activeOperationCount > 0 else {
                        throw DatabaseTransactionError
                            .invalidOperationContext
                    }
                    let (requiredCount, countOverflow) = state.cursors.count
                        .addingReportingOverflow(1)
                    guard !countOverflow else {
                        throw DatabaseTransactionError.invalidOperationContext
                    }
                    let identifier = state.nextCursorIdentifier
                    let nextIdentifier = identifier
                        .addingReportingOverflow(1)
                    guard !nextIdentifier.overflow else {
                        throw DatabaseTransactionError.invalidOperationContext
                    }
                    let growth = try cursorRegistryLayout.growth(
                        from: state.accountedCursorCapacity,
                        toFit: requiredCount
                    )
                    try cursorRegistryReservation.reserveAdditional(
                        bytes: growth.additionalByteCount,
                        at: .indexScan
                    )
                    if growth.capacity != state.accountedCursorCapacity {
                        state.cursors.reserveCapacity(growth.capacity)
                        state.accountedCursorCapacity = growth.capacity
                    }
                    state.nextCursorIdentifier = nextIdentifier.partialValue
                    return identifier
                }
                identifier = admission
            } catch {
                let admissionError = error
                cursorOperation.end()
                return KeyValueCursor(validatingScope: {
                    throw admissionError
                })
            }
            let cursor = factory(cursorOperation)
            let registration = CursorRegistration(
                reservation: reservation,
                operation: cursorOperation
            ) { [self] in
                removeCursor(identifier)
            }
            let registeredCursor = cursor.retainingLifetime(of: registration)
            let cleanupCursor = registeredCursor
            let waiters = state.withLock { state in
                precondition(state.lifecycle != .inactive)
                state.cursors[identifier] = CursorRecord(
                    drainEligibility: DatabaseReadCursorDrainEligibility(
                        parentOperation: cursorOperation
                    ),
                    cleanup: {
                        defer { cursorOperation.end() }
                        var cursor = cleanupCursor
                        try await cursor.finish()
                    }
                )
                if state.lifecycle == .closing {
                    let waiters = state.invalidationWaiters
                    state.invalidationWaiters.removeAll(
                        keepingCapacity: false
                    )
                    return waiters
                }
                return []
            }
            for waiter in waiters { waiter.resume() }
            return registeredCursor
        }

        func invalidate() async throws {
            state.withLock { state in
                precondition(state.lifecycle == .active)
                state.lifecycle = .closing
            }

            var firstCleanupError: (any Error)?
            while true {
                let cursors = state.withLock { state in
                    let identifiers = state.cursors.compactMap {
                        identifier, record in
                        record.drainEligibility.allowsInvalidation
                            ? identifier
                            : nil
                    }
                    var cursors: [UInt64: CursorRecord] = [:]
                    cursors.reserveCapacity(identifiers.count)
                    for identifier in identifiers {
                        if let cursor = state.cursors.removeValue(
                            forKey: identifier
                        ) {
                            cursors[identifier] = cursor
                        }
                    }
                    return cursors
                }
                for identifier in cursors.keys.sorted() {
                    guard let record = cursors[identifier] else { continue }
                    do {
                        try await record.cleanup()
                    } catch where firstCleanupError == nil {
                        firstCleanupError = error
                    } catch {
                        // Every registered cursor must reach terminal cleanup;
                        // the first failure remains authoritative.
                    }
                }

                let didFinish = state.withLock { state in
                    guard state.cursors.isEmpty,
                          state.activeOperationCount == 0 else {
                        return false
                    }
                    state.owners = nil
                    state.lifecycle = .inactive
                    return true
                }
                if didFinish { break }

                await withCheckedContinuation { continuation in
                    let resumeImmediately = state.withLock { state in
                        if state.cursors.values.contains(where: {
                            $0.drainEligibility.allowsInvalidation
                        })
                            || state.activeOperationCount == 0 {
                            return true
                        }
                        state.invalidationWaiters.append(continuation)
                        return false
                    }
                    if resumeImmediately { continuation.resume() }
                }
            }

            // Every lifecycle-bearing owner has moved out of the escaped
            // public capability after operations and cursors became terminal.
            cursorRegistryReservation.release()
            if let firstCleanupError { throw firstCleanupError }
        }

        private func endOperation() {
            let waiters = state.withLock { state in
                precondition(state.activeOperationCount > 0)
                state.activeOperationCount -= 1
                guard state.lifecycle == .closing else {
                    return [CheckedContinuation<Void, Never>]()
                }
                let waiters = state.invalidationWaiters
                state.invalidationWaiters.removeAll(keepingCapacity: false)
                return waiters
            }
            for waiter in waiters { waiter.resume() }
        }

        private func rootOperationDidEnd() {
            let waiters = state.withLock { state in
                guard state.lifecycle == .closing else {
                    return [CheckedContinuation<Void, Never>]()
                }
                let waiters = state.invalidationWaiters
                state.invalidationWaiters.removeAll(keepingCapacity: false)
                return waiters
            }
            for waiter in waiters { waiter.resume() }
        }

        private func removeCursor(_ identifier: UInt64) {
            _ = state.withLock { state in
                state.cursors.removeValue(forKey: identifier)
            }
        }

        private func validateBinding(_ owners: Owners) throws {
            let context = owners.context
            let binding = owners.binding
            let storageAccess = owners.storageAccess
            guard let activeBinding = ActiveDatabaseTransactionContext.binding
            else {
                throw DatabaseTransactionError.invalidOperationContext
            }
            try activeBinding.validate(for: context)
            guard activeBinding.schemaLease === binding.schemaLease,
                  storageAccess.matches(activeBinding.transaction)
            else {
                throw DatabaseTransactionError.invalidOperationContext
            }
        }

        private func validateAdmittedOperation() throws {
            try state.withLock { state in
                guard state.lifecycle != .inactive,
                      let owners = state.owners else {
                    throw DatabaseTransactionError.invalidOperationContext
                }
                try validateBinding(owners)
            }
        }
    }

    private let dataRoot: Subspace
    private let scope: Scope

    public let transaction: DatabaseReadTransaction
    public let schema: Schema
    public let monotonicClock: any StorageMonotonicClock
    public let wallClock: any WallClock
    package let schemaGeneration: UInt64

    private init(
        context: DatabaseContext,
        storageAccess: ReadAuthorizedTransactionAccess,
        binding: DatabaseTransactionExecutionBinding,
        workMeter: DatabaseWorkMeter
    ) throws {
        let policy = DatabaseReadPolicy(
            schemaLease: binding.schemaLease,
            authorization: context.authorization
        )
        let scope = try Scope(
            context: context,
            binding: binding,
            storageAccess: storageAccess,
            policy: policy,
            workMeter: workMeter
        )
        self.scope = scope
        let scopedStorageAccess = ReadAuthorizedTransactionAccess.scoped(
            admitted: storageAccess,
            resolveTransaction: scope.storageAccess,
            validateScope: scope.validate,
            beginScopeOperation: scope.beginStorageOperation,
            registerScopeCursor: scope.makeCursor
        )
        self.transaction = DatabaseReadTransaction(
            storageAccess: scopedStorageAccess
        )
        self.schema = policy.schema
        self.schemaGeneration = policy.schemaGeneration
        self.dataRoot = binding.dataRoot
        self.monotonicClock = context.container.monotonicClock
        self.wallClock = context.container.wallClock
    }

    private init(
        scope: Scope,
        transaction: DatabaseReadTransaction,
        schema: Schema,
        schemaGeneration: UInt64,
        dataRoot: Subspace,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock
    ) {
        self.scope = scope
        self.transaction = transaction
        self.schema = schema
        self.schemaGeneration = schemaGeneration
        self.dataRoot = dataRoot
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
    }

    package static func withSession<Result: ~Copyable & Sendable>(
        context: DatabaseContext,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable (
            DatabaseReadSession
        ) async throws -> sending Result
    ) async throws -> sending Result {
        guard let binding = ActiveDatabaseTransactionContext.binding else {
            throw DatabaseTransactionError.invalidOperationContext
        }
        try binding.validate(for: context)
        #if DATABASE_MULTI_BASE
        guard binding.resource == context.resource,
              binding.authorization == context.authorization,
              binding.grantedAccess.isSuperset(of: .read) else {
            throw DatabaseGrantAuthorizationError.denied(
                resource: context.resource,
                required: .read
            )
        }
        #endif
        let storageAccess = ReadAuthorizedTransactionAccess
            .admittedReadAccess(binding.transaction)
        let session = try DatabaseReadSession(
            context: context,
            storageAccess: storageAccess,
            binding: binding,
            workMeter: workMeter
        )
        let readBinding = binding.admittingRead(
            session.transaction.storageAccess
        )
        let output = try await ActiveDatabaseTransactionContext.$binding.withValue(
            readBinding
        ) {
            let result: Result
            do {
                result = try await operation(session)
            } catch {
                let operationError = error
                do {
                    try await session.scope.invalidate()
                } catch {
                    throw DatabaseReadSessionCleanupError(
                        operationError: operationError,
                        cleanupError: error
                    )
                }
                throw operationError
            }
            try await session.scope.invalidate()
            return DatabaseReadResultBox(consume result)
        }
        return output.take()
    }

    /// Resolves one schema-declared index on this session's snapshot.
    public func readableIndex(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject
    ) async throws -> ReadableIndex? {
        try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                let index = try await owners.context.indexQueryContext
                    .readableIndex(
                        named: indexName,
                        indexType: indexType,
                        forEntityName: entityName,
                        partitions: partitions,
                        transaction: transaction,
                        authorizedBy: transaction.authorization
                    )
                try operation.validate()
                return index
            }
        }
    }

    /// Resolves one polymorphic index on this session's snapshot.
    public func readablePolymorphicIndex(
        _ descriptor: IndexDeclaration<String>,
        in group: PolymorphicGroup
    ) async throws -> ReadablePolymorphicIndex? {
        try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                let index = try await owners.context.container
                    .readablePolymorphicIndex(
                        descriptor,
                        in: group,
                        transaction: transaction
                    )
                try operation.validate()
                return index
            }
        }
    }

    public func authorizeCanonicalListAccess(
        entity: Schema.Entity,
        selectQuery: SelectQuery
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        try owners.policy.authorizeCanonicalListAccess(
            entityName: entity.name,
            selectQuery: selectQuery
        )
    }

    package func requireCanonicalIndexReadAuthorization(
        entity: Schema.Entity,
        index: IndexDescriptor,
        selectQuery: SelectQuery,
        additionalFieldNames: Set<String>?
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization
        else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        let fields = DatabaseFieldReadAuthorizationPlan.index(
            entity: entity,
            descriptor: index
        ).merging(DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: [
                entity.name: additionalFieldNames ?? Set(entity.allFields)
            ]
        ))
        guard authorization.covers(
            listRequirements: [
                try DatabaseReadPolicy.listRequirement(
                    entityName: entity.name,
                    selectQuery: selectQuery
                )
            ],
            fields: fields
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    public func authorizePolymorphicListAccess(
        group: PolymorphicGroup,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        try owners.policy.authorizePolymorphicListAccess(
            group: group,
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )
    }

    /// Validates the previously sealed polymorphic list and field requirements
    /// without asking the security delegate to make another decision.
    package func requirePolymorphicReadAuthorization(
        group: PolymorphicGroup,
        selectQuery: SelectQuery
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        let requirements = try group.memberTypeNames.map {
            try DatabaseReadPolicy.listRequirement(
                entityName: $0,
                selectQuery: selectQuery
            )
        }
        let fields = DatabaseFieldReadAuthorizationPlan.make(
            query: selectQuery,
            schema: owners.policy.schema
        )
        guard authorization.covers(
            listRequirements: requirements,
            fields: fields
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    /// Validates complete-model field authority for already resolved IDs.
    /// This check happens before Core can form or read a storage key.
    package func requirePolymorphicModelReadAuthorization(
        group: PolymorphicGroup
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        var fieldsByEntity: [String: Set<String>] = [:]
        fieldsByEntity.reserveCapacity(group.memberTypeNames.count)
        for entityName in group.memberTypeNames {
            guard let runtime = owners.policy.entityRuntime(
                named: entityName
            ) else {
                throw DatabaseReadSessionError.authorizationMismatch
            }
            fieldsByEntity[entityName] = Set(runtime.entity.allFields)
        }
        guard authorization.covers(
            listRequirements: [],
            fields: DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: fieldsByEntity
            )
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    package func requireCanonicalPolymorphicIndexReadAuthorization(
        index: IndexDeclaration<String>,
        group: PolymorphicGroup,
        selectQuery: SelectQuery,
        additionalFieldNames: Set<String>?
    ) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization
        else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        let requirements = try group.memberTypeNames.map {
            try DatabaseReadPolicy.listRequirement(
                entityName: $0,
                selectQuery: selectQuery
            )
        }
        let indexFieldNames = Set(index.fieldNames + index.includedFields)
        var fieldsByEntity: [String: Set<String>] = [:]
        for entityName in group.memberTypeNames {
            if let additionalFieldNames {
                fieldsByEntity[entityName] = indexFieldNames.union(
                    additionalFieldNames
                )
            } else {
                guard let runtime = owners.policy.entityRuntime(
                    named: entityName
                ) else {
                    throw DatabaseReadSessionError.authorizationMismatch
                }
                fieldsByEntity[entityName] = Set(runtime.entity.allFields)
            }
        }
        let fields = DatabaseFieldReadAuthorizationPlan(
            fieldsByEntity: fieldsByEntity
        )
        guard authorization.covers(
            listRequirements: requirements,
            fields: fields
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    /// Makes the single authorization decision required to dispatch an RDF
    /// dataset read. Existing evidence is validated and never widened.
    package func admittingRDFDatasetRead() throws -> DatabaseReadSession {
        if transaction.authorization != nil {
            try requireRDFDatasetReadAuthorization()
            return self
        }
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        let fields = DatabaseFieldReadAuthorizationPlan.rdfDataset(
            schema: owners.policy.schema
        )
        let authorization = try owners.policy.authorizeRead(
            listRequirements: [],
            fields: fields
        )
        return try authorizedSession(authorization)
    }

    /// Requires sealed authorization for the canonical RDF dataset without
    /// making another policy decision.
    package func requireRDFDatasetReadAuthorization() throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        let fields = DatabaseFieldReadAuthorizationPlan.rdfDataset(
            schema: owners.policy.schema
        )
        guard authorization.covers(
            listRequirements: [],
            fields: fields
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    public func executeSPARQLAsk(
        _ query: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject = FieldObject()
    ) async throws -> Bool {
        try scope.requireWorkMeter(options.workMeter)
        return try await scope.withOperation { owners, operation in
            let authorizedSession = try admittingRDFDatasetRead()
            guard let executor = owners.policy.sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource(
                    "SPARQL source executor is not registered"
                )
            }
            let result = try await executor.executeAskInTransaction(
                session: authorizedSession,
                askQuery: query,
                options: options,
                partitions: partitions
            )
            try operation.validate()
            return result
        }
    }

    public func executeSPARQLConstruct(
        _ query: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject = FieldObject()
    ) async throws -> DatabaseRetainedRDFGraph {
        try scope.requireWorkMeter(options.workMeter)
        return try await scope.withNoncopyableOperation { owners, operation in
            let authorizedSession = try admittingRDFDatasetRead()
            guard let executor = owners.policy.sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource(
                    "SPARQL source executor is not registered"
                )
            }
            let result = try await executor.executeConstructInTransaction(
                session: authorizedSession,
                constructQuery: query,
                nodeNamespace: nodeNamespace,
                options: options,
                partitions: partitions
            )
            try operation.validate()
            return result
        }
    }

    public func executeSPARQLDescribe(
        _ query: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject = FieldObject()
    ) async throws -> DatabaseRetainedRDFGraph {
        try scope.requireWorkMeter(options.workMeter)
        return try await scope.withNoncopyableOperation { owners, operation in
            let authorizedSession = try admittingRDFDatasetRead()
            guard let executor = owners.policy.sparqlSourceExecutor else {
                throw CanonicalReadError.unsupportedSource(
                    "SPARQL source executor is not registered"
                )
            }
            let result = try await executor.executeDescribeInTransaction(
                session: authorizedSession,
                describeQuery: query,
                options: options,
                partitions: partitions
            )
            try operation.validate()
            return result
        }
    }

    /// Immutable data root captured for this exact read execution.
    public var operationDataRoot: Subspace {
        dataRoot
    }

    package func validatePreparedExecution(
        authorization: DatabaseReadAuthorization,
        workMeter: DatabaseWorkMeter
    ) throws {
        try scope.requireWorkMeter(workMeter)
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        try owners.policy.validate(authorization)
    }

    func executeFusionRelationalRows(
        _ query: SelectQuery,
        options: ReadExecutionContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        authorization: DatabaseReadAuthorization,
        listAuthorizationRequirement:
            DatabaseListReadAuthorizationRequirement
    ) async throws -> CanonicalRetainedQueryResponse {
        try scope.requireWorkMeter(options.workMeter)
        return try await scope.withNoncopyableOperation { owners, operation in
            try owners.policy.validate(authorization)
            let result = try await owners.context.executeFusionRelationalRows(
                query,
                options: options,
                preparedFusionGraph: preparedFusionGraph,
                session: self,
                authorization: authorization,
                listAuthorizationRequirement: listAuthorizationRequirement
            )
            try operation.validate()
            return result
        }
    }

    func executeFusionCandidateRelationalRows(
        _ candidates: FusionCandidateDomain,
        query: SelectQuery,
        options: ReadExecutionContext,
        preparedFusionGraph: FusionPreparedQueryGraph,
        authorization: DatabaseReadAuthorization
    ) async throws -> CanonicalRetainedQueryResponse {
        try scope.requireWorkMeter(options.workMeter)
        return try await scope.withNoncopyableOperation { owners, operation in
            try owners.policy.validate(authorization)
            let result = try await owners.context
                .executeFusionCandidateRelationalRows(
                    candidates,
                    query: query,
                    options: options,
                    preparedFusionGraph: preparedFusionGraph,
                    session: self,
                    authorization: authorization
                )
            try operation.validate()
            return result
        }
    }

    /// Keeps one admitted session operation alive for a complete canonical
    /// execution. Nested feature and storage operations borrow this lifetime
    /// instead of creating another read-session root for the same snapshot.
    @_spi(DatabaseExecution)
    public func withCanonicalExecution<Result: Sendable>(
        workMeter: DatabaseWorkMeter,
        _ body: @Sendable (
            DatabaseReadTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try scope.requireWorkMeter(workMeter)
        return try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                let result = try await body(transaction)
                try operation.validate()
                return result
            }
        }
    }

    package func authorizedTransaction(
        _ authorization: DatabaseReadAuthorization
    ) throws -> DatabaseReadTransaction {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        try owners.policy.validate(authorization)
        return DatabaseReadTransaction(
            storageAccess: transaction.storageAccess,
            authorization: authorization
        )
    }

    package func authorizedSession(
        _ authorization: DatabaseReadAuthorization
    ) throws -> DatabaseReadSession {
        let authorized = try authorizedTransaction(authorization)
        return DatabaseReadSession(
            scope: scope,
            transaction: authorized,
            schema: schema,
            schemaGeneration: schemaGeneration,
            dataRoot: dataRoot,
            monotonicClock: monotonicClock,
            wallClock: wallClock
        )
    }

    package func readableFusionIndex(
        descriptor: IndexDescriptor,
        entity: Schema.Entity,
        partitions: FieldObject,
        authorization: DatabaseReadAuthorization
    ) async throws -> ReadableIndex? {
        try await scope.withOperation { owners, operation in
            try owners.policy.validate(authorization)
            let index = try await owners.context.indexQueryContext
                .readableIndex(
                    named: descriptor.name,
                    indexType: descriptor.type,
                    forEntityName: entity.name,
                    partitions: partitions,
                    transaction: self.transaction,
                    authorizedBy: authorization
                )
            try operation.validate()
            return index
        }
    }

    func withFusionIndexReadLease<Result: Sendable>(
        index: ReadableIndex,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        _ operation: @Sendable (
            FusionIndexReadSession
        ) async throws -> Result
    ) async throws -> Result {
        try scope.requireWorkMeter(workMeter)
        return try await scope.withOperation { _, parentOperation in
            let admission = FusionIndexReadAdmission(
                transaction: self.transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
            let lease = try FusionIndexReadSession(
                index: index,
                admission: admission
            )
            let result: Result
            do {
                result = try await operation(lease)
            } catch {
                let operationError = error
                do {
                    try await lease.invalidate()
                } catch {
                    throw StorageRangeCleanupError(
                        iterationError: operationError,
                        cleanupError: error
                    )
                }
                throw operationError
            }
            try await lease.invalidate()
            try parentOperation.validate()
            return result
        }
    }

    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedPersistedModels
    where PrimaryKeys: Collection & Sendable,
        PrimaryKeys.Element == Tuple {
        try scope.requireWorkMeter(workMeter)
        return try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                guard let admittedAuthorization = transaction.authorization
                else {
                    throw DatabaseReadSessionError.authorizationMismatch
                }
                try owners.policy.validate(admittedAuthorization)
                let admittedFieldNames = Set(entity.allFields)
                guard admittedAuthorization.covers(
                    listRequirements: [],
                    fields: DatabaseFieldReadAuthorizationPlan(
                        fieldsByEntity: [
                            entity.name: admittedFieldNames
                        ]
                    )
                ) else {
                    throw DatabaseReadSessionError.authorizationMismatch
                }
                let admittedTransaction = DatabaseReadTransaction(
                    storageAccess: transaction.storageAccess,
                    authorization: admittedAuthorization
                )
                let models = try await owners.context
                    .fetchPersistedModelsPreservingOrder(
                        entity: entity,
                        primaryKeys: primaryKeys,
                        partitions: partitions,
                        transaction: admittedTransaction,
                        snapshot: snapshot,
                        workMeter: workMeter,
                        admittedFieldNames: admittedFieldNames
                    )
                try operation.validate()
                return models
            }
        }
    }

    package func fetchRetainedPersistedModelsPreservingOrder(
        entity: Schema.Entity,
        primaryKeys: any DatabaseRetainedPrimaryKeyCollection,
        partitions: FieldObject,
        snapshot: Bool
    ) async throws -> DatabaseRetainedPersistedModels {
        try await fetchRetainedModelsPreservingOrder(
            entity: entity,
            primaryKeys: primaryKeys,
            partitions: partitions,
            snapshot: snapshot,
            readAuthority: .complete
        )
    }

    package func fetchRetainedFusionCandidateModelsPreservingOrder(
        entity: Schema.Entity,
        primaryKeys: any DatabaseRetainedPrimaryKeyCollection,
        partitions: FieldObject,
        snapshot: Bool,
        authorization: DatabaseReadAuthorization
    ) async throws -> DatabaseRetainedPersistedModels {
        try await fetchRetainedModelsPreservingOrder(
            entity: entity,
            primaryKeys: primaryKeys,
            partitions: partitions,
            snapshot: snapshot,
            readAuthority: .fusionProjection(authorization)
        )
    }

    private enum RetainedModelReadAuthority: Sendable {
        case complete
        case fusionProjection(DatabaseReadAuthorization)
    }

    private func fetchRetainedModelsPreservingOrder(
        entity: Schema.Entity,
        primaryKeys: any DatabaseRetainedPrimaryKeyCollection,
        partitions: FieldObject,
        snapshot: Bool,
        readAuthority: RetainedModelReadAuthority
    ) async throws -> DatabaseRetainedPersistedModels {
        let workMeter = scope.operationWorkMeter()
        guard primaryKeys.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        return try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                let admittedAuthorization: DatabaseReadAuthorization
                switch readAuthority {
                case .complete:
                    guard let transactionAuthorization = transaction
                        .authorization
                    else {
                        throw DatabaseReadSessionError.authorizationMismatch
                    }
                    admittedAuthorization = transactionAuthorization
                case .fusionProjection(let authorization):
                    admittedAuthorization = authorization
                }
                try owners.policy.validate(admittedAuthorization)
                let admittedFieldNames: Set<String>
                switch readAuthority {
                case .complete:
                    admittedFieldNames = Set(entity.allFields)
                case .fusionProjection:
                    guard let fields = admittedAuthorization
                        .admittedFieldNames(forEntityName: entity.name) else {
                        throw DatabaseReadSessionError.authorizationMismatch
                    }
                    admittedFieldNames = fields
                }
                guard admittedAuthorization.covers(
                    listRequirements: [],
                    fields: DatabaseFieldReadAuthorizationPlan(
                        fieldsByEntity: [
                            entity.name: admittedFieldNames
                        ]
                    )
                ) else {
                    throw DatabaseReadSessionError.authorizationMismatch
                }
                let admittedTransaction = DatabaseReadTransaction(
                    storageAccess: transaction.storageAccess,
                    authorization: admittedAuthorization
                )
                let admission = RetainedModelFetchAdmission(
                    entity: entity,
                    transaction: admittedTransaction,
                    admittedFieldNames: admittedFieldNames,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    authorization: admittedAuthorization
                )
                let models = try await owners.context
                    .fetchRetainedPersistedModelsPreservingOrder(
                        primaryKeys: primaryKeys,
                        partitions: partitions,
                        admission: consume admission
                    )
                try operation.validate()
                return models
            }
        }
    }

    package func scanRetainedPolymorphicItems(
        group: PolymorphicGroup,
        selectQuery: SelectQuery
    ) async throws -> sending DatabaseRetainedPolymorphicEntities {
        try await scope.withNoncopyableOperation { owners, operation in
            try requirePolymorphicReadAuthorization(
                group: group,
                selectQuery: selectQuery
            )
            let admission = PolymorphicReadAdmission(
                groupIdentifier: group.identifier,
                operation: .scan,
                workMeter: scope.operationWorkMeter()
            )
            let entities = try await owners.context
                .scanRetainedPolymorphicItems(
                    group: group,
                    session: self,
                    admission: admission
                )
            try operation.validate()
            return entities
        }
    }

    package func fetchRetainedPolymorphicItemsPreservingOrder(
        group: PolymorphicGroup,
        ids: any DatabaseRetainedPrimaryKeyCollection,
        snapshot: Bool = false
    ) async throws -> sending DatabaseRetainedPolymorphicEntities {
        let workMeter = scope.operationWorkMeter()
        guard ids.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        return try await scope.withNoncopyableOperation {
            owners,
            operation in
            try requirePolymorphicModelReadAuthorization(group: group)
            let admission = PolymorphicReadAdmission(
                groupIdentifier: group.identifier,
                operation: .orderedPointFetch,
                workMeter: workMeter
            )
            let entities = try await owners.context
                .fetchRetainedPolymorphicItemsPreservingOrder(
                    group: group,
                    ids: ids,
                    session: self,
                    snapshot: snapshot,
                    admission: admission
                )
            try operation.validate()
            return entities
        }
    }

    // FIXME(INCOMPLETE_IMPLEMENTATION): Bitmap, Rank, FullText, and Vector
    // production executors still use this raw entity-array bridge. DF-06F is
    // complete only after those callers consume the retained aggregate and
    // this declaration is deleted.
    package func fetchPolymorphicItemsPreservingOrder<Identifiers>(
        group: PolymorphicGroup,
        ids: Identifiers,
        snapshot: Bool = false,
        workMeter: DatabaseWorkMeter
    ) async throws -> [PolymorphicEntity?]
    where Identifiers: Collection & Sendable,
        Identifiers.Element == Tuple {
        try scope.requireWorkMeter(workMeter)
        return try await scope.withOperation { owners, operation in
            try await owners.policy.withAuthorization {
                let entities = try await owners.context
                    .fetchPolymorphicItemsPreservingOrder(
                        group: group,
                        ids: Array(ids),
                        transaction: transaction.storageAccess,
                        workMeter: workMeter
                    )
                try operation.validate()
                return entities
            }
        }
    }

    package func polymorphicTypeMap(
        for group: PolymorphicGroup
    ) throws -> [Int64: EntityRuntimeRegistration] {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        return try owners.policy.polymorphicTypeMap(for: group)
    }

    package func entityRuntime(
        named entityName: String
    ) throws -> EntityRuntimeRegistration? {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        return owners.policy.entityRuntime(named: entityName)
    }

    package func indexConfigurations(
        named indexName: String
    ) throws -> [any IndexRuntimeConfiguration] {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        return owners.policy.indexConfigurations(named: indexName)
    }

    package func authorizeGet(_ model: PersistedModel) throws {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        guard let authorization = transaction.authorization else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.validate(authorization)
        guard let admittedFieldNames = authorization.admittedFieldNames(
            forEntityName: model.entity
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
        try owners.policy.authorizeGet(
            model,
            fields: admittedFieldNames
        )
    }

    package func fieldSchema(
        entityName: String,
        fieldName: String
    ) throws -> FieldSchema? {
        let (owners, operation) = try scope.beginOperation()
        defer { operation.end() }
        return owners.policy.fieldSchema(
            entityName: entityName,
            fieldName: fieldName
        )
    }

    @_spi(DatabaseExecution)
    public func executeCanonical(
        _ query: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        try scope.requireWorkMeter(execution.workMeter)
        return try await scope.withOperation { owners, operation in
            let response = try await owners.context.querySessionBound(
                query,
                execution: execution,
                graphPartitions: graphPartitions,
                session: self
            )
            try operation.validate()
            return response
        }
    }

    @_spi(DatabaseExecution)
    public func retainedCanonicalPage(
        _ query: SelectQuery,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> DatabaseRetainedQueryPage {
        try scope.requireWorkMeter(execution.workMeter)
        return try await scope.withNoncopyableOperation { owners, operation in
            let page = try await owners.context.retainedSessionBoundPage(
                query,
                execution: execution,
                graphPartitions: graphPartitions,
                session: self
            )
            try operation.validate()
            return page
        }
    }

}
