import DatabaseKit
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct TransactionMutationJournalModel {
    #Directory<TransactionMutationJournalModel>("transaction-journal")

    var id: String
    var revision: Int64
    var payload: String = ""
}

@Suite("Transaction mutation journal")
struct TransactionMutationJournalTests {
    @Test("journal preserves first-seen order and net effects")
    func orderedNetEffects() throws {
        let initial = TransactionMutationJournalModel(
            id: "inserted",
            revision: 1
        )
        let updated = TransactionMutationJournalModel(
            id: "inserted",
            revision: 2
        )
        let deleted = TransactionMutationJournalModel(
            id: "deleted",
            revision: 1
        )
        let cancelled = TransactionMutationJournalModel(
            id: "cancelled",
            revision: 1
        )
        let insertedIdentity = try EntityReferenceEncoder.encode(initial)
        let deletedIdentity = try EntityReferenceEncoder.encode(deleted)
        let cancelledIdentity = try EntityReferenceEncoder.encode(cancelled)
        let initialModel = try PersistedModel(initial)
        let updatedModel = try PersistedModel(updated)
        let deletedModel = try PersistedModel(deleted)
        var journal = TransactionMutationJournal()

        try journal.record(
            identity: insertedIdentity,
            previousModel: nil,
            currentModel: initialModel
        )
        try journal.record(
            identity: insertedIdentity,
            previousModel: initialModel,
            currentModel: updatedModel
        )
        try journal.record(
            identity: deletedIdentity,
            previousModel: deletedModel,
            currentModel: nil
        )
        try journal.record(
            identity: cancelledIdentity,
            previousModel: nil,
            currentModel: nil
        )

        let effects = journal.persistedEffects()
        #expect(effects.count == 2)
        #expect(effects[0].kind == .insert)
        #expect(effects[0].identity == insertedIdentity)
        #expect(
            try effects[0].model?.decode(
                as: TransactionMutationJournalModel.self
            ).revision == 2
        )
        #expect(effects[1].kind == .delete)
        #expect(effects[1].identity == deletedIdentity)
        #expect(effects[1].model == nil)

        let currentModels = journal.currentModels()
        #expect(currentModels.count == 1)
        #expect(
            try currentModels[0].decode(
                as: TransactionMutationJournalModel.self
            ).revision == 2
        )
    }

    @Test("metered journal owns retained memory until transaction cleanup")
    func meteredLifetime() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let model = TransactionMutationJournalModel(
            id: "metered",
            revision: 1
        )
        var journal = TransactionMutationJournal()
        try journal.bind(to: meter)
        let containerBytes = meter.retainedIntermediateBytes

        try journal.record(
            identity: EntityReferenceEncoder.encode(model),
            previousModel: nil,
            currentModel: try PersistedModel(model)
        )

        #expect(meter.retainedIntermediateRows == 1)
        #expect(meter.retainedIntermediateBytes > containerBytes)

        journal.removeAll()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("journal rejects a different request meter")
    func rejectsDifferentRequestMeter() throws {
        let budget = ExecutionBudget(
            maximumIntermediateRows: 16,
            maximumIntermediateBytes: 1_048_576
        )
        var journal = TransactionMutationJournal()
        try journal.bind(
            to: DatabaseWorkMeter(
                budget: budget,
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(
            throws: DatabaseTransactionMutationError.workMeterMismatch
        ) {
            try journal.bind(
                to: DatabaseWorkMeter(
                    budget: budget,
                    monotonicClock: TestProcessMonotonicClock()
                )
            )
        }
    }

    @Test("meter binding after an unmetered mutation fails explicitly")
    func rejectsMeterBindingAfterMutation() throws {
        let model = TransactionMutationJournalModel(
            id: "already-recorded",
            revision: 1
        )
        var journal = TransactionMutationJournal()
        try journal.record(
            identity: EntityReferenceEncoder.encode(model),
            previousModel: nil,
            currentModel: try PersistedModel(model)
        )

        #expect(
            throws: DatabaseTransactionMutationError
                .workMeterBoundAfterMutation
        ) {
            try journal.bind(to: makeMeter())
        }
    }

    @Test("failed journal admission preserves state and releases ownership")
    func failedAdmissionIsAtomic() throws {
        let model = TransactionMutationJournalModel(
            id: "over-budget",
            revision: 1
        )
        let meter = makeMeter(maximumIntermediateRows: 0)
        var journal = TransactionMutationJournal()
        try journal.bind(to: meter)
        let containerBytes = meter.retainedIntermediateBytes

        do {
            try journal.record(
                identity: EntityReferenceEncoder.encode(model),
                previousModel: nil,
                currentModel: try PersistedModel(model)
            )
            Issue.record("Expected journal admission to exceed the row budget")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateRows(
                stage: .mutationPlanning,
                consumed: 0,
                requested: _,
                maximum: 0
            ) = error else {
                Issue.record("Unexpected work-limit error: \(error)")
                return
            }
        }

        #expect(journal.persistedEffectCount == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == containerBytes)
        journal.removeAll()
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("replacing a journal value releases payload shrinkage")
    func replacementReleasesShrinkage() throws {
        let small = TransactionMutationJournalModel(
            id: "replacement",
            revision: 1,
            payload: "small"
        )
        let large = TransactionMutationJournalModel(
            id: small.id,
            revision: 2,
            payload: String(repeating: "x", count: 4_096)
        )
        let meter = makeMeter()
        var journal = TransactionMutationJournal()
        try journal.bind(to: meter)
        let identity = try EntityReferenceEncoder.encode(small)
        try journal.record(
            identity: identity,
            previousModel: nil,
            currentModel: try PersistedModel(small)
        )
        let smallBytes = meter.retainedIntermediateBytes

        try journal.record(
            identity: identity,
            previousModel: try PersistedModel(small),
            currentModel: try PersistedModel(large)
        )
        #expect(meter.retainedIntermediateBytes > smallBytes)

        try journal.record(
            identity: identity,
            previousModel: try PersistedModel(large),
            currentModel: try PersistedModel(small)
        )
        #expect(meter.retainedIntermediateBytes == smallBytes)
        journal.removeAll()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("final validation view charges structure without duplicating payload")
    func validationViewSharesJournalPayload() throws {
        let model = TransactionMutationJournalModel(
            id: "validation-view",
            revision: 1,
            payload: String(repeating: "v", count: 4_096)
        )
        let meter = makeMeter()
        var journal = TransactionMutationJournal()
        try journal.bind(to: meter)
        try journal.record(
            identity: EntityReferenceEncoder.encode(model),
            previousModel: nil,
            currentModel: try PersistedModel(model)
        )
        let journalRows = meter.retainedIntermediateRows
        let journalBytes = meter.retainedIntermediateBytes

        do {
            let retained = try #require(
                try journal.retainedCurrentModels()
            )
            #expect(retained.count == 1)
            #expect(meter.retainedIntermediateRows == journalRows)
            #expect(meter.retainedIntermediateBytes > journalBytes)
        }

        #expect(meter.retainedIntermediateRows == journalRows)
        #expect(meter.retainedIntermediateBytes == journalBytes)
        journal.removeAll()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(
        maximumIntermediateRows: UInt32 = 64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: 1_048_576
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
