import DatabaseKit
import Testing

@testable import DatabaseEngine

@Persistable
private struct TransactionMutationJournalModel {
    #Directory<TransactionMutationJournalModel>("transaction-journal")

    var id: String
    var revision: Int64
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

        journal.record(
            identity: insertedIdentity,
            previousModel: nil,
            currentModel: initialModel
        )
        journal.record(
            identity: insertedIdentity,
            previousModel: initialModel,
            currentModel: updatedModel
        )
        journal.record(
            identity: deletedIdentity,
            previousModel: deletedModel,
            currentModel: nil
        )
        journal.record(
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
}
