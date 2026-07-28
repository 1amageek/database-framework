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
        var journal = TransactionMutationJournal()

        journal.record(
            identity: insertedIdentity,
            previousModel: nil,
            currentModel: initial
        )
        journal.record(
            identity: insertedIdentity,
            previousModel: initial,
            currentModel: updated
        )
        journal.record(
            identity: deletedIdentity,
            previousModel: deleted,
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
            (effects[0].model as? TransactionMutationJournalModel)?.revision
                == 2
        )
        #expect(effects[1].kind == .delete)
        #expect(effects[1].identity == deletedIdentity)
        #expect(effects[1].model == nil)

        let currentModels = journal.currentModels()
        #expect(currentModels.count == 1)
        #expect(
            (currentModels[0] as? TransactionMutationJournalModel)?.revision
                == 2
        )
    }
}
