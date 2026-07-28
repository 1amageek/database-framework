import DatabaseKit
import DatabaseTypes

/// Preserves the first-seen order and net effect of transaction mutations.
struct TransactionMutationJournal: Sendable {
    private struct Entry: Sendable {
        let identity: EntityReference
        let originalModel: (any Persistable)?
        var currentModel: (any Persistable)?
    }

    private struct Lookup: Sendable {
        let identity: EntityReference
        let entryIndex: Int
    }

    private var entries: [Entry] = []
    private var lookup: [Lookup] = []

    mutating func record(
        identity: EntityReference,
        previousModel: (any Persistable)?,
        currentModel: (any Persistable)?
    ) {
        let location = lookupLocation(for: identity)
        if location.found {
            let entryIndex = lookup[location.index].entryIndex
            entries[entryIndex].currentModel = currentModel
            return
        }

        let entryIndex = entries.count
        entries.append(
            Entry(
                identity: identity,
                originalModel: previousModel,
                currentModel: currentModel
            )
        )
        lookup.insert(
            Lookup(identity: identity, entryIndex: entryIndex),
            at: location.index
        )
    }

    func currentModels() -> [any Persistable] {
        entries.compactMap(\.currentModel)
    }

    func persistedEffects() -> [PersistableMutationEffect] {
        entries.compactMap { entry in
            switch (entry.originalModel, entry.currentModel) {
            case (nil, .some(let model)):
                return PersistableMutationEffect(
                    kind: .insert,
                    identity: entry.identity,
                    model: model
                )
            case (.some, .some(let model)):
                return PersistableMutationEffect(
                    kind: .update,
                    identity: entry.identity,
                    model: model
                )
            case (.some, nil):
                return PersistableMutationEffect(
                    kind: .delete,
                    identity: entry.identity,
                    model: nil
                )
            case (nil, nil):
                return nil
            }
        }
    }

    private func lookupLocation(
        for identity: EntityReference
    ) -> (index: Int, found: Bool) {
        var lowerBound = 0
        var upperBound = lookup.count
        while lowerBound < upperBound {
            let middle = lowerBound + (upperBound - lowerBound) / 2
            if lookup[middle].identity < identity {
                lowerBound = middle + 1
            } else {
                upperBound = middle
            }
        }
        return (
            index: lowerBound,
            found: lowerBound < lookup.count
                && lookup[lowerBound].identity == identity
        )
    }
}
