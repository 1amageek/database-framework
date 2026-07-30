import DatabaseTypes

/// Owns the net mutations staged by one database context.
///
/// Entries remain in canonical identity order so capture, restore, and save
/// produce deterministic behavior on every supported runtime.
struct ContextPendingMutations: Sendable {
    private struct Entry: Sendable {
        let identity: EntityReference
        var mutation: DatabaseContext.PendingMutation
    }

    private var entries: [Entry] = []

    var count: Int {
        entries.count
    }

    var isEmpty: Bool {
        entries.isEmpty
    }

    subscript(identity: EntityReference) -> DatabaseContext.PendingMutation? {
        get {
            let location = location(for: identity)
            guard location.found else {
                return nil
            }
            return entries[location.index].mutation
        }
        set {
            let location = location(for: identity)
            if let newValue {
                if location.found {
                    entries[location.index].mutation = newValue
                } else {
                    entries.insert(
                        Entry(identity: identity, mutation: newValue),
                        at: location.index
                    )
                }
            } else if location.found {
                entries.remove(at: location.index)
            }
        }
    }

    @discardableResult
    mutating func remove(
        identifiedBy identity: EntityReference
    ) -> DatabaseContext.PendingMutation? {
        let location = location(for: identity)
        guard location.found else {
            return nil
        }
        return entries.remove(at: location.index).mutation
    }

    mutating func removeAll(keepingCapacity: Bool = false) {
        entries.removeAll(keepingCapacity: keepingCapacity)
    }

    func forEach(
        _ body: (EntityReference, DatabaseContext.PendingMutation) throws -> Void
    ) rethrows {
        for entry in entries {
            try body(entry.identity, entry.mutation)
        }
    }

    private func location(
        for identity: EntityReference
    ) -> (index: Int, found: Bool) {
        var lowerBound = 0
        var upperBound = entries.count
        while lowerBound < upperBound {
            let middle = lowerBound + (upperBound - lowerBound) / 2
            if entries[middle].identity < identity {
                lowerBound = middle + 1
            } else {
                upperBound = middle
            }
        }
        return (
            index: lowerBound,
            found: lowerBound < entries.count
                && entries[lowerBound].identity == identity
        )
    }
}
