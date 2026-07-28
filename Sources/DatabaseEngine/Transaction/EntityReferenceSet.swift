import DatabaseTypes

/// Maintains unique entity references in canonical order.
///
/// Canonical ordering gives every supported runtime the same membership
/// semantics without depending on platform hash-table specialization.
struct EntityReferenceSet: Sendable {
    private var references: [EntityReference]

    init(minimumCapacity: Int = 0) {
        references = []
        references.reserveCapacity(minimumCapacity)
    }

    var count: Int {
        references.count
    }

    func contains(_ reference: EntityReference) -> Bool {
        location(for: reference).found
    }

    @discardableResult
    mutating func insert(_ reference: EntityReference) -> Bool {
        let location = location(for: reference)
        guard !location.found else {
            return false
        }
        references.insert(reference, at: location.index)
        return true
    }

    @discardableResult
    mutating func remove(_ reference: EntityReference) -> Bool {
        let location = location(for: reference)
        guard location.found else {
            return false
        }
        references.remove(at: location.index)
        return true
    }

    mutating func removeAll(keepingCapacity: Bool = false) {
        references.removeAll(keepingCapacity: keepingCapacity)
    }

    private func location(
        for reference: EntityReference
    ) -> (index: Int, found: Bool) {
        var lowerBound = 0
        var upperBound = references.count
        while lowerBound < upperBound {
            let middle = lowerBound + (upperBound - lowerBound) / 2
            if references[middle] < reference {
                lowerBound = middle + 1
            } else {
                upperBound = middle
            }
        }
        return (
            index: lowerBound,
            found: lowerBound < references.count
                && references[lowerBound] == reference
        )
    }
}
