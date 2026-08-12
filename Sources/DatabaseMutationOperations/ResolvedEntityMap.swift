import DatabaseQueryOperations
import DatabaseOperationCore
/// Stores one value for each complete resolved entity identity.
///
/// Canonical ordering makes lookup behavior deterministic on every runtime and
/// avoids relying on platform hash-table specialization at the WASI boundary.
struct ResolvedEntityMap<Value: Sendable>: Sendable {
    private struct Entry: Sendable {
        let key: ResolvedEntityReference.Key
        var value: Value
    }

    private var entries: [Entry] = []

    var count: Int {
        entries.count
    }

    func value(
        for key: ResolvedEntityReference.Key
    ) -> Value? {
        let location = location(for: key)
        guard location.found else {
            return nil
        }
        return entries[location.index].value
    }

    @discardableResult
    mutating func insert(
        _ value: Value,
        for key: ResolvedEntityReference.Key
    ) -> Bool {
        let location = location(for: key)
        if location.found {
            entries[location.index].value = value
            return false
        }
        entries.insert(
            Entry(key: key, value: value),
            at: location.index
        )
        return true
    }

    private func location(
        for key: ResolvedEntityReference.Key
    ) -> (index: Int, found: Bool) {
        var lowerBound = 0
        var upperBound = entries.count
        while lowerBound < upperBound {
            let middle = lowerBound + (upperBound - lowerBound) / 2
            if entries[middle].key < key {
                lowerBound = middle + 1
            } else {
                upperBound = middle
            }
        }
        return (
            index: lowerBound,
            found: lowerBound < entries.count
                && entries[lowerBound].key == key
        )
    }
}
