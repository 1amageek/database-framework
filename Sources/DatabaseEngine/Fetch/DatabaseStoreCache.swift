/// Keeps resolved database stores ordered by their semantic directory key.
///
/// Store resolution normally touches a small number of entities and partitions
/// per transaction. Binary lookup keeps that path deterministic without relying
/// on hash-table storage whose cross-module specialization is not reliable in
/// the supported standard-WASI toolchain.
struct DatabaseStoreCache<Value: Sendable>: Sendable {
    private struct Entry: Sendable {
        let key: DatabaseStoreCacheKey
        var value: Value
    }

    private var entries: [Entry] = []

    var count: Int {
        entries.count
    }

    func value(for key: DatabaseStoreCacheKey) -> Value? {
        let location = location(for: key)
        guard location.found else {
            return nil
        }
        return entries[location.index].value
    }

    mutating func insert(
        _ value: Value,
        for key: DatabaseStoreCacheKey
    ) {
        let location = location(for: key)
        if location.found {
            entries[location.index].value = value
        } else {
            entries.insert(
                Entry(key: key, value: value),
                at: location.index
            )
        }
    }

    private func location(
        for key: DatabaseStoreCacheKey
    ) -> (index: Int, found: Bool) {
        var lowerBound = 0
        var upperBound = entries.count
        while lowerBound < upperBound {
            let middle = lowerBound + (upperBound - lowerBound) / 2
            let candidate = entries[middle].key
            if candidate < key {
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
