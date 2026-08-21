import DatabaseTypes

/// Maintains unique entity references for transaction membership checks.
///
/// Iteration order is intentionally not exposed. Every caller consumes only
/// membership and insertion/removal results, allowing batch insertion to stay
/// expected O(1) instead of shifting a canonically sorted Array.
struct EntityReferenceSet: Sendable {
    private var references: Set<EntityReference>

    init(minimumCapacity: Int = 0) {
        references = Set(minimumCapacity: minimumCapacity)
    }

    var count: Int {
        references.count
    }

    func contains(_ reference: EntityReference) -> Bool {
        references.contains(reference)
    }

    @discardableResult
    mutating func insert(_ reference: EntityReference) -> Bool {
        references.insert(reference).inserted
    }

    @discardableResult
    mutating func remove(_ reference: EntityReference) -> Bool {
        references.remove(reference) != nil
    }

    mutating func removeAll(keepingCapacity: Bool = false) {
        references.removeAll(keepingCapacity: keepingCapacity)
    }
}
