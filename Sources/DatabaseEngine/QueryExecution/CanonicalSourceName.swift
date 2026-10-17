import DatabaseKit

/// SQL source identity retains existing UTF-8 storage without normalization.
struct CanonicalSourceName: Sendable, Hashable {
    let value: String
    init(_ value: String) { self.value = value }
    static func == (left: Self, right: Self) -> Bool {
        left.value.utf8.elementsEqual(right.value.utf8)
    }
    func hash(into hasher: inout Hasher) {
        hasher.combine(value.utf8.count)
        for byte in value.utf8 { hasher.combine(byte) }
    }
    static func equal(_ left: [String], _ right: [String]) -> Bool {
        left.count == right.count && zip(left, right).allSatisfy { pair in equal(pair.0, pair.1) }
    }
    static func equal(_ left: String?, _ right: String?) -> Bool {
        switch (left, right) {
        case (nil, nil): return true
        case (.some(let left), .some(let right)): return left.utf8.elementsEqual(right.utf8)
        default: return false
        }
    }
}

/// Set membership for logical SQL names; published names remain Strings.
struct CanonicalSourceNames: Sendable, Equatable, Sequence, ExpressibleByArrayLiteral {
    private var keys: Set<CanonicalSourceName>
    init() { keys = [] }
    init<S: Sequence>(_ values: S) where S.Element == String { keys = Set(values.map(CanonicalSourceName.init)) }
    init(arrayLiteral elements: String...) { self.init(elements) }
    var count: Int { keys.count }
    var isEmpty: Bool { keys.isEmpty }
    func contains(_ name: String) -> Bool { keys.contains(CanonicalSourceName(name)) }
    struct Iterator: IteratorProtocol {
        var values: Set<CanonicalSourceName>.Iterator
        mutating func next() -> String? { values.next()?.value }
    }
    func makeIterator() -> Iterator { Iterator(values: keys.makeIterator()) }
    @discardableResult mutating func insert(_ name: String) -> (inserted: Bool, memberAfterInsert: String) {
        let result = keys.insert(CanonicalSourceName(name))
        return (result.inserted, result.memberAfterInsert.value)
    }
    mutating func formUnion<S: Sequence>(_ values: S) where S.Element == String { for value in values { insert(value) } }
    func union<S: Sequence>(_ names: S) -> Self where S.Element == String {
        var result = self; result.formUnion(names); return result
    }
    func intersection(_ other: Self) -> Self {
        var result = self; result.keys.formIntersection(other.keys); return result
    }
    func subtracting(_ other: Self) -> Self {
        var result = self; result.keys.subtract(other.keys); return result
    }
    func intersection<S: Sequence>(_ names: S) -> Self where S.Element == String {
        let other = Self(names); return Self(filter(other.contains))
    }
    func subtracting<S: Sequence>(_ names: S) -> Self where S.Element == String {
        let other = Self(names); return Self(filter { !other.contains($0) })
    }
    func isSubset<S: Sequence>(of names: S) -> Bool where S.Element == String {
        let other = Self(names); return allSatisfy(other.contains)
    }
}

struct SQLCatalogSourceName: Sendable, Hashable {
    let namespace: CanonicalSourceName?
    let name: CanonicalSourceName
    init(_ table: TableRef) {
        namespace = table.schema.map(CanonicalSourceName.init)
        name = CanonicalSourceName(table.table)
    }
}
