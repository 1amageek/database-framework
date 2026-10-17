/// Exact SQL identity without allocating a second owner for declaration bytes.
struct SQLCatalogName: Hashable, Sendable {
    let value: String

    init(_ value: String) { self.value = value }

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.value.utf8.elementsEqual(rhs.value.utf8)
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(value.utf8.count)
        for byte in value.utf8 { hasher.combine(byte) }
    }
}
