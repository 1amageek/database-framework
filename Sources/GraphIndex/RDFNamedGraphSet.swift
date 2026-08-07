import DatabaseKit

/// A canonical, immutable set of RDF named graphs.
///
/// Elements are sorted and deduplicated at the configuration boundary so
/// scan planning, hashing, and equality never depend on caller ordering.
public struct RDFNamedGraphSet: Sendable, Hashable, RandomAccessCollection {
    public typealias Element = RDFGraphName
    public typealias Index = Int

    private let storage: [RDFGraphName]

    public init(_ graphs: [RDFGraphName]) {
        let sorted = graphs.sorted()
        var canonical: [RDFGraphName] = []
        canonical.reserveCapacity(sorted.count)
        for graph in sorted where canonical.last != graph {
            canonical.append(graph)
        }
        self.storage = canonical
    }

    public var startIndex: Int { storage.startIndex }
    public var endIndex: Int { storage.endIndex }

    public subscript(position: Int) -> RDFGraphName {
        storage[position]
    }

    public func index(after index: Int) -> Int {
        storage.index(after: index)
    }

    public func index(before index: Int) -> Int {
        storage.index(before: index)
    }
}
