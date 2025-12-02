// FlatVectorIndexMaintainer.swift
// VectorIndexLayer - Flat scan vector index maintainer
//
// Provides exact nearest neighbor search using brute force linear scan.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for flat scan vector indexes
///
/// **Algorithm**: Brute force linear search
/// - Time: O(n) search
/// - Memory: O(n * d) (just vectors, no graph)
/// - Recall: 100% (exact)
/// - Best for: <10K vectors, development, low memory
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][primaryKey]
/// Value: Tuple(Float, Float, ..., Float)  // Vector dimensions
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = FlatVectorIndexMaintainer<Product>(
///     index: vectorIndex,
///     kind: VectorIndexKind(dimensions: 384, metric: .cosine),
///     subspace: vectorSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct FlatVectorIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let dimensions: Int
    private let metric: VectorMetric

    public init(
        index: Index,
        dimensions: Int,
        metric: VectorMetric,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.dimensions = dimensions
        self.metric = metric
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entry
        if let oldItem = oldItem {
            let oldKey = try buildIndexKey(for: oldItem)
            transaction.clear(key: oldKey)
        }

        // Add new index entry
        if let newItem = newItem {
            let newKey = try buildIndexKey(for: newItem)
            let value = try buildIndexValue(for: newItem)
            transaction.setValue(value, for: newKey)
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let indexKey = try buildIndexKey(for: item, id: id)
        let value = try buildIndexValue(for: item)
        transaction.setValue(value, for: indexKey)
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return [try buildIndexKey(for: item, id: id)]
    }

    /// Search for k nearest neighbors using linear scan
    ///
    /// **Algorithm**:
    /// 1. Scan all vectors in the index
    /// 2. Calculate distance to query vector
    /// 3. Keep top-k smallest distances using min-heap
    /// 4. Return sorted results
    ///
    /// **Performance**:
    /// - Time: O(n * d) where n = vectors, d = dimensions
    /// - Memory: O(k) for heap
    /// - Recall: 100% (exact)
    ///
    /// - Parameters:
    ///   - queryVector: Query vector (must match dimensions)
    ///   - k: Number of nearest neighbors to return
    ///   - transaction: FDB transaction
    /// - Returns: Array of (primaryKey, distance) sorted by distance ascending
    public func search(
        queryVector: [Float],
        k: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }

        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        // Scan all vectors
        let (begin, end) = subspace.range()
        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        var heap = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )

        for try await (key, value) in sequence {
            // Decode primary key - skip corrupt entries
            guard let primaryKeyTuple = try? subspace.unpack(key),
                  let primaryKey = try? Tuple.unpack(from: primaryKeyTuple.pack()) else {
                continue // Skip corrupt entry
            }

            // Decode vector - skip corrupt entries
            guard let vectorTuple = try? Tuple.unpack(from: value) else {
                continue // Skip corrupt entry
            }

            var vector: [Float] = []
            vector.reserveCapacity(dimensions)
            var isValid = true

            for i in 0..<dimensions {
                guard i < vectorTuple.count else {
                    isValid = false
                    break // Incomplete vector
                }

                let element = vectorTuple[i]
                let floatValue: Float
                if let f = element as? Float {
                    floatValue = f
                } else if let d = element as? Double {
                    floatValue = Float(d)
                } else if let i64 = element as? Int64 {
                    floatValue = Float(i64)
                } else if let i = element as? Int {
                    floatValue = Float(i)
                } else {
                    isValid = false
                    break // Invalid element type
                }

                vector.append(floatValue)
            }

            guard isValid else { continue } // Skip invalid vector

            // Calculate distance
            let distance = calculateDistance(queryVector, vector)

            // Insert into heap
            heap.insert((primaryKey: primaryKey, distance: distance))
        }

        return heap.sorted()
    }

    // MARK: - Private Methods

    /// Build index key using only primary key
    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> [UInt8] {
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        let key = subspace.pack(primaryKeyTuple)
        try validateKeySize(key)
        return key
    }

    /// Build index value containing the vector data
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildIndexValue(for item: Item) throws -> [UInt8] {
        // Evaluate expression using optimized DataAccess method
        // Uses KeyPath direct extraction when available, falls back to KeyExpression
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        // Convert to Float array
        var floatArray: [Float] = []
        for element in fieldValues {
            if let array = element as? [Float] {
                floatArray.append(contentsOf: array)
            } else if let array = element as? [Float32] {
                floatArray.append(contentsOf: array.map { Float($0) })
            } else if let array = element as? [Double] {
                floatArray.append(contentsOf: array.map { Float($0) })
            } else if let f = element as? Float {
                floatArray.append(f)
            } else if let d = element as? Double {
                floatArray.append(Float(d))
            } else {
                throw VectorIndexError.invalidArgument(
                    "Vector field must contain numeric values, got: \(type(of: element))"
                )
            }
        }

        // Validate dimensions
        guard floatArray.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: floatArray.count
            )
        }

        // Encode as tuple
        let tupleElements: [any TupleElement] = floatArray.map { $0 as any TupleElement }
        let tuple = Tuple(tupleElements)
        return tuple.pack()
    }

    /// Calculate distance between two vectors
    private func calculateDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        switch metric {
        case .cosine:
            return cosineDistance(v1, v2)
        case .euclidean:
            return euclideanDistance(v1, v2)
        case .dotProduct:
            return dotProductDistance(v1, v2)
        }
    }

    /// Cosine distance: 1 - cosine_similarity
    private func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        let norm1 = sqrt(v1.map { Double($0) * Double($0) }.reduce(0, +))
        let norm2 = sqrt(v2.map { Double($0) * Double($0) }.reduce(0, +))

        guard norm1 > 0 && norm2 > 0 else {
            return 2.0  // Maximum distance for zero vectors
        }

        let cosineSimilarity = dotProduct / (norm1 * norm2)
        return 1.0 - cosineSimilarity
    }

    /// Euclidean distance
    private func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        var sum: Double = 0.0
        for (a, b) in zip(v1, v2) {
            let diff = Double(a) - Double(b)
            sum += diff * diff
        }
        return sqrt(sum)
    }

    /// Dot product distance: -dot_product
    private func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        return -dotProduct
    }
}

// MARK: - Vector Index Errors

/// Errors specific to vector index operations
public enum VectorIndexError: Error, CustomStringConvertible {
    case dimensionMismatch(expected: Int, actual: Int)
    case invalidArgument(String)
    case invalidStructure(String)
    case graphNotBuilt
    case graphTooLarge(maxLevel: Int)

    public var description: String {
        switch self {
        case .dimensionMismatch(let expected, let actual):
            return "Vector dimension mismatch. Expected: \(expected), Got: \(actual)"
        case .invalidArgument(let message):
            return "Invalid argument: \(message)"
        case .invalidStructure(let message):
            return "Invalid index structure: \(message)"
        case .graphNotBuilt:
            return "HNSW graph not built yet"
        case .graphTooLarge(let maxLevel):
            return "HNSW graph has grown beyond inline indexing capacity (maxLevel: \(maxLevel)). Use batch indexing instead."
        }
    }
}

// MARK: - BinaryHeap

/// Binary heap for k-NN search with O(log k) operations
///
/// This is a proper heap implementation with consistent semantics:
/// - `top`: Returns the root element (smallest for min-heap, largest for max-heap)
/// - `pop()`: Removes and returns the root element
/// - `insert()`: Adds element and maintains heap property in O(log k)
///
/// For k-NN search:
/// - Use min-heap (comparator: `<`) for candidates - pop closest first
/// - Use max-heap (comparator: `>`) for results - track k best, evict worst
internal struct BinaryHeap<Element> {
    private var elements: [Element] = []
    private let maxSize: Int
    private let comparator: (Element, Element) -> Bool

    /// Create a binary heap
    /// - Parameters:
    ///   - maxSize: Maximum number of elements (use Int.max for unbounded)
    ///   - comparator: Returns true if first element should be closer to root
    ///                 Use `<` for min-heap, `>` for max-heap
    init(maxSize: Int = Int.max, comparator: @escaping (Element, Element) -> Bool) {
        self.maxSize = maxSize
        self.comparator = comparator
    }

    var count: Int { elements.count }
    var isEmpty: Bool { elements.isEmpty }
    var isFull: Bool { elements.count >= maxSize }

    /// Returns the root element without removing it
    var top: Element? { elements.first }

    /// Insert element maintaining heap property - O(log k)
    mutating func insert(_ element: Element) {
        if elements.count < maxSize {
            elements.append(element)
            siftUp(elements.count - 1)
        } else if let rootElement = elements.first, comparator(element, rootElement) {
            // Element should be closer to root than current root
            // For max-heap with distance: new element has larger distance, don't insert
            // For min-heap with distance: new element has smaller distance, replace root
            // Actually for bounded k-NN with max-heap:
            // - We want to keep k smallest distances
            // - Root is the largest distance among k
            // - If new distance < root distance, replace root
            // So comparator should be > for max-heap, and we check !comparator
        } else if let rootElement = elements.first, !comparator(rootElement, element) && rootElement as AnyObject !== element as AnyObject {
            // For bounded heap: replace root if new element should be in heap
            // This logic depends on use case - see replaceRoot method below
        }
    }

    /// Insert element, replacing root if heap is full and element qualifies
    /// For k-NN max-heap: replaces if new distance < root distance
    mutating func insertBounded(_ element: Element, shouldReplace: (Element, Element) -> Bool) {
        if elements.count < maxSize {
            elements.append(element)
            siftUp(elements.count - 1)
        } else if let root = elements.first, shouldReplace(element, root) {
            elements[0] = element
            siftDown(0)
        }
    }

    /// Remove and return the root element - O(log k)
    mutating func pop() -> Element? {
        guard !elements.isEmpty else { return nil }
        if elements.count == 1 {
            return elements.removeLast()
        }
        let result = elements[0]
        elements[0] = elements.removeLast()
        siftDown(0)
        return result
    }

    /// Returns all elements sorted (closest to root first)
    func toSortedArray() -> [Element] {
        return elements.sorted(by: comparator)
    }

    /// Returns all elements sorted in reverse order (farthest from root first)
    func toReverseSortedArray() -> [Element] {
        return elements.sorted { !comparator($0, $1) }
    }

    // MARK: - Private Heap Operations

    private mutating func siftUp(_ index: Int) {
        var i = index
        while i > 0 {
            let parent = (i - 1) / 2
            if comparator(elements[i], elements[parent]) {
                elements.swapAt(i, parent)
                i = parent
            } else {
                break
            }
        }
    }

    private mutating func siftDown(_ index: Int) {
        var i = index
        while true {
            let left = 2 * i + 1
            let right = 2 * i + 2
            var smallest = i

            if left < elements.count && comparator(elements[left], elements[smallest]) {
                smallest = left
            }
            if right < elements.count && comparator(elements[right], elements[smallest]) {
                smallest = right
            }

            if smallest == i {
                break
            }

            elements.swapAt(i, smallest)
            i = smallest
        }
    }
}

// MARK: - Specialized Heaps for k-NN

/// Min-heap for candidate exploration (pop smallest distance first)
internal struct CandidateHeap<PK> {
    private var heap: BinaryHeap<(primaryKey: PK, distance: Double)>

    init() {
        heap = BinaryHeap { $0.distance < $1.distance }
    }

    var isEmpty: Bool { heap.isEmpty }
    var count: Int { heap.count }

    /// Returns candidate with smallest distance
    var top: (primaryKey: PK, distance: Double)? { heap.top }

    mutating func insert(_ candidate: (primaryKey: PK, distance: Double)) {
        heap.insertBounded(candidate) { _, _ in true }  // Unbounded
    }

    /// Remove and return candidate with smallest distance
    mutating func pop() -> (primaryKey: PK, distance: Double)? {
        heap.pop()
    }
}

/// Max-heap for tracking k-best results (keeps k smallest distances)
internal struct ResultHeap<PK> {
    private var heap: BinaryHeap<(primaryKey: PK, distance: Double)>
    private let maxSize: Int

    init(k: Int) {
        self.maxSize = k
        heap = BinaryHeap(maxSize: k) { $0.distance > $1.distance }
    }

    var isEmpty: Bool { heap.isEmpty }
    var count: Int { heap.count }
    var isFull: Bool { heap.count >= maxSize }

    /// Returns result with largest distance (worst in top-k)
    var worst: (primaryKey: PK, distance: Double)? { heap.top }

    mutating func insert(_ result: (primaryKey: PK, distance: Double)) {
        heap.insertBounded(result) { new, root in
            // Replace root if new distance is smaller (better)
            new.distance < root.distance
        }
    }

    /// Returns results sorted by distance ascending (best first)
    func toSortedArray() -> [(primaryKey: PK, distance: Double)] {
        heap.toReverseSortedArray()
    }
}

// MARK: - Legacy MinHeap (deprecated, use CandidateHeap/ResultHeap instead)

/// Min-heap for k-NN search
///
/// **DEPRECATED**: This type has confusing semantics. Use these instead:
/// - `CandidateHeap<PK>`: For exploring candidates (pop smallest distance)
/// - `ResultHeap<PK>`: For tracking k-best results (keeps k smallest distances)
///
/// **Legacy behavior**:
/// - With `heapType: .max` and `comparator: >`, this acts as a max-heap
/// - `min`/`top` returns root (largest element for max-heap)
/// - `removeMin()`/`pop()` removes and returns root
///
/// **Bounded insertion for k-NN**:
/// For max-heap (comparator: `>`): replaces root when new element should NOT be at root
/// (i.e., when new < root, meaning new is better for k-NN tracking)
internal struct MinHeap<Element> {
    private var heap: BinaryHeap<Element>
    private let maxSize: Int
    private let comparator: (Element, Element) -> Bool

    enum HeapType {
        case min
        case max
    }

    init(maxSize: Int, heapType: HeapType, comparator: @escaping (Element, Element) -> Bool) {
        self.maxSize = maxSize
        self.heap = BinaryHeap(maxSize: maxSize, comparator: comparator)
        self.comparator = comparator
    }

    var count: Int { heap.count }
    var isEmpty: Bool { heap.isEmpty }
    var isFull: Bool { heap.isFull }

    /// Returns the root element
    var min: Element? { heap.top }

    /// Returns the root element
    var top: Element? { heap.top }

    /// Returns the maximum element (alias for root in max-heap configuration)
    var max: Element? { heap.top }

    mutating func insert(_ element: Element) {
        // For bounded k-NN max-heap:
        // - Root is the "worst" element (largest distance)
        // - Replace root when new element should be in heap instead
        // - With comparator `>`: root has max value, replace when new < root
        // - !comparator(new, root) means "new is NOT > root" = "new <= root"
        // - comparator(root, new) means "root > new"
        // So replace when root > new (new is better)
        heap.insertBounded(element) { new, root in
            comparator(root, new)  // Replace if root "beats" new in the wrong direction
        }
    }

    /// Remove and return the root element
    mutating func removeMin() -> Element? {
        heap.pop()
    }

    /// Remove and return the root element
    mutating func pop() -> Element? {
        heap.pop()
    }

    func sorted() -> [Element] {
        heap.toReverseSortedArray()
    }
}
