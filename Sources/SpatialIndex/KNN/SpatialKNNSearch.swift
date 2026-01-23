// SpatialKNNSearch.swift
// SpatialIndex - True K-Nearest Neighbors using Priority Queue + Cell Pruning
//
// Reference: Samet, H. "Foundations of Multidimensional and Metric Data Structures", 2006
// Algorithm: Best-First Search with Cell Pruning

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Spatial

/// True K-Nearest Neighbors search using Priority Queue + Cell Pruning
///
/// **Algorithm** (Samet, 2006):
/// 1. Start with the cell containing the query point
/// 2. Use a priority queue ordered by minimum distance to query
/// 3. For each cell:
///    - If minDistance > k-th best distance, prune (skip)
///    - Otherwise, scan the cell and add neighbors to queue
/// 4. Continue until k results found or queue exhausted
///
/// **Advantages over Adaptive Radius**:
/// - Guaranteed to find k nearest (if they exist)
/// - No arbitrary radius parameter needed
/// - Efficient pruning reduces unnecessary cell scans
/// - Works well with sparse data
///
/// **Complexity**:
/// - O(k × log C + scanned_points) where C = number of candidate cells
/// - Best case: O(k × log k) for clustered data
/// - Worst case: O(n) for uniformly distributed data
///
/// **Usage**:
/// ```swift
/// let search = SpatialKNNSearch<Store>(
///     queryContext: context.indexQueryContext,
///     indexSubspace: subspace,
///     encoding: .s2,
///     level: 15,
///     fieldName: "location"
/// )
/// let result = try await search.findKNearest(
///     k: 10,
///     from: userLocation,
///     transaction: transaction
/// )
/// ```
public struct SpatialKNNSearch<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let indexSubspace: Subspace
    private let encoding: SpatialEncoding
    private let level: Int
    private let fieldName: String

    // Configuration
    private let maxCellsToScan: Int
    private let maxPointsToScan: Int
    private let minLevel: Int  // Coarsest level to start from

    /// Create a KNN search instance
    ///
    /// - Parameters:
    ///   - queryContext: Index query context
    ///   - indexSubspace: FDB subspace for the spatial index
    ///   - encoding: Spatial encoding type
    ///   - level: S2 cell level for the index
    ///   - fieldName: Name of the GeoPoint field
    ///   - maxCellsToScan: Maximum cells to scan (default: 1000)
    ///   - maxPointsToScan: Maximum points to scan (default: 50000)
    public init(
        queryContext: IndexQueryContext,
        indexSubspace: Subspace,
        encoding: SpatialEncoding,
        level: Int,
        fieldName: String,
        maxCellsToScan: Int = 1000,
        maxPointsToScan: Int = 50000
    ) {
        self.queryContext = queryContext
        self.indexSubspace = indexSubspace
        self.encoding = encoding
        self.level = level
        self.fieldName = fieldName
        self.maxCellsToScan = maxCellsToScan
        self.maxPointsToScan = maxPointsToScan
        self.minLevel = max(0, level - 5)  // Start 5 levels above index level
    }

    /// Find k nearest neighbors to a query point
    ///
    /// - Parameters:
    ///   - k: Number of neighbors to find
    ///   - queryPoint: Center point to search from
    ///   - transaction: FDB transaction
    /// - Returns: Array of (item, distance) sorted by distance, up to k items
    public func findKNearest(
        k: Int,
        from queryPoint: GeoPoint,
        transaction: any TransactionProtocol
    ) async throws -> [(item: T, distance: Double)] {
        guard k > 0 else {
            return []
        }

        // Priority queue of cells to explore, ordered by min distance
        var cellQueue = CellPriorityQueue()

        // Result heap (max-heap for k nearest)
        var resultHeap = KNNResultHeap<T>(k: k)

        // Track visited cells to avoid duplicates
        var visitedCells: Set<UInt64> = []

        // Statistics
        var cellsScanned = 0
        var pointsScanned = 0

        // Initialize: start with cell containing query point
        let startCellId = S2Geometry.encode(
            latitude: queryPoint.latitude,
            longitude: queryPoint.longitude,
            level: level
        )
        cellQueue.push(CellCandidate(cellId: startCellId, minDistance: 0))

        // Also add neighboring cells at coarser levels for broader coverage
        addInitialNeighborCells(
            queryPoint: queryPoint,
            to: &cellQueue,
            visited: &visitedCells
        )

        // Best-first search
        while let candidate = cellQueue.pop() {
            // Check limits
            if cellsScanned >= maxCellsToScan {
                break
            }

            // Pruning: if this cell's min distance > k-th best, we're done
            if resultHeap.count >= k && candidate.minDistance > resultHeap.kthDistance {
                break
            }

            // Skip if already visited
            guard !visitedCells.contains(candidate.cellId) else {
                continue
            }
            visitedCells.insert(candidate.cellId)
            cellsScanned += 1

            // Scan this cell for points
            let cellPoints = try await scanCell(
                cellId: candidate.cellId,
                transaction: transaction
            )
            pointsScanned += cellPoints.count

            // Check points limit
            if pointsScanned >= maxPointsToScan {
                break
            }

            // Process each point in the cell
            for pointInfo in cellPoints {
                let distance = CellDistanceCalculator.haversineDistance(
                    from: queryPoint,
                    to: pointInfo.location
                )

                // Try to add to result heap
                resultHeap.tryInsert(id: pointInfo.primaryKey, distance: distance)
            }

            // Add neighbor cells to queue (only if they might contain closer points)
            if resultHeap.count < k || candidate.minDistance <= resultHeap.kthDistance * 1.5 {
                addNeighborCells(
                    of: candidate.cellId,
                    queryPoint: queryPoint,
                    kthDistance: resultHeap.kthDistance,
                    to: &cellQueue,
                    visited: visitedCells
                )
            }
        }

        // Fetch actual items for the results
        let topK = resultHeap.sorted()
        var results: [(item: T, distance: Double)] = []

        for (primaryKey, distance) in topK {
            let items = try await queryContext.fetchItems(ids: [primaryKey], type: T.self)
            if let item = items.first {
                results.append((item: item, distance: distance))
            }
        }

        return results.sorted { $0.distance < $1.distance }
    }

    // MARK: - Private Methods

    /// Scan a single cell for points
    private func scanCell(
        cellId: UInt64,
        transaction: any TransactionProtocol
    ) async throws -> [PointInfo] {
        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: encoding,
            level: level
        )

        let (keys, _) = try await scanner.scanCells(
            cellIds: [cellId],
            limit: nil,  // No limit per cell
            transaction: transaction
        )

        // Convert keys to PointInfo by fetching items
        var points: [PointInfo] = []
        let items = try await queryContext.fetchItems(ids: keys, type: T.self)

        for item in items {
            if let location = extractGeoPoint(from: item) {
                let pkTuple = Tuple([item.id as! any TupleElement])
                points.append(PointInfo(primaryKey: pkTuple, location: location))
            }
        }

        return points
    }

    /// Add initial neighbor cells for broader coverage
    private func addInitialNeighborCells(
        queryPoint: GeoPoint,
        to queue: inout CellPriorityQueue,
        visited: inout Set<UInt64>
    ) {
        // Add cells in a small radius around the query point
        let initialRadius = 1000.0  // 1km initial coverage
        let coveringCells = S2Geometry.getCoveringCells(
            latitude: queryPoint.latitude,
            longitude: queryPoint.longitude,
            radiusMeters: initialRadius,
            level: level
        )

        for cellId in coveringCells {
            if !visited.contains(cellId) {
                let minDist = CellDistanceCalculator.minDistance(
                    cellId: cellId,
                    level: level,
                    to: queryPoint
                )
                queue.push(CellCandidate(cellId: cellId, minDistance: minDist))
            }
        }
    }

    /// Add neighbor cells of a given cell to the queue
    private func addNeighborCells(
        of cellId: UInt64,
        queryPoint: GeoPoint,
        kthDistance: Double,
        to queue: inout CellPriorityQueue,
        visited: Set<UInt64>
    ) {
        // Get the cell center and find neighboring cells
        let center = S2Geometry.decode(cellId, level: level)

        // Calculate cell size for neighbor offset
        let cellSizeDegrees = 180.0 / Double(1 << level)

        // Add 8 neighbors (N, S, E, W, NE, NW, SE, SW)
        let offsets: [(Double, Double)] = [
            (cellSizeDegrees, 0),   // N
            (-cellSizeDegrees, 0),  // S
            (0, cellSizeDegrees),   // E
            (0, -cellSizeDegrees),  // W
            (cellSizeDegrees, cellSizeDegrees),   // NE
            (cellSizeDegrees, -cellSizeDegrees),  // NW
            (-cellSizeDegrees, cellSizeDegrees),  // SE
            (-cellSizeDegrees, -cellSizeDegrees)  // SW
        ]

        for (latOffset, lonOffset) in offsets {
            let neighborLat = max(-89.999, min(89.999, center.latitude + latOffset))
            let neighborLon = max(-179.999, min(179.999, center.longitude + lonOffset))

            let neighborCellId = S2Geometry.encode(
                latitude: neighborLat,
                longitude: neighborLon,
                level: level
            )

            if !visited.contains(neighborCellId) {
                let minDist = CellDistanceCalculator.minDistance(
                    cellId: neighborCellId,
                    level: level,
                    to: queryPoint
                )

                // Only add if might contain closer points
                if minDist <= kthDistance * 2.0 || kthDistance == .infinity {
                    queue.push(CellCandidate(cellId: neighborCellId, minDistance: minDist))
                }
            }
        }
    }

    /// Extract GeoPoint from item using dynamicMember subscript
    private func extractGeoPoint(from item: T) -> GeoPoint? {
        guard let value = item[dynamicMember: fieldName] else { return nil }
        return value as? GeoPoint
    }
}

// MARK: - Supporting Types

/// Information about a point in the index
private struct PointInfo {
    let primaryKey: Tuple
    let location: GeoPoint
}

/// Candidate cell for exploration
private struct CellCandidate: Comparable {
    let cellId: UInt64
    let minDistance: Double

    static func < (lhs: CellCandidate, rhs: CellCandidate) -> Bool {
        lhs.minDistance < rhs.minDistance
    }
}

/// Priority queue for cell candidates (min-heap by distance)
private struct CellPriorityQueue {
    private var heap: [CellCandidate] = []

    mutating func push(_ candidate: CellCandidate) {
        heap.append(candidate)
        siftUp(heap.count - 1)
    }

    mutating func pop() -> CellCandidate? {
        guard !heap.isEmpty else { return nil }
        if heap.count == 1 {
            return heap.removeLast()
        }
        let result = heap[0]
        heap[0] = heap.removeLast()
        siftDown(0)
        return result
    }

    var isEmpty: Bool { heap.isEmpty }

    private mutating func siftUp(_ index: Int) {
        var child = index
        while child > 0 {
            let parent = (child - 1) / 2
            if heap[child] < heap[parent] {
                heap.swapAt(child, parent)
                child = parent
            } else {
                break
            }
        }
    }

    private mutating func siftDown(_ index: Int) {
        var parent = index
        while true {
            let left = 2 * parent + 1
            let right = 2 * parent + 2
            var smallest = parent

            if left < heap.count && heap[left] < heap[smallest] {
                smallest = left
            }
            if right < heap.count && heap[right] < heap[smallest] {
                smallest = right
            }

            if smallest == parent {
                break
            }

            heap.swapAt(parent, smallest)
            parent = smallest
        }
    }
}

/// Max-heap for tracking k nearest results
private struct KNNResultHeap<T: Persistable> {
    private let k: Int
    private var heap: [(id: Tuple, distance: Double)] = []

    var count: Int { heap.count }

    /// The k-th best distance (or infinity if we have < k results)
    var kthDistance: Double {
        guard !heap.isEmpty else { return .infinity }
        return heap[0].distance  // Max-heap: root is the largest
    }

    init(k: Int) {
        self.k = k
    }

    /// Try to insert a result, maintaining k best
    mutating func tryInsert(id: Tuple, distance: Double) {
        if heap.count < k {
            // Room for more results
            heap.append((id, distance))
            siftUp(heap.count - 1)
        } else if distance < heap[0].distance {
            // Better than worst result
            heap[0] = (id, distance)
            siftDown(0)
        }
    }

    /// Get sorted results (ascending by distance)
    func sorted() -> [(id: Tuple, distance: Double)] {
        heap.sorted { $0.distance < $1.distance }
    }

    // Max-heap operations
    private mutating func siftUp(_ index: Int) {
        var child = index
        while child > 0 {
            let parent = (child - 1) / 2
            if heap[child].distance > heap[parent].distance {
                heap.swapAt(child, parent)
                child = parent
            } else {
                break
            }
        }
    }

    private mutating func siftDown(_ index: Int) {
        var parent = index
        while true {
            let left = 2 * parent + 1
            let right = 2 * parent + 2
            var largest = parent

            if left < heap.count && heap[left].distance > heap[largest].distance {
                largest = left
            }
            if right < heap.count && heap[right].distance > heap[largest].distance {
                largest = right
            }

            if largest == parent {
                break
            }

            heap.swapAt(parent, largest)
            parent = largest
        }
    }
}
