// IndexKindMaintainable.swift
// FDBIndexing - Bridges IndexKind (metadata) with IndexMaintainer (runtime)
//
// IndexMaintainer implementors are responsible for adding IndexKindMaintainable
// conformance to their corresponding IndexKind.

import FoundationDB
import Core

/// Protocol that bridges IndexKind (metadata) with IndexMaintainer (runtime)
///
/// **Responsibility**: IndexMaintainer implementors MUST add this conformance to their IndexKind.
///
/// **Design**:
/// ```
/// IndexKind (FDBModel)          IndexMaintainer (FDBIndexing)
///       ↓                              ↓
/// ScalarIndexKind              ScalarIndexMaintainer
///       ↓                              ↓
///       └──── IndexKindMaintainable ───┘
///             (implementor bridges them)
/// ```
///
/// **How it works**:
/// 1. User defines `#Index([\.email], type: ScalarIndexKind())` in model
/// 2. `@Persistable` macro generates `IndexDescriptor` with the `IndexKind`
/// 3. At runtime, system casts `IndexKind` to `IndexKindMaintainable`
/// 4. `makeIndexMaintainer()` creates the appropriate `IndexMaintainer`
///
/// **Standard IndexKinds**: Implementors provide `IndexKindMaintainable` conformance for:
/// - `ScalarIndexKind` → `ScalarIndexMaintainer`
/// - `CountIndexKind` → `CountIndexMaintainer`
/// - `SumIndexKind` → `SumIndexMaintainer`
/// - `MinIndexKind` / `MaxIndexKind` → `MinMaxIndexMaintainer`
/// - `AverageIndexKind` → `AverageIndexMaintainer`
/// - `VersionIndexKind` → `VersionIndexMaintainer`
///
/// **Third-Party Extension**:
/// ```swift
/// // 1. Define your IndexKind (in your FDB-independent module)
/// public struct VectorIndexKind: IndexKind {
///     public static let identifier = "vector"
///     public static let subspaceStructure = SubspaceStructure.hierarchical
///     // ...
/// }
///
/// // 2. Define your IndexMaintainer (in your FDB-dependent module)
/// public struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer {
///     // ...
/// }
///
/// // 3. Bridge them with IndexKindMaintainable (implementor's responsibility)
/// extension VectorIndexKind: IndexKindMaintainable {
///     public func makeIndexMaintainer<Item: Persistable>(
///         index: Index,
///         subspace: Subspace,
///         idExpression: KeyExpression,
///         configurations: [any IndexConfiguration]
///     ) -> any IndexMaintainer<Item> {
///         // Find matching configuration for this index
///         let config = configurations.first { $0.indexName == index.name }
///         if let vectorConfig = config as? VectorIndexConfiguration<Item> {
///             return HNSWIndexMaintainer<Item>(
///                 index: index,
///                 subspace: subspace,
///                 parameters: vectorConfig.hnswParameters
///             )
///         }
///         // Default to flat index if no config
///         return FlatVectorIndexMaintainer<Item>(index: index, subspace: subspace)
///     }
/// }
/// ```
public protocol IndexKindMaintainable: IndexKind {
    /// Create an IndexMaintainer for this IndexKind
    ///
    /// **Implementor's Responsibility**: This method bridges the IndexKind metadata
    /// with the concrete IndexMaintainer implementation.
    ///
    /// - Parameters:
    ///   - index: Index definition (name, rootExpression, etc.)
    ///   - subspace: FDB subspace for storing index data
    ///   - idExpression: KeyExpression for extracting item's unique identifier
    ///   - configurations: Index configurations from FDBContainer (may contain runtime parameters)
    /// - Returns: IndexMaintainer instance for maintaining this index
    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item>
}
