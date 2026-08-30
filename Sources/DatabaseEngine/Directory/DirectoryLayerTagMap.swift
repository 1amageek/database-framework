import DatabaseKit

/// Derives the layer tag of every Directory node position from the complete set
/// of `#Directory` declarations in a schema.
///
/// A declaration assigns a layer only to its own leaf. The layer of a node
/// position is therefore a property of the whole schema rather than of one
/// declaration: a position that some declaration resolves as a `.partition` leaf
/// is a Partition for every declaration that passes through it, and a position
/// that no declaration resolves as a leaf is a plain Directory. The map performs
/// that derivation once, at bootstrap, over component *shapes* rather than
/// concrete dynamic values, so it does not depend on stored data.
///
/// `Sources/DatabaseEngine/Directory/DESIGN.md` owns the rules this type
/// enforces and the reasons it rejects a schema.
package struct DirectoryLayerTagMap: Sendable {

    /// Derived layers per entity, one element per declared component, in
    /// declaration order.
    private let layersByEntityName: [String: [DirectoryLayer]]

    /// The derivation trie, retained so a literal path that no entity declares
    /// as a whole can still be typed component by component. A polymorphic
    /// group addresses such a path: it declares no entity of its own, yet its
    /// components may coincide with positions entities resolve.
    private let positions: [Position]

    /// Derives the map, or rejects the schema with the first disagreement found.
    ///
    /// Both kinds of `#Directory` declaration are inserted: an entity resolves
    /// the directory holding its records, and a polymorphic group resolves the
    /// directory holding the projection shared by its members. A group leaf
    /// therefore assigns a layer exactly as an entity leaf does, so a group that
    /// declares `.partition` is created as a Partition and a group that
    /// disagrees with an entity about one position is rejected here rather than
    /// silently resolved as a plain Directory.
    ///
    /// Entities are visited in name order and groups in identifier order, so a
    /// rejection names the same pair of declarations regardless of how the
    /// schema was assembled.
    package init(
        entities: [Schema.Entity],
        polymorphicGroups: [PolymorphicGroup]
    ) throws(DirectoryLayerTagError) {
        var positions: [Position] = [Position()]
        var renderedPaths: [String] = [""]
        let ordered = entities.sorted { $0.name < $1.name }
        var visited: [(name: String, indices: [Int])] = []
        visited.reserveCapacity(ordered.count)

        for entity in ordered {
            var index = 0
            var indices: [Int] = []
            indices.reserveCapacity(entity.directoryComponents.count)

            for component in entity.directoryComponents {
                switch component {
                case .staticPath(let value):
                    guard !Self.isCanonicalComponent(value) else {
                        throw .staticComponentInCanonicalImage(
                            declaration: .entity(entity.name),
                            component: value
                        )
                    }
                    if let child = positions[index].staticChildren[value] {
                        index = child
                    } else {
                        positions.append(Position())
                        renderedPaths.append(Self.path(renderedPaths[index], appending: value))
                        let child = positions.count - 1
                        positions[index].staticChildren[value] = child
                        index = child
                    }

                case .dynamicField(let fieldName):
                    guard let field = entity.fieldMapByName[fieldName] else {
                        throw .unknownDynamicField(entity: entity.name, fieldName: fieldName)
                    }
                    guard Self.admitsDynamicComponent(field.type) else {
                        throw .unsupportedDynamicFieldKind(
                            entity: entity.name,
                            fieldName: fieldName,
                            kind: field.type
                        )
                    }
                    if let existing = positions[index].dynamicChild {
                        guard existing.kind == field.type else {
                            throw .inconsistentDynamicFieldKind(
                                position: renderedPaths[index],
                                entity: entity.name,
                                kind: field.type,
                                conflictingEntity: existing.owner,
                                conflictingKind: existing.kind
                            )
                        }
                        index = existing.index
                    } else {
                        positions.append(Position())
                        renderedPaths.append(
                            Self.path(renderedPaths[index], appending: "{\(field.type.rawValue)}")
                        )
                        let child = positions.count - 1
                        positions[index].dynamicChild = DynamicChild(
                            kind: field.type,
                            owner: entity.name,
                            index: child
                        )
                        index = child
                    }
                }
                indices.append(index)
            }

            if let leafIndex = indices.last {
                try Self.resolveLeaf(
                    at: leafIndex,
                    layer: entity.directoryLayer,
                    owner: .entity(entity.name),
                    positions: &positions,
                    renderedPaths: renderedPaths
                )
            }
            visited.append((name: entity.name, indices: indices))
        }

        // A group declares only static components, so it never opens a dynamic
        // position; it can still resolve a position an entity passes through.
        for group in polymorphicGroups.sorted(by: { $0.identifier < $1.identifier }) {
            var index = 0
            for component in group.directoryComponents {
                switch component {
                case .staticPath(let value):
                    guard !Self.isCanonicalComponent(value) else {
                        throw .staticComponentInCanonicalImage(
                            declaration: .polymorphicGroup(group.identifier),
                            component: value
                        )
                    }
                    if let child = positions[index].staticChildren[value] {
                        index = child
                    } else {
                        positions.append(Position())
                        renderedPaths.append(Self.path(renderedPaths[index], appending: value))
                        let child = positions.count - 1
                        positions[index].staticChildren[value] = child
                        index = child
                    }

                case .dynamicField(let fieldName):
                    throw .dynamicComponentInPolymorphicGroup(
                        group: group.identifier,
                        fieldName: fieldName
                    )
                }
            }
            guard !group.directoryComponents.isEmpty else { continue }
            try Self.resolveLeaf(
                at: index,
                layer: group.directoryLayer,
                owner: .polymorphicGroup(group.identifier),
                positions: &positions,
                renderedPaths: renderedPaths
            )
        }

        // The layer of a position is known only once every declaration has been
        // inserted, because a later declaration can resolve a position that an
        // earlier one merely passes through.
        var derived: [String: [DirectoryLayer]] = [:]
        derived.reserveCapacity(visited.count)
        for entry in visited {
            derived[entry.name] = entry.indices.map { positions[$0].leaf?.layer ?? .default }
        }
        self.layersByEntityName = derived
        self.positions = positions
    }

    /// The derived layer of each declared component of `name`, in declaration
    /// order, or `nil` when the schema this map was derived from does not
    /// declare that entity.
    package func layers(forEntityNamed name: String) -> [DirectoryLayer]? {
        layersByEntityName[name]
    }

    /// The derived layer of each component of a literal path.
    ///
    /// A component that leaves the trie is a position no declaration reaches,
    /// so it and everything below it is a plain Directory. Only a static edge is
    /// followed: a literal that equals a canonical component image would address
    /// a dynamic sibling, which the initializer already rejects as unaddressable.
    package func layers(forPath path: [String]) -> [DirectoryLayer] {
        var index: Int? = 0
        var layers: [DirectoryLayer] = []
        layers.reserveCapacity(path.count)
        for component in path {
            index = index.flatMap { positions[$0].staticChildren[component] }
            layers.append(index.flatMap { positions[$0].leaf?.layer } ?? .default)
        }
        return layers
    }

    /// Whether a field kind has a canonical textual Directory component form.
    ///
    /// `FieldSchemaType` owns the admitted set so that a declaration this
    /// bridge cannot encode is rejected where it is written rather than at
    /// container bootstrap. This bridge owns the exact strings, and
    /// `DirectoryLayerTagMapTests` proves the owner's set is exactly the set
    /// `DirectoryComponentCodec` encodes.
    package static func admitsDynamicComponent(_ type: FieldSchemaType) -> Bool {
        type.hasCanonicalDirectoryComponent
    }

    /// Records the layer a declaration assigns to its leaf position, or rejects
    /// the pair of declarations that disagree about it.
    private static func resolveLeaf(
        at index: Int,
        layer: DirectoryLayer,
        owner: DirectoryDeclarationOwner,
        positions: inout [Position],
        renderedPaths: [String]
    ) throws(DirectoryLayerTagError) {
        if let existing = positions[index].leaf {
            guard existing.layer == layer else {
                throw .inconsistentLayer(
                    position: renderedPaths[index],
                    declaration: owner,
                    layer: layer,
                    conflictingDeclaration: existing.owner,
                    conflictingLayer: existing.layer
                )
            }
        } else {
            positions[index].leaf = Leaf(layer: layer, owner: owner)
        }
    }

    // MARK: - Derivation state

    private struct Position: Sendable {
        var staticChildren: [String: Int] = [:]
        var dynamicChild: DynamicChild?
        var leaf: Leaf?
    }

    private struct DynamicChild: Sendable {
        let kind: FieldSchemaType
        let owner: String
        let index: Int
    }

    private struct Leaf: Sendable {
        let layer: DirectoryLayer
        let owner: DirectoryDeclarationOwner
    }

    private static func path(_ parent: String, appending component: String) -> String {
        parent.isEmpty ? component : parent + "/" + component
    }

    /// Whether a literal component is a canonical component image.
    ///
    /// A dynamic sibling produces canonical components, so a static component
    /// that decodes as one is not addressable as a distinct node.
    private static func isCanonicalComponent(_ value: String) -> Bool {
        do {
            _ = try DirectoryComponentCodec.decode(value)
            return true
        } catch {
            return false
        }
    }
}
