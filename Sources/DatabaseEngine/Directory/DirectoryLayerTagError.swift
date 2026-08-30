import DatabaseKit

/// A typed schema error raised while deriving the layer tag of every Directory
/// node position from the complete set of `#Directory` declarations.
///
/// The derivation is owned by `DirectoryLayerTagMap`; the reasons it can reject a
/// schema are documented in `Sources/DatabaseEngine/Directory/DESIGN.md`. Every
/// case names the offending declaration so an operator can repair the model that
/// introduced the disagreement rather than the one that merely observed it.
package enum DirectoryLayerTagError: Error, Equatable, Sendable {

    /// Two declarations resolve the same node position as a leaf and assign it
    /// different layers. The position cannot be both a plain Directory and a
    /// Partition, so neither reading wins. Either declaration may be an entity
    /// or a polymorphic group, because both declare a leaf.
    case inconsistentLayer(
        position: String,
        declaration: DirectoryDeclarationOwner,
        layer: DirectoryLayer,
        conflictingDeclaration: DirectoryDeclarationOwner,
        conflictingLayer: DirectoryLayer
    )

    /// Two declarations pass through the same dynamic node position and declare
    /// different field kinds there. One node position carries one field kind.
    case inconsistentDynamicFieldKind(
        position: String,
        entity: String,
        kind: FieldSchemaType,
        conflictingEntity: String,
        conflictingKind: FieldSchemaType
    )

    /// A dynamic component references a field kind that has no canonical textual
    /// Directory component form, so no node name can represent its values.
    case unsupportedDynamicFieldKind(entity: String, fieldName: String, kind: FieldSchemaType)

    /// A static component is itself a canonical component image. A dynamic sibling
    /// can produce the same name, so the static component is not addressable as a
    /// distinct node.
    case staticComponentInCanonicalImage(
        declaration: DirectoryDeclarationOwner,
        component: String
    )

    /// A polymorphic group declaration carries a dynamic component. A group
    /// addresses one directory shared by every member, so it has no record from
    /// which a dynamic component could be resolved.
    case dynamicComponentInPolymorphicGroup(group: String, fieldName: String)

    /// A dynamic component references a field the entity does not declare.
    case unknownDynamicField(entity: String, fieldName: String)
}
