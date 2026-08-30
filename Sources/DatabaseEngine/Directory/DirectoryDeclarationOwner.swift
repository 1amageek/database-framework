/// The declaration a derived Directory node position came from.
///
/// A node position is derived from the whole schema, so a disagreement about it
/// is a disagreement between two declarations. An entity and a polymorphic group
/// are both declarations and both can resolve a position as a leaf, so a
/// diagnostic names which kind of declaration it refers to rather than calling
/// every owner an entity.
package enum DirectoryDeclarationOwner: Sendable, Equatable, CustomStringConvertible {

    /// An entity `#Directory` declaration, named by the entity name.
    case entity(String)

    /// A polymorphic group `#Directory` declaration, named by the group
    /// identifier.
    case polymorphicGroup(String)

    package var description: String {
        switch self {
        case .entity(let name):
            return "entity '\(name)'"
        case .polymorphicGroup(let identifier):
            return "polymorphic group '\(identifier)'"
        }
    }
}
