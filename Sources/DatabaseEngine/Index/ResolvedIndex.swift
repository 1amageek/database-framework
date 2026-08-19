import DatabaseKit

/// A validated schema index resolved for one runtime and physical subspace.
public struct ResolvedIndex: Sendable {
    public let descriptor: IndexDescriptor
    public let rootExpression: KeyExpression
    public let itemTypes: Set<String>?

    public init(
        descriptor: IndexDescriptor,
        rootExpression: KeyExpression,
        itemTypes: Set<String>? = nil
    ) {
        self.descriptor = descriptor
        self.rootExpression = rootExpression
        self.itemTypes = itemTypes
    }

    /// Builds a resolved index from a typed entity and the same semantic
    /// definition retained by its schema. This initializer is package-scoped
    /// because production runtimes resolve descriptors from an installed
    /// `Schema`; feature modules and their tests use it for direct maintainers.
    package init<Item: Persistable>(
        for itemType: Item.Type,
        name: String,
        definition: IndexDefinition<FieldIdentity>,
        rootExpression: KeyExpression,
        itemTypes: Set<String>? = nil
    ) throws {
        let descriptor = try IndexDescriptor(
            entityName: itemType.persistableType,
            declaration: IndexDeclaration(
                name: name,
                definition: definition
            ),
            fieldSchemas: itemType.fieldSchemas
        )
        self.init(
            descriptor: descriptor,
            rootExpression: rootExpression,
            itemTypes: itemTypes
        )
    }

    public var name: String { descriptor.name }
    public var type: IndexType { descriptor.type }
    public var definition: IndexDefinition<FieldIdentity> {
        descriptor.declaration.definition
    }
    public var isUnique: Bool { descriptor.isUnique }
    public var fieldNames: [String] { descriptor.fieldNames }
    public var includedFieldNames: [String] {
        descriptor.includedFieldNames
    }
}
