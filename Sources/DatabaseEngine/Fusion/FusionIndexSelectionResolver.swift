import DatabaseKit

/// Resolves the schema-level meaning of every Fusion index selection.
enum FusionIndexSelectionResolver {
    static func resolve(
        _ selection: FusionIndexSelection,
        in entity: Schema.Entity
    ) throws -> IndexDescriptor {
        switch selection {
        case .named(let name, let expectedType):
            guard let descriptor = entity.indexDescriptors.first(
                where: { $0.name == name }
            ) else {
                throw FusionExecutionError.indexNotFound(
                    entity: entity.name,
                    name: name
                )
            }
            guard descriptor.type == expectedType else {
                throw FusionExecutionError.indexTypeMismatch(
                    entity: entity.name,
                    name: name,
                    expected: expectedType,
                    actual: descriptor.type
                )
            }
            return descriptor
        case .matching(let type, let fields, let fieldMatch):
            let requestedFields = Set(fields)
            let matches = entity.indexDescriptors.filter { descriptor in
                guard descriptor.type == type else { return false }
                switch fieldMatch {
                case .exact:
                    return descriptor.fieldIdentities == fields
                case .contains:
                    return requestedFields.isSubset(
                        of: Set(descriptor.fieldIdentities)
                    )
                }
            }
            guard !matches.isEmpty else {
                throw FusionExecutionError.indexMatchNotFound(
                    entity: entity.name,
                    type: type
                )
            }
            guard matches.count == 1, let match = matches.first else {
                throw FusionExecutionError.ambiguousIndexMatch(
                    entity: entity.name,
                    type: type,
                    names: matches.map(\.name).sorted()
                )
            }
            return match
        }
    }
}
