import DatabaseKit

/// Validates deployment-specific index policy against the compiled schema.
enum IndexRuntimeConfigurationValidator {
    static func validate(
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        allowingConfigurationsOutsideSchema: Bool = false
    ) throws(IndexRuntimeConfigurationError) -> [String: IndexPhysicalLayout] {
        let entityRuntimes = runtimeConfiguration.entityRuntimes
        let configurations = runtimeConfiguration.indexConfigurations

        var typesByName: [String: IndexType] = [:]
        for descriptor in schema.indexDescriptors {
            typesByName[descriptor.name] = descriptor.type
        }
        for group in schema.polymorphicGroups {
            for memberTypeName in group.memberTypeNames {
                guard entityRuntimes.registration(
                    named: memberTypeName
                ) != nil else {
                    throw .invalidConfiguration(
                        indexName: group.identifier,
                        reason: "compiled polymorphic member '\(memberTypeName)' is unavailable"
                    )
                }
            }
            for declaration in group.indexes {
                typesByName[declaration.name] = declaration.definition.type
            }
        }

        for configuration in configurations {
            guard !configuration.indexName.isEmpty else {
                throw .invalidConfiguration(
                    indexName: configuration.indexName,
                    reason: "index name must not be empty"
                )
            }
            guard let indexType = typesByName[configuration.indexName] else {
                if allowingConfigurationsOutsideSchema {
                    continue
                }
                throw .unknownIndex(indexName: configuration.indexName)
            }
            guard indexType == configuration.indexType else {
                throw .indexTypeMismatch(
                    indexName: configuration.indexName,
                    expected: indexType,
                    actual: configuration.indexType
                )
            }
        }

        var configurationsByIndex: [String: [any IndexRuntimeConfiguration]] = [:]
        configurationsByIndex.reserveCapacity(configurations.count)
        for configuration in configurations
        where
            typesByName[configuration.indexName] != nil
        {
            configurationsByIndex[configuration.indexName, default: []]
                .append(configuration)
        }

        var layouts: [String: IndexPhysicalLayout] = [:]
        layouts.reserveCapacity(schema.indexDescriptors.count)
        for entity in schema.entities {
            guard let runtime = entityRuntimes.registration(named: entity.name)
            else {
                continue
            }
            for descriptor in entity.indexDescriptors {
                let index = resolvedIndex(
                    descriptor,
                    itemTypes: Set([entity.name])
                )
                let layout = try providerLayout(
                    index: index,
                    configurations: configurationsByIndex[descriptor.name] ?? []
                ) {
                    try runtime.physicalLayout(
                        for: index,
                        configurations: $0
                    )
                }
                try insert(
                    layout,
                    for: descriptor.name,
                    into: &layouts
                )
            }
        }

        for group in schema.polymorphicGroups {
            for declaration in group.indexes {
                let matchingConfigurations =
                    configurationsByIndex[declaration.name] ?? []
                var groupLayout: IndexPhysicalLayout?
                var representative: ResolvedIndex?
                for memberTypeName in group.memberTypeNames {
                    guard
                        let runtime = entityRuntimes.registration(
                            named: memberTypeName
                        ),
                        let descriptor = schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberTypeName: memberTypeName
                        ).first(where: { $0.name == declaration.name })
                    else {
                        continue
                    }
                    let index = resolvedIndex(
                        descriptor,
                        itemTypes: Set([group.identifier])
                    )
                    representative = representative ?? index
                    let layout = try providerLayout(
                        index: index,
                        configurations: matchingConfigurations
                    ) {
                        try runtime.physicalLayout(
                            for: index,
                            configurations: $0
                        )
                    }
                    if let groupLayout, groupLayout != layout {
                        throw .inconsistentPhysicalLayout(
                            indexName: declaration.name
                        )
                    }
                    groupLayout = layout
                }

                if let representative {
                    let sharedLayout = try providerLayout(
                        index: representative,
                        configurations: matchingConfigurations,
                        resolve: {
                            try runtimeConfiguration.indexMaintainerProviders
                                .physicalLayout(
                                    for: representative,
                                    configurations: $0
                                )
                        }
                    )
                    if let groupLayout, sharedLayout != groupLayout {
                        throw .inconsistentPhysicalLayout(
                            indexName: declaration.name
                        )
                    }
                }
                guard let groupLayout else {
                    continue
                }
                try insert(
                    groupLayout,
                    for: declaration.name,
                    into: &layouts
                )
            }
        }
        return layouts
    }

    private static func resolvedIndex(
        _ descriptor: IndexDescriptor,
        itemTypes: Set<String>
    ) -> ResolvedIndex {
        ResolvedIndex(
            descriptor: descriptor,
            rootExpression: KeyExpressionFactory.from(
                keyPaths: descriptor.fieldNames
            ),
            itemTypes: itemTypes
        )
    }

    private static func providerLayout(
        index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration],
        resolve: ([any IndexRuntimeConfiguration]) throws
            -> IndexPhysicalLayout
    ) throws(IndexRuntimeConfigurationError) -> IndexPhysicalLayout {
        do {
            return try resolve(configurations)
        } catch let error as IndexRuntimeConfigurationError {
            throw error
        } catch let error as IndexMaintainerProviderError {
            throw .providerRejected(
                indexName: index.name,
                indexType: index.type,
                reason: error.description
            )
        } catch {
            throw .providerRejected(
                indexName: index.name,
                indexType: index.type,
                reason: "Index maintainer provider rejected the runtime configuration"
            )
        }
    }

    private static func insert(
        _ layout: IndexPhysicalLayout,
        for indexName: String,
        into layouts: inout [String: IndexPhysicalLayout]
    ) throws(IndexRuntimeConfigurationError) {
        if let existing = layouts[indexName], existing != layout {
            throw .inconsistentPhysicalLayout(indexName: indexName)
        }
        layouts[indexName] = layout
    }
}
