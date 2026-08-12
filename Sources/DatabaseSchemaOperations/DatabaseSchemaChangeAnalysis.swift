import DatabaseJobRuntime
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseSchemaChangeAnalysis: Sendable {
    package let compatibility: SchemaExecuteOperation.Compatibility
    package let issues: [SchemaExecuteOperation.CompatibilityIssue]
    package let indexBuilds: [DatabaseSchemaIndexBuildDeclaration]

    package static func analyze(
        current: Schema,
        target: Schema
    ) -> DatabaseSchemaChangeAnalysis {
        if current.entities.isEmpty {
            return DatabaseSchemaChangeAnalysis(
                compatibility: .initial,
                issues: [],
                indexBuilds: []
            )
        }
        if current == target {
            return DatabaseSchemaChangeAnalysis(
                compatibility: .compatible,
                issues: [],
                indexBuilds: []
            )
        }

        var migrationIssues = target.compatibilityReport(from: current)
            .allIssues
            .map(wireIssue)
        var advisoryIssues: [SchemaExecuteOperation.CompatibilityIssue] = []
        var indexBuilds: [DatabaseSchemaIndexBuildDeclaration] = []
        if target.version <= current.version {
            migrationIssues.append(
                issue(
                    code: "schema-version-not-advanced",
                    path: "version",
                    message: "A changed schema must advance the schema version beyond \(current.version)"
                )
            )
        }

        for targetEntity in target.entities {
            guard let currentEntity = current.entity(named: targetEntity.name) else {
                continue
            }
            let entityPath = "entities.\(targetEntity.name)"
            if targetEntity.identifierType != currentEntity.identifierType {
                migrationIssues.append(issue(
                    code: "identifier-type-changed",
                    path: "\(entityPath).identifierType",
                    message: "Entity identifier encoding changed"
                ))
            }
            if targetEntity.directoryComponents != currentEntity.directoryComponents
                || targetEntity.directoryLayer != currentEntity.directoryLayer {
                migrationIssues.append(issue(
                    code: "directory-changed",
                    path: "\(entityPath).directory",
                    message: "Entity storage directory changed"
                ))
            }
            if targetEntity.relationships != currentEntity.relationships {
                migrationIssues.append(issue(
                    code: "relationships-changed",
                    path: "\(entityPath).relationships",
                    message: "Relationship maintenance contract changed"
                ))
            }
            if targetEntity.ontology != currentEntity.ontology {
                migrationIssues.append(issue(
                    code: "ontology-binding-changed",
                    path: "\(entityPath).ontology",
                    message: "Ontology projection changed and requires reindexing"
                ))
            }
            if targetEntity.polymorphicMembership
                != currentEntity.polymorphicMembership {
                migrationIssues.append(issue(
                    code: "polymorphic-membership-changed",
                    path: "\(entityPath).polymorphicMembership",
                    message: "Polymorphic projection membership changed"
                ))
            }
            let indexAnalysis = indexAnalysis(
                current: currentEntity,
                target: targetEntity,
                entityPath: entityPath
            )
            migrationIssues.append(contentsOf: indexAnalysis.migrationIssues)
            advisoryIssues.append(contentsOf: indexAnalysis.advisoryIssues)
            indexBuilds.append(contentsOf: indexAnalysis.builds)

            let report = targetEntity.compatibilityReport(from: currentEntity)
            for field in report.addedFields where !field.isOptional {
                migrationIssues.append(issue(
                    code: "required-field-added",
                    path: "\(entityPath).fields.\(field.name)",
                    message: "A required field was added to an existing entity"
                ))
            }
            for (field, currentCases) in currentEntity.enumMetadata {
                let targetCases = Set(targetEntity.enumMetadata[field] ?? [])
                if !Set(currentCases).isSubset(of: targetCases) {
                    migrationIssues.append(issue(
                        code: "enum-case-removed",
                        path: "\(entityPath).enumMetadata.\(field)",
                        message: "A previously valid enum case was removed"
                    ))
                }
            }
        }

        let uniqueMigrationIssues = deduplicate(migrationIssues)
        let uniqueIssues = deduplicate(migrationIssues + advisoryIssues)
        return DatabaseSchemaChangeAnalysis(
            compatibility: uniqueMigrationIssues.isEmpty
                ? .compatible
                : .requiresMigration,
            issues: uniqueIssues,
            indexBuilds: indexBuilds.sorted {
                if $0.entity != $1.entity { return $0.entity < $1.entity }
                return $0.index < $1.index
            }
        )
    }

    package static func mergedIndexBuilds(
        analyzed: [DatabaseSchemaIndexBuildDeclaration],
        pending: [String: Set<String>],
        schema: Schema
    ) -> [DatabaseSchemaIndexBuildDeclaration] {
        var builds = Set(analyzed)
        for entity in schema.entities {
            guard let pendingIndexes = pending[entity.name] else { continue }
            for index in pendingIndexes {
                builds.insert(
                    DatabaseSchemaIndexBuildDeclaration(
                        entity: entity.name,
                        index: index,
                        usesDynamicDirectory: entity.hasDynamicDirectory
                    )
                )
            }
        }
        return builds.sorted {
            if $0.entity != $1.entity { return $0.entity < $1.entity }
            return $0.index < $1.index
        }
    }

    private static func indexAnalysis(
        current: Schema.Entity,
        target: Schema.Entity,
        entityPath: String
    ) -> IndexAnalysis {
        let currentByName = Dictionary(
            uniqueKeysWithValues: current.indexes.map { ($0.name, $0) }
        )
        let targetByName = Dictionary(
            uniqueKeysWithValues: target.indexes.map { ($0.name, $0) }
        )
        var migrationIssues: [SchemaExecuteOperation.CompatibilityIssue] = []
        var advisoryIssues: [SchemaExecuteOperation.CompatibilityIssue] = []
        var builds: [DatabaseSchemaIndexBuildDeclaration] = []
        for name in currentByName.keys.sorted() where targetByName[name] == nil {
            migrationIssues.append(issue(
                code: "index-removed",
                path: "\(entityPath).indexes.\(name)",
                message: "Removing an index requires an explicit migration"
            ))
        }
        for name in targetByName.keys.sorted() {
            guard let currentIndex = currentByName[name] else {
                advisoryIssues.append(issue(
                    code: "index-build-required",
                    path: "\(entityPath).indexes.\(name)",
                    message: "Adding an index to an existing entity requires a persistent build job"
                ))
                builds.append(
                    DatabaseSchemaIndexBuildDeclaration(
                        entity: target.name,
                        index: name,
                        usesDynamicDirectory: target.hasDynamicDirectory
                    )
                )
                continue
            }
            if currentIndex != targetByName[name] {
                migrationIssues.append(issue(
                    code: "index-definition-changed",
                    path: "\(entityPath).indexes.\(name)",
                    message: "Changing an index definition requires an explicit migration"
                ))
            }
        }
        return IndexAnalysis(
            migrationIssues: migrationIssues,
            advisoryIssues: advisoryIssues,
            builds: builds
        )
    }

    private static func wireIssue(
        _ issueValue: SchemaCompatibilityIssue
    ) -> SchemaExecuteOperation.CompatibilityIssue {
        switch issueValue {
        case .removedEntity(let entityName):
            return issue(
                code: "entity-removed",
                path: "entities.\(entityName)",
                message: issueValue.description
            )
        case .removedField(let entityName, let fieldName, _),
             .renumberedField(let entityName, let fieldName, _, _),
             .changedFieldEncoding(let entityName, let fieldName, _, _),
             .nonAppendOnlyFieldAddition(let entityName, let fieldName, _, _):
            return issue(
                code: schemaIssueCode(issueValue),
                path: "entities.\(entityName).fields.\(fieldName)",
                message: issueValue.description
            )
        }
    }

    private static func schemaIssueCode(
        _ issueValue: SchemaCompatibilityIssue
    ) -> String {
        switch issueValue {
        case .removedEntity: "entity-removed"
        case .removedField: "field-removed"
        case .renumberedField: "field-renumbered"
        case .changedFieldEncoding: "field-encoding-changed"
        case .nonAppendOnlyFieldAddition: "field-number-not-append-only"
        }
    }

    private static func issue(
        code: String,
        path: String,
        message: String
    ) -> SchemaExecuteOperation.CompatibilityIssue {
        SchemaExecuteOperation.CompatibilityIssue(
            code: code,
            path: path,
            message: message
        )
    }

    private static func deduplicate(
        _ issues: [SchemaExecuteOperation.CompatibilityIssue]
    ) -> [SchemaExecuteOperation.CompatibilityIssue] {
        var seen = Set<String>()
        return issues.filter {
            seen.insert("\($0.code)\u{0}\($0.path)").inserted
        }
    }
}

package struct DatabaseSchemaIndexBuildDeclaration: Sendable, Hashable {
    package let entity: String
    package let index: String
    package let usesDynamicDirectory: Bool

    package init(
        entity: String,
        index: String,
        usesDynamicDirectory: Bool
    ) {
        self.entity = entity
        self.index = index
        self.usesDynamicDirectory = usesDynamicDirectory
    }
}

private struct IndexAnalysis {
    let migrationIssues: [SchemaExecuteOperation.CompatibilityIssue]
    let advisoryIssues: [SchemaExecuteOperation.CompatibilityIssue]
    let builds: [DatabaseSchemaIndexBuildDeclaration]
}
