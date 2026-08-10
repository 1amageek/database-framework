import DatabaseKit
import DatabaseTypes

/// Evaluates compiled field authorization rules without reconstructing or
/// mutating model values.
public enum FieldSecurityEvaluator {
    /// Validates the schema-declared fields that a read will observe.
    public static func validateRead(
        entity: Schema.Entity,
        fields: Set<String>?,
        context: borrowing AuthorizationContext
    ) throws(FieldSecurityError) {
        let violations: [String] = entity.fieldAccessRules.compactMap { rule in
            guard fields == nil || fields?.contains(rule.field.name) == true
            else { return nil }
            return rule.read.allows(context) ? nil : rule.field.name
        }
        guard violations.isEmpty else {
            throw .readNotAllowed(
                type: entity.name,
                fields: violations.sorted()
            )
        }
    }

    /// Validates every schema-declared field written by a canonical insert.
    public static func validateInsert(
        entity: Schema.Entity,
        updated: borrowing PersistedModel,
        context: borrowing AuthorizationContext
    ) throws(FieldSecurityError) {
        guard updated.entity == entity.name else {
            throw .unsupportedFieldValue(
                type: entity.name,
                field: "<entity>",
                reason: "the canonical model belongs to a different entity"
            )
        }
        let violations: [String] = entity.fieldAccessRules.compactMap { rule in
            rule.write.allows(context) ? nil : rule.field.name
        }
        guard violations.isEmpty else {
            throw .writeNotAllowed(
                type: entity.name,
                fields: violations.sorted()
            )
        }
    }

    /// Validates only schema-declared fields changed by a canonical update.
    public static func validateUpdate(
        entity: Schema.Entity,
        original: borrowing PersistedModel,
        updated: borrowing PersistedModel,
        context: borrowing AuthorizationContext
    ) throws(FieldSecurityError) {
        guard original.entity == entity.name, updated.entity == entity.name else {
            throw .unsupportedFieldValue(
                type: entity.name,
                field: "<entity>",
                reason: "the canonical model belongs to a different entity"
            )
        }
        var violations: [String] = []
        violations.reserveCapacity(entity.fieldAccessRules.count)
        for rule in entity.fieldAccessRules where !rule.write.allows(context) {
            let oldValue = try canonicalValue(
                for: rule.field,
                in: original,
                entity: entity.name
            )
            let newValue = try canonicalValue(
                for: rule.field,
                in: updated,
                entity: entity.name
            )
            if oldValue != newValue { violations.append(rule.field.name) }
        }
        guard violations.isEmpty else {
            throw .writeNotAllowed(
                type: entity.name,
                fields: violations.sorted()
            )
        }
    }

    /// Returns whether the request may read one exact compiled field.
    public static func canRead<Model: Persistable, Value>(
        _ field: Field<Model, Value>,
        in model: borrowing Model,
        context: borrowing AuthorizationContext
    ) -> Bool {
        accessRule(for: field.identity, in: Model.self)?.read.allows(context)
            ?? true
    }

    /// Returns whether the request may write one exact compiled field.
    public static func canWrite<Model: Persistable, Value>(
        _ field: Field<Model, Value>,
        in model: borrowing Model,
        context: borrowing AuthorizationContext
    ) -> Bool {
        accessRule(for: field.identity, in: Model.self)?.write.allows(context)
            ?? true
    }

    /// Returns the exact compiled fields the request may not read.
    public static func unreadableFields<Model: Persistable>(
        in model: borrowing Model,
        context: borrowing AuthorizationContext
    ) -> [FieldIdentity] {
        Model.fieldAccessRules.compactMap { rule in
            rule.read.allows(context) ? nil : rule.field
        }
    }

    /// Returns the exact compiled fields the request may not write.
    public static func unwritableFields<Model: Persistable>(
        in model: borrowing Model,
        context: borrowing AuthorizationContext
    ) -> [FieldIdentity] {
        Model.fieldAccessRules.compactMap { rule in
            rule.write.allows(context) ? nil : rule.field
        }
    }

    /// Validates every persisted field written by an insert.
    public static func validateInsert<Model: Persistable>(
        updated: borrowing Model,
        context: borrowing AuthorizationContext
    ) throws(FieldSecurityError) {
        let violations = Model.fieldAccessRules.compactMap { rule in
            rule.write.allows(context) ? nil : rule.field.name
        }
        try rejectWriteIfNeeded(violations, modelType: Model.self)
    }

    /// Validates only fields whose canonical values change during an update.
    public static func validateUpdate<Model: Persistable>(
        original: borrowing Model,
        updated: borrowing Model,
        context: borrowing AuthorizationContext
    ) throws(FieldSecurityError) {
        var violations: [String] = []
        violations.reserveCapacity(Model.fieldAccessRules.count)

        for rule in Model.fieldAccessRules where !rule.write.allows(context) {
            let oldValue = try canonicalValue(for: rule.field, in: original)
            let newValue = try canonicalValue(for: rule.field, in: updated)
            if oldValue != newValue {
                violations.append(rule.field.name)
            }
        }
        try rejectWriteIfNeeded(violations, modelType: Model.self)
    }

    private static func accessRule<Model: Persistable>(
        for field: FieldIdentity,
        in type: Model.Type
    ) -> FieldAccessRule? {
        Model.fieldAccessRules.first { $0.field == field }
    }

    private static func canonicalValue<Model: Persistable>(
        for field: FieldIdentity,
        in model: borrowing Model
    ) throws(FieldSecurityError) -> FieldValue {
        let value: FieldValue?
        do {
            value = try model.persistedFieldValue(for: field)
        } catch {
            throw .unsupportedFieldValue(
                type: Model.persistableType,
                field: field.name,
                reason: "compiled field encoding failed"
            )
        }
        guard let value else {
            throw .unsupportedFieldValue(
                type: Model.persistableType,
                field: field.name,
                reason: "the compiled model adapter did not emit the field"
            )
        }
        return value
    }

    private static func canonicalValue(
        for field: FieldIdentity,
        in model: borrowing PersistedModel,
        entity: String
    ) throws(FieldSecurityError) -> FieldValue {
        guard let value = model.value(for: field) else {
            throw .unsupportedFieldValue(
                type: entity,
                field: field.name,
                reason: "the canonical model did not contain the schema field"
            )
        }
        return value
    }

    private static func rejectWriteIfNeeded<Model: Persistable>(
        _ violations: [String],
        modelType: Model.Type
    ) throws(FieldSecurityError) {
        guard violations.isEmpty else {
            throw .writeNotAllowed(
                type: modelType.persistableType,
                fields: violations.sorted()
            )
        }
    }
}
