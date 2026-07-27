import DatabaseKit

extension DatabaseContext {
    /// Validates restricted fields before inserting a model.
    public func validateFieldWrite<Model: Persistable>(
        _ model: borrowing Model
    ) throws(FieldSecurityError) {
        try FieldSecurityEvaluator.validateInsert(
            updated: model,
            context: RequestAuthorization.context
        )
    }

    /// Validates restricted fields changed by an update.
    public func validateFieldWrite<Model: Persistable>(
        original: borrowing Model,
        updated: borrowing Model
    ) throws(FieldSecurityError) {
        try FieldSecurityEvaluator.validateUpdate(
            original: original,
            updated: updated,
            context: RequestAuthorization.context
        )
    }

    /// Returns the exact compiled fields the current request may not read.
    public func unreadableFields<Model: Persistable>(
        in model: borrowing Model
    ) -> [FieldIdentity] {
        FieldSecurityEvaluator.unreadableFields(
            in: model,
            context: RequestAuthorization.context
        )
    }

    /// Returns the exact compiled fields the current request may not write.
    public func unwritableFields<Model: Persistable>(
        in model: borrowing Model
    ) -> [FieldIdentity] {
        FieldSecurityEvaluator.unwritableFields(
            in: model,
            context: RequestAuthorization.context
        )
    }

    /// Returns whether the current request may read one exact compiled field.
    public func canRead<Model: Persistable, Value>(
        _ field: Field<Model, Value>,
        in model: borrowing Model
    ) -> Bool {
        FieldSecurityEvaluator.canRead(
            field,
            in: model,
            context: RequestAuthorization.context
        )
    }

    /// Returns whether the current request may write one exact compiled field.
    public func canWrite<Model: Persistable, Value>(
        _ field: Field<Model, Value>,
        in model: borrowing Model
    ) -> Bool {
        FieldSecurityEvaluator.canWrite(
            field,
            in: model,
            context: RequestAuthorization.context
        )
    }
}
