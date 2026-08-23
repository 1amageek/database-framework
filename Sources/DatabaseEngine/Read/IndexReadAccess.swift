import DatabaseKit
import StorageKit

/// Read-only physical access constrained to one schema-admitted index.
///
/// Implementations reject keys and ranges outside the admitted index subspace.
/// Mutation, transaction-control, and lifecycle capabilities are intentionally
/// absent from this contract.
public protocol IndexReadAccess: TransactionReadAccess {}

package struct IndexReadRequest: Sendable {
    package let indexName: String
    package let indexType: IndexType
    package let entityName: String
    package let partitions: FieldObject
    package let authorization: IndexReadAuthorization

    package init(
        indexName: String,
        indexType: IndexType,
        entityName: String,
        partitions: FieldObject = FieldObject(),
        authorization: IndexReadAuthorization
    ) {
        self.indexName = indexName
        self.indexType = indexType
        self.entityName = entityName
        self.partitions = partitions
        self.authorization = authorization
    }
}

/// Package capability that combines one confined physical index reader with
/// persistence projection on the same hidden database transaction. Feature
/// packages can fetch declared entities without recovering root-wide storage
/// access or opening a second snapshot.
package protocol IndexQuerySnapshotAccess: PersistenceQueryReadAccess {
    func withAuxiliaryReadStorage<Result: Sendable>(
        namespace: ByteString,
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result

    func withAuxiliaryReadStorage<Result: Sendable>(
        path: [String],
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result

    func withReadableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result

    func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result

    func withReadableIndexes<Result: Sendable>(
        _ requests: [IndexReadRequest],
        _ operation: @Sendable @escaping (
            [ReadableIndex?],
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result
}

package protocol IndexQueryReadAccess:
    IndexReadAccess,
    IndexQuerySnapshotAccess {}
