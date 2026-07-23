import DatabaseEngine
import DatabaseValue
import Graph
import GraphIndex
import StorageKit
import Synchronization

final class ConflictOnceRDFGraphMutationStore: RDFGraphMutationStore, Sendable {
    enum ConflictInjectionStoreError: Error {
        case unexpectedOperation
    }

    private let insertionAttempts = Mutex(0)

    var insertAttemptCount: Int {
        insertionAttempts.withLock { $0 }
    }

    func scan(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName] {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func containsGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func createGraph(
        _ graph: RDFGraphName,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func insert(
        _ quad: RDFQuad,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphInsertResult {
        let attempt = insertionAttempts.withLock { count in
            count += 1
            return count
        }
        if attempt == 1 {
            throw StorageError.transactionConflict
        }
        return RDFGraphInsertResult(
            quadInserted: true,
            graphCreated: false
        )
    }

    func delete(
        _ quad: RDFQuad,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func clear(
        _ scope: RDFGraphMutationScope,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        throw ConflictInjectionStoreError.unexpectedOperation
    }

    func drop(
        _ scope: RDFGraphMutationScope,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        throw ConflictInjectionStoreError.unexpectedOperation
    }
}
