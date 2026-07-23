import DatabaseEngine
import QueryIR
import StorageKit

/// Database-wide authoritative catalog for SQL/PGQ property graph definitions.
///
/// Implementations participate in the caller-owned transaction. They must not
/// commit, retry, or otherwise outlive that transaction.
public protocol PropertyGraphDefinitionCatalog: Sendable {
    func definition(
        named graphName: String,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> CreateGraphStatement?

    @discardableResult
    func create(
        _ definition: CreateGraphStatement,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> PropertyGraphDefinitionCreation

    func dropDefinition(
        named graphName: String,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws
}

/// Observable outcome of a conditional property graph definition creation.
public enum PropertyGraphDefinitionCreation: Sendable, Equatable {
    case created
    case retainedExistingDefinition
}
