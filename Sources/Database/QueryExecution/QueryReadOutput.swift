import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine

/// Retains read payload claims until their encoded output has been admitted.
package enum QueryReadOutput: Sendable {
    case rows(DatabaseSharedRetainedQueryRows)
    case boolean(Bool, DatabaseReadPosition, String)
    case graph(DatabaseSharedRetainedRDFGraph, DatabaseReadPosition, String)
}

