#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Immutable mapping from one authoritative layout-v1 range to its Base-local
/// layout-v2 destination. Physical prefixes are retained only for the lifetime
/// of one inventory and never exposed over DatabaseWire.
package struct DatabaseLegacyLayoutInventory: Sendable {
    package struct Entry: Sendable, Hashable {
        package let identifier: String
        package let sourceRoot: Subspace
        package let destinationSuffix: ByteString

        package func destinationRoot(in baseRoot: Subspace) -> Subspace {
            Subspace(
                prefix: baseRoot.prefix.appending(
                    contentsOf: destinationSuffix
                )
            )
        }
    }

    package let entries: [Entry]
    package let cleanupNamespacePaths: [[String]]
    package let cleanupRawRoots: [Subspace]

    package init(
        entries: [Entry],
        cleanupNamespacePaths: [[String]],
        cleanupRawRoots: [Subspace]
    ) throws {
        let orderedEntries = entries.sorted {
            $0.identifier.utf8.lexicographicallyPrecedes($1.identifier.utf8)
        }
        var identifiers: Set<String> = []
        var sourcePrefixes: Set<ByteString> = []
        var destinationSuffixes: Set<ByteString> = []
        for entry in orderedEntries {
            guard !entry.identifier.isEmpty,
                  !entry.sourceRoot.prefix.isEmpty,
                  !entry.destinationSuffix.isEmpty,
                  identifiers.insert(entry.identifier).inserted,
                  sourcePrefixes.insert(entry.sourceRoot.prefix).inserted,
                  destinationSuffixes.insert(entry.destinationSuffix).inserted
            else {
                throw DatabaseLegacyLayoutMigrationError.invalidInventory
            }
        }
        self.entries = orderedEntries
        self.cleanupNamespacePaths = Array(Set(cleanupNamespacePaths)).sorted {
            $0.lexicographicallyPrecedes($1)
        }
        self.cleanupRawRoots = cleanupRawRoots.sorted {
            $0.prefix.lexicographicallyPrecedes($1.prefix)
        }
    }
}

package struct DatabaseLegacyLayoutTransferProgress: Sendable, Hashable {
    package let entryIndex: Int
    package let continuation: ByteString?
    package let digest: ByteString
    package let keyCount: UInt64
    package let byteCount: UInt64
    package let isComplete: Bool

    package init(
        entryIndex: Int,
        continuation: ByteString?,
        digest: ByteString,
        keyCount: UInt64,
        byteCount: UInt64,
        isComplete: Bool
    ) {
        self.entryIndex = entryIndex
        self.continuation = continuation
        self.digest = digest
        self.keyCount = keyCount
        self.byteCount = byteCount
        self.isComplete = isComplete
    }
}

package enum DatabaseLegacyLayoutMigrationError:
    Error,
    Sendable,
    Equatable
{
    case layoutIsCurrent
    case invalidInventory
    case legacyJobsPresent
    case unknownPartitionEntity(String)
    case conflictingLegacyOntologyStores
    case controlDomainOverlapsLegacyData
    case destinationOverlapsLegacyData
    case destinationBaseExists(Base.ID)
    case fingerprintMismatch
    case sourceChangedDuringMigration
    case destinationDigestMismatch
    case invalidTransferState
    case transferOverflow
    case cleanupFailed(String)
}

#endif
