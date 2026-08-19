#if DATABASE_MULTI_BASE
import StorageKit
import Synchronization

/// Owns every storage engine in one immutable topology through terminal shutdown.
final class DatabaseStorageTopologyLifecycle: Sendable {
    private enum Phase: Sendable, Equatable {
        case available
        case opening
        case open
        case closing
        case closed
    }

    private struct DomainLifecycle: Sendable {
        let id: DatabaseStorageDomain.ID
        let namespacePath: [String]
        let lifecycle: DatabaseStorageLifecycle
    }

    private let topology: DatabaseStorageTopology
    private let domains: [DomainLifecycle]
    private let phase = Mutex(Phase.available)

    init(topology: DatabaseStorageTopology) {
        self.topology = topology
        self.domains = topology.domains.map { domain in
            DomainLifecycle(
                id: domain.id,
                namespacePath: domain.namespacePath,
                lifecycle: DatabaseStorageLifecycle(
                    storageEngine: domain.storageEngine
                )
            )
        }
    }

    func claim() throws -> ClaimedDatabaseStorageTopology {
        try phase.withLock { phase in
            guard phase == .available else {
                throw DatabaseStorageTopologyError.configurationAlreadyClaimed
            }
            phase = .opening
        }

        do {
            var claimed: [
                DatabaseStorageDomain.ID: ClaimedDatabaseStorageTopology.Domain
            ] = [:]
            claimed.reserveCapacity(domains.count)
            for domain in domains {
                claimed[domain.id] = ClaimedDatabaseStorageTopology.Domain(
                    id: domain.id,
                    namespacePath: domain.namespacePath,
                    engine: try domain.lifecycle.claimStorageEngine()
                )
            }
            return ClaimedDatabaseStorageTopology(
                controlDomainID: topology.controlDomainID,
                domains: claimed,
                placements: Dictionary(
                    uniqueKeysWithValues: topology.placements.map {
                        ($0.id, $0)
                    }
                ),
                defaultPlacementID: topology.defaultPlacementID
            )
        } catch {
            phase.withLock { $0 = .closing }
            for domain in domains {
                domain.lifecycle.requestShutdown()
            }
            throw error
        }
    }

    func finishOpening() throws {
        try phase.withLock { phase in
            guard phase == .opening else {
                throw DatabaseStorageTopologyError.configurationAlreadyClaimed
            }
        }
        do {
            for domain in domains {
                try domain.lifecycle.finishOpening()
            }
            phase.withLock { $0 = .open }
        } catch {
            phase.withLock { $0 = .closing }
            for domain in domains {
                domain.lifecycle.requestShutdown()
            }
            throw error
        }
    }

    func shutdown() async {
        phase.withLock { phase in
            if phase != .closed {
                phase = .closing
            }
        }
        await withTaskGroup(of: Void.self) { group in
            for domain in domains {
                group.addTask {
                    await domain.lifecycle.shutdown()
                }
            }
        }
        phase.withLock { $0 = .closed }
    }

    func shutdownIfUnclaimed() async {
        let shouldShutdown = phase.withLock { phase in
            guard phase == .available else { return false }
            phase = .closing
            return true
        }
        guard shouldShutdown else { return }
        await withTaskGroup(of: Void.self) { group in
            for domain in domains {
                group.addTask {
                    await domain.lifecycle.shutdownIfUnclaimed()
                }
            }
        }
        phase.withLock { $0 = .closed }
    }

    func requestShutdown() {
        phase.withLock { phase in
            if phase != .closed {
                phase = .closing
            }
        }
        for domain in domains {
            domain.lifecycle.requestShutdown()
        }
    }
}
#endif
