#if DATABASE_MULTIPLE_BASES
/// Durable admission state for the physical Base layout.
package enum DatabaseLayoutStatus: UInt8, Sendable, Hashable {
    case current = 0
    case migrationRequired = 1
}

#endif
