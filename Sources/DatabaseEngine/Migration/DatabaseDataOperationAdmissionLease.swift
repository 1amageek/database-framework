/// Retains one data-operation admission until every task inheriting the
/// operation scope has released it.
final class DatabaseDataOperationAdmissionLease: Sendable {
    let schemaGeneration: UInt64

    private let gate: DatabaseMigrationAdmissionGate

    init(
        gate: DatabaseMigrationAdmissionGate,
        schemaGeneration: UInt64
    ) throws {
        try gate.enterDataOperation(schemaGeneration: schemaGeneration)
        self.gate = gate
        self.schemaGeneration = schemaGeneration
    }

    func belongs(to gate: DatabaseMigrationAdmissionGate) -> Bool {
        self.gate === gate
    }

    deinit {
        gate.leaveDataOperation()
    }
}
