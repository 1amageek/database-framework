import DatabaseTypes

struct DatabasePersistentJobSnapshot: Sendable {
    let specification: DatabasePersistentJobSpecification
    let specificationDigest: ByteString
    let plan: DatabasePersistentJobPlan
    let state: DatabasePersistentJobState
}
