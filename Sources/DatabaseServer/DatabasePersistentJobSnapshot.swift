import DatabaseValue

struct DatabasePersistentJobSnapshot: Sendable {
    let specification: DatabasePersistentJobSpecification
    let specificationDigest: DatabaseBytes
    let plan: DatabasePersistentJobPlan
    let state: DatabasePersistentJobState
}
