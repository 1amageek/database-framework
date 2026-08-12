import DatabaseOperationCore
import DatabaseTypes

package struct DatabasePersistentJobSnapshot: Sendable {
    package let specification: DatabasePersistentJobSpecification
    package let specificationDigest: ByteString
    package let plan: DatabasePersistentJobPlan
    package let state: DatabasePersistentJobState

    package init(
        specification: DatabasePersistentJobSpecification,
        specificationDigest: ByteString,
        plan: DatabasePersistentJobPlan,
        state: DatabasePersistentJobState
    ) {
        self.specification = specification
        self.specificationDigest = specificationDigest
        self.plan = plan
        self.state = state
    }
}
