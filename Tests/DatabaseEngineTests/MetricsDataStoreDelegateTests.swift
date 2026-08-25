#if !os(WASI)
import TestHeartbeat
import Testing
@testable import DatabaseEngine

@Suite("Metrics data store delegate", .heartbeat)
struct MetricsDataStoreDelegateTests {
    private struct GenericFailure<Value>: Error {}
    private struct ThisErrorTypeNameIsIntentionallyLongerThanTheMetricsLabelLimit: Error {}

    @Test("Error labels use one bounded low-cardinality value")
    func errorLabelsAreStableAndLowCardinality() {
        let label = MetricsDataStoreDelegate.metricsErrorType(
            for: GenericFailure<[Int]>()
        )
        let otherLabel = MetricsDataStoreDelegate.metricsErrorType(
            for: ThisErrorTypeNameIsIntentionallyLongerThanTheMetricsLabelLimit()
        )

        #expect(label == "operation_failure")
        #expect(otherLabel == label)
        #expect(label.unicodeScalars.count <= 50)
        #expect(label.unicodeScalars.allSatisfy { scalar in
            switch scalar.value {
            case 48...57, 65...90, 95, 97...122:
                return true
            default:
                return false
            }
        })
    }
}
#endif
