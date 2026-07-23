#if !os(WASI)
#if FOUNDATION_DB
import TestHeartbeat
import Testing
@testable import DatabaseEngine

@Suite("Metrics data store delegate", .heartbeat)
struct MetricsDataStoreDelegateTests {
    private struct GenericFailure<Value>: Error {}
    private struct ThisErrorTypeNameIsIntentionallyLongerThanTheMetricsLabelLimit: Error {}

    @Test("Error type labels replace punctuation with ASCII underscores")
    func errorTypeLabelsReplacePunctuation() {
        let label = MetricsDataStoreDelegate.metricsErrorType(
            for: GenericFailure<[Int]>()
        )

        #expect(label == "GenericFailure_Array_Int__")
    }

    @Test("Error type labels are bounded to fifty ASCII scalars")
    func errorTypeLabelsAreBounded() {
        let label = MetricsDataStoreDelegate.metricsErrorType(
            for: ThisErrorTypeNameIsIntentionallyLongerThanTheMetricsLabelLimit()
        )

        #expect(label.unicodeScalars.count == 50)
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
#endif
