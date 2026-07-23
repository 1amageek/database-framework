import Synchronization
import Testing
@testable import DatabaseEngine

@Suite("Database logging configuration")
struct DatabaseLoggingConfigurationTests {
    private struct EmittedEvent: Equatable, Sendable {
        let label: String
        let level: DatabaseLogger.Level
        let message: String
        let metadata: [String: String]
    }

    @Test("selected logging endpoint receives the database event")
    func selectedEndpointReceivesEvent() {
        let emittedEvents = Mutex<[EmittedEvent]>([])
        let configuration = DatabaseLoggingConfiguration { label in
            DatabaseLogger(minimumLevel: .info) {
                level,
                message,
                metadata in
                emittedEvents.withLock {
                    $0.append(
                        EmittedEvent(
                            label: label,
                            level: level,
                            message: message,
                            metadata: metadata
                        )
                    )
                }
            }
        }

        let logger = configuration.logger(label: "database.query")
        logger.debug("not emitted")
        logger.info(
            "query completed",
            metadata: ["operation": "query.execute"]
        )

        #expect(
            emittedEvents.withLock { $0 } == [
                EmittedEvent(
                    label: "database.query",
                    level: .info,
                    message: "query completed",
                    metadata: ["operation": "query.execute"]
                )
            ]
        )
    }

    @Test("disabled logging does not evaluate event messages")
    func disabledLoggingDoesNotEvaluateMessages() {
        let evaluationCount = Mutex(0)
        let logger = DatabaseLoggingConfiguration.disabled.logger(
            label: "database.disabled"
        )

        logger.error(
            evaluationCount.withLock {
                $0 += 1
                return "unexpected evaluation"
            }
        )

        #expect(evaluationCount.withLock { $0 } == 0)
    }
}
