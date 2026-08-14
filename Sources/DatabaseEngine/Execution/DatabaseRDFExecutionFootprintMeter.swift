import DatabaseKit
import DatabaseWire

@_spi(DatabaseExecution)
public struct DatabaseExecutionFootprint: Sendable, Equatable {
    public let rows: UInt64
    public let bytes: UInt64

    public init(rows: UInt64, bytes: UInt64) {
        self.rows = rows
        self.bytes = bytes
    }
}

/// Accounts for RDF values retained by an execution adapter without exposing
/// DatabaseEngine's internal reservation representation.
@_spi(DatabaseExecution)
public final class DatabaseRDFExecutionFootprintMeter {
    private let meter: DatabaseRDFQuadFootprintMeter

    private init(meter: DatabaseRDFQuadFootprintMeter) {
        self.meter = meter
    }

    public static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRDFExecutionFootprintMeter {
        DatabaseRDFExecutionFootprintMeter(
            meter: try DatabaseRDFQuadFootprintMeter.make(
                workMeter: workMeter,
                stage: stage
            )
        )
    }

    public func footprint(
        of quad: borrowing RDFQuad
    ) throws -> DatabaseExecutionFootprint {
        let footprint = try meter.footprint(of: quad)
        return DatabaseExecutionFootprint(
            rows: footprint.rows,
            bytes: footprint.bytes
        )
    }

    public func shutdown() {
        meter.shutdown()
    }

    deinit {
        shutdown()
    }
}
