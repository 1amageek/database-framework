import DatabaseEngine
import DatabaseWire
import QueryIR

public struct CanonicalPreparedStatementMutation: Sendable {
    enum Payload: Sendable {
        case statement(QueryStatement)
        case sparql(PreparedSPARQLUpdateRequest)
    }

    let payload: Payload
    let workMeter: DatabaseWorkMeter
    let structuralLimits: QueryStructuralLimits

    init(
        payload: Payload,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) {
        self.payload = payload
        self.workMeter = workMeter
        self.structuralLimits = structuralLimits
    }
}
