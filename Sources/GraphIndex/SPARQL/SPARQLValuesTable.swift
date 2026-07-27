import DatabaseTypes
import DatabaseKit

/// A compiled SPARQL VALUES relation backed by one row-major cell buffer.
public struct SPARQLValuesTable: Sendable, Equatable {
    public let variables: [String]
    public let rowCount: Int
    package let alwaysBoundVariables: Set<String>
    package let cells: [FieldValue?]

    package init(
        variables: consuming [String],
        rowCount: Int,
        cells: consuming [FieldValue?]
    ) {
        let ownedVariables = consume variables
        let ownedCells = consume cells
        self.variables = ownedVariables
        self.rowCount = rowCount
        self.cells = ownedCells

        guard rowCount > 0 else {
            self.alwaysBoundVariables = []
            return
        }
        var alwaysBound = Set(ownedVariables)
        for column in ownedVariables.indices {
            for row in 0..<rowCount
            where ownedCells[row * ownedVariables.count + column] == nil {
                alwaysBound.remove(ownedVariables[column])
                break
            }
        }
        self.alwaysBoundVariables = alwaysBound
    }

    package func value(row: Int, column: Int) -> FieldValue? {
        cells[row * variables.count + column]
    }
}
