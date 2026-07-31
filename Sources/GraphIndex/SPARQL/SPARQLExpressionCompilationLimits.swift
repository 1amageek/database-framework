import DatabaseKit

/// Resource limits applied when QueryIR is compiled into SPARQL expression
/// algebra.
///
/// Depth, node, and collection budgets are derived from the request's canonical
/// `QueryStructuralLimits`. The SPARQL-specific string limit supplements that
/// shared structural authority.
public struct SPARQLExpressionCompilationLimits: Sendable, Equatable {
    public let structuralLimits: QueryStructuralLimits
    public let maximumStringUTF8Count: UInt64

    public init(
        structuralLimits: QueryStructuralLimits = .default,
        maximumStringUTF8Count: UInt64 = 1_048_576
    ) {
        self.structuralLimits = structuralLimits
        self.maximumStringUTF8Count = maximumStringUTF8Count
    }

    public static let `default` = Self()

    public var maximumDepth: UInt64 {
        structuralLimits.maximumNestingDepth
    }

    public var maximumNodes: UInt64 {
        structuralLimits.maximumTotalNodes
    }

    public var maximumFunctionArguments: UInt64 {
        structuralLimits.maximumCollectionElements
    }

    public var maximumCollectionElements: UInt64 {
        structuralLimits.maximumCollectionElements
    }
}
