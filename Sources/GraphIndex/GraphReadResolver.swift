import Foundation
import Core

struct GraphReadResolution {
    let entity: Schema.Entity
    let indexDescriptor: IndexDescriptor
    let kind: any AnyGraphIndexKind
}

enum GraphReadResolver {
    static func resolve(
        graphName: String?,
        schema: Schema
    ) -> GraphReadResolution? {
        let candidates = graphResolutions(in: schema)

        if let graphName,
           let matched = candidates.first(where: { matches($0, graphName: graphName) }) {
            return matched
        }

        if candidates.count == 1 {
            return candidates[0]
        }

        return nil
    }

    static func errorMessage(
        graphName: String?,
        schema: Schema
    ) -> String {
        let candidates = graphResolutions(in: schema)
        guard !candidates.isEmpty else {
            if let graphName {
                return "Graph '\(graphName)' not found in schema"
            }
            return "No graph-capable entity found in schema"
        }

        let names = candidates.map { "\($0.entity.name):\($0.indexDescriptor.name)" }
            .sorted()
            .joined(separator: ", ")
        if let graphName {
            return "Graph '\(graphName)' could not be resolved. Available graph-capable indexes: \(names)"
        }
        return "Graph source is ambiguous. Available graph-capable indexes: \(names)"
    }

    private static func graphResolutions(in schema: Schema) -> [GraphReadResolution] {
        schema.entities.flatMap { entity in
            entity.indexDescriptors.compactMap { descriptor in
                guard let kind = descriptor.kind as? any AnyGraphIndexKind else {
                    return nil
                }
                return GraphReadResolution(entity: entity, indexDescriptor: descriptor, kind: kind)
            }
        }
    }

    private static func matches(_ resolution: GraphReadResolution, graphName: String) -> Bool {
        resolution.entity.name.caseInsensitiveCompare(graphName) == .orderedSame
        || resolution.indexDescriptor.name.caseInsensitiveCompare(graphName) == .orderedSame
    }
}
