import DatabaseKit
import DatabaseKit

package struct PropertyGraphReadResolution {
    package let entity: Schema.Entity
    package let indexDescriptor: IndexDescriptor
    package let metadata: PropertyGraphIndexMetadata
}

package enum PropertyGraphReadResolver {
    package static func resolve(
        graphName: String?,
        schema: Schema
    ) throws -> PropertyGraphReadResolution? {
        let candidates = try resolutions(in: schema)

        if let graphName {
            let matches = candidates.filter {
                matches($0, graphName: graphName)
            }
            guard matches.count == 1 else { return nil }
            return matches[0]
        }

        if candidates.count == 1 {
            return candidates[0]
        }

        return nil
    }

    package static func errorMessage(
        graphName: String?,
        schema: Schema
    ) throws -> String {
        let candidates = try resolutions(in: schema)
        guard !candidates.isEmpty else {
            if let graphName {
                return "Property graph '\(graphName)' not found in schema"
            }
            return "No property-graph index found in schema"
        }

        let names = candidates.map { "\($0.entity.name):\($0.indexDescriptor.name)" }
            .sorted()
            .joined(separator: ", ")
        if let graphName {
            return "Property graph '\(graphName)' could not be resolved. Available indexes: \(names)"
        }
        return "Property graph source is ambiguous. Available indexes: \(names)"
    }

    private static func resolutions(
        in schema: Schema
    ) throws -> [PropertyGraphReadResolution] {
        var resolutions: [PropertyGraphReadResolution] = []
        for entity in schema.entities {
            for descriptor in entity.indexDescriptors
            where descriptor.kindIdentifier == "graph" {
                let metadata = try PropertyGraphIndexMetadata(
                    canonical: descriptor.kind
                )
                resolutions.append(PropertyGraphReadResolution(
                    entity: entity,
                    indexDescriptor: descriptor,
                    metadata: metadata
                ))
            }
        }
        return resolutions
    }

    private static func matches(
        _ resolution: PropertyGraphReadResolution,
        graphName: String
    ) -> Bool {
        resolution.entity.name == graphName
            || resolution.indexDescriptor.name == graphName
    }
}
