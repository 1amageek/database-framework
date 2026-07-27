import DatabaseKit

package struct RDFDatasetReadResolution {
    package let entity: Schema.Entity
    package let indexDescriptor: IndexDescriptor
    package let metadata: RDFDatasetIndexMetadata
}

package enum RDFDatasetReadResolver {
    package static func resolve(
        schema: Schema
    ) throws -> RDFDatasetReadResolution? {
        let candidates = try resolutions(in: schema)
        return candidates.count == 1 ? candidates[0] : nil
    }

    package static func resolve(
        entity: Schema.Entity
    ) throws -> RDFDatasetReadResolution? {
        let candidates = try resolutions(for: entity)
        return candidates.count == 1 ? candidates[0] : nil
    }

    package static func errorMessage(schema: Schema) throws -> String {
        let candidates = try resolutions(in: schema)
        guard !candidates.isEmpty else {
            return "No RDF dataset index found in schema"
        }

        let names = candidates.map { "\($0.entity.name):\($0.indexDescriptor.name)" }
            .sorted()
            .joined(separator: ", ")
        return "RDF dataset source is ambiguous. Available indexes: \(names)"
    }

    private static func resolutions(
        in schema: Schema
    ) throws -> [RDFDatasetReadResolution] {
        var result: [RDFDatasetReadResolution] = []
        for entity in schema.entities {
            result.append(contentsOf: try resolutions(for: entity))
        }
        return result
    }

    private static func resolutions(
        for entity: Schema.Entity
    ) throws -> [RDFDatasetReadResolution] {
        var result: [RDFDatasetReadResolution] = []
        for descriptor in entity.indexDescriptors {
            guard let selection = try RDFDatasetIndexSelection(
                descriptor: descriptor
            ) else {
                continue
            }
            result.append(RDFDatasetReadResolution(
                entity: entity,
                indexDescriptor: descriptor,
                metadata: selection.metadata
            ))
        }
        return result
    }
}
