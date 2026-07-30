import DatabaseEngine
import DatabaseTypes

/// The vector module's execution policy after decoding canonical runtime options.
struct VectorRuntimePolicy: Sendable {
    let algorithm: VectorAlgorithm
    let subspaceKey: String?

    static func resolve(
        in configurations: [any IndexRuntimeConfiguration]
    ) throws -> VectorRuntimePolicy? {
        guard let configuration = configurations.first(where: {
            $0.kindIdentifier == VectorIndexSpecification.identifier
        }) else {
            return nil
        }

        let options = try configuration.executionOptions
        guard let algorithm = options["algorithm"]?.stringValue else {
            throw VectorIndexError.invalidArgument(
                "Vector execution policy requires an algorithm"
            )
        }

        let resolvedAlgorithm: VectorAlgorithm
        switch algorithm {
        case "flat":
            resolvedAlgorithm = .flat
        case "hnsw":
            resolvedAlgorithm = .hnsw(VectorHNSWParameters(
                m: try positiveInteger("m", in: options),
                efConstruction: try positiveInteger(
                    "efConstruction",
                    in: options
                ),
                efSearch: try positiveInteger("efSearch", in: options)
            ))
        case "ivf":
            let nlist = try positiveInteger("nlist", in: options)
            let nprobe = try positiveInteger("nprobe", in: options)
            guard nprobe <= nlist else {
                throw VectorIndexError.invalidArgument(
                    "Vector IVF nprobe cannot exceed nlist"
                )
            }
            resolvedAlgorithm = .ivf(VectorIVFParameters(
                nlist: nlist,
                nprobe: nprobe,
                kmeansIterations: try positiveInteger(
                    "kmeansIterations",
                    in: options
                )
            ))
        case "pq":
            resolvedAlgorithm = .pq(VectorPQParameters(
                m: try positiveInteger("m", in: options),
                niter: try positiveInteger("niter", in: options)
            ))
        default:
            throw VectorIndexError.invalidArgument(
                "Unknown vector algorithm policy"
            )
        }

        return VectorRuntimePolicy(
            algorithm: resolvedAlgorithm,
            subspaceKey: configuration.subspaceKey
        )
    }

    private static func positiveInteger(
        _ name: String,
        in options: FieldObject
    ) throws -> Int {
        guard let value = options[name]?.int64Value,
              value > 0,
              let result = Int(exactly: value) else {
            throw VectorIndexError.invalidArgument(
                "Vector execution policy has an invalid positive integer"
            )
        }
        return result
    }
}
