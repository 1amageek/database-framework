import Testing
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
@testable import VectorIndex

@Suite("Vector Conversion")
struct VectorConversionTests {

    @Test("Encodes Float32 arrays as little-endian binary payloads")
    func encodesFloat32LittleEndianPayloads() {
        let bytes = VectorConversion.floatArrayToBytes([1.0, -2.5, 0.25])

        #expect(bytes == [
            0x00, 0x00, 0x80, 0x3F,
            0x00, 0x00, 0x20, 0xC0,
            0x00, 0x00, 0x80, 0x3E
        ])
    }

    @Test("Decodes validated Float32 payloads")
    func decodesValidatedFloat32Payloads() throws {
        let payload = VectorConversion.floatArrayToBytes([1.0, -2.5, 0.25])
        let decoded = try VectorConversion.materializeFloatArrayForTraining(
            payload,
            expectedCount: 3
        )

        #expect(decoded == [1.0, -2.5, 0.25])
    }

    @Test("Rejects malformed Float32 payload lengths")
    func rejectsMalformedFloat32PayloadLengths() {
        #expect(throws: VectorIndexError.self) {
            _ = try VectorConversion.persistedVector(
                [0x00, 0x00, 0x80],
                expectedCount: 1
            )
        }
    }

    @Test("Round-trips fixed-width integer payloads")
    func roundTripsFixedWidthIntegerPayloads() throws {
        let unsignedValue = UInt64.max - 42
        let signedValue = Int64.min + 42

        #expect(
            try VectorConversion.bytesToUInt64(
                VectorConversion.uint64ToBytes(unsignedValue)
            ) == unsignedValue
        )
        #expect(
            try VectorConversion.bytesToInt64(
                VectorConversion.int64ToBytes(signedValue)
            ) == signedValue
        )
        #expect(throws: ByteConversionError.self) {
            _ = try VectorConversion.bytesToUInt64([0x01, 0x02])
        }
        #expect(throws: ByteConversionError.self) {
            _ = try VectorConversion.bytesToInt64([0x01, 0x02])
        }
    }

    @Test("Round-trips larger payloads without changing order")
    func roundTripsLargerPayloadsWithoutChangingOrder() throws {
        let vector = (0..<257).map { Float($0) / 10.0 - 12.0 }
        let payload = VectorConversion.floatArrayToBytes(vector)
        let decoded = try VectorConversion.materializeFloatArrayForTraining(
            payload,
            expectedCount: vector.count
        )

        #expect(payload.count == vector.count * 4)
        #expect(decoded == vector)
    }

    @Test("Distance functions match expected scalar results")
    func distanceFunctionsMatchExpectedScalarResults() {
        let lhs: [Float] = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let rhs: [Float] = [9, 8, 7, 6, 5, 4, 3, 2, 1]

        let squared = VectorConversion.euclideanDistanceSquared(lhs, rhs)
        let euclidean = VectorConversion.euclideanDistance(lhs, rhs)
        let dot = VectorConversion.dotProductDistance(lhs, rhs)

        #expect(squared == 240)
        #expect(abs(euclidean - Double(squared).squareRoot()) < 0.0001)
        #expect(dot == -165)
    }

    @Test("Finite Float32 inputs produce finite Double distances")
    func finiteFloat32InputsProduceFiniteDoubleDistances() throws {
        let magnitude = Float.greatestFiniteMagnitude
        let query = try Vector(float32: [magnitude, magnitude])
        let candidate = try Vector(float32: [-magnitude, -magnitude])

        for metric in [
            VectorMetric.cosine,
            VectorMetric.euclidean,
            VectorMetric.dotProduct,
        ] {
            let distance = try VectorConversion.distance(
                metric: metric,
                from: query,
                to: candidate
            )
            #expect(distance.isFinite)
        }
    }

    @Test("Cosine treats zero vectors as orthogonal across exact and PQ paths")
    func cosineZeroVectorContract() throws {
        #expect(VectorConversion.cosineDistance([0, 0], [1, 0]) == 1)
        #expect(VectorConversion.cosineDistance([0, 0], [0, 0]) == 1)

        let quantizer = try ProductQuantizer(
            dimensions: 2,
            codebooks: [[[0, 0]]]
        )
        let table = try quantizer.distanceTable(
            for: [1, 0],
            metric: .cosine
        )
        #expect(try quantizer.distance(for: [0], using: table) == 1)
    }

    @Test("Retains the canonical Float32 owner through field extraction")
    func retainsCanonicalFloat32OwnerThroughFieldExtraction() throws {
        let source = try Vector(float32: [1, 2, 3, 4])
        let element = try FieldValue.vector(source).toTupleElement()
        let extracted = try VectorConversion.extractFloat32Vector(
            from: [element]
        )

        let sourceAddress = try #require(source.withFloat32Elements {
            UInt(bitPattern: $0.baseAddress)
        })
        let extractedAddress = try #require(extracted.withFloat32Elements {
            UInt(bitPattern: $0.baseAddress)
        })

        #expect(sourceAddress == extractedAddress)
    }

    @Test("Borrows persisted candidates without materializing element arrays")
    func borrowsPersistedCandidatesWithoutMaterializing() throws {
        let values: [Float] = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let payload = VectorConversion.floatArrayToBytes(values)
        let owner = BorrowCountingVectorBytesOwner(bytes: payload.copyBytes())
        let candidate = try VectorConversion.persistedVector(
            ByteString(retaining: owner),
            expectedCount: values.count
        )
        let query = try Vector(float32: Array(values.reversed()))

        #expect(owner.borrowCount == 0)
        for metric in [
            VectorMetric.cosine,
            VectorMetric.euclidean,
            VectorMetric.dotProduct,
        ] {
            _ = try VectorConversion.distance(
                metric: metric,
                from: query,
                to: candidate
            )
        }
        #expect(owner.borrowCount == 3)
    }

    @Test("Sliced persisted vectors match aligned distance results")
    func slicedPersistedVectorsMatchAlignedResults() throws {
        let queryValues: [Float] = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let candidateValues: [Float] = [9, 8, 7, 6, 5, 4, 3, 2, 1]
        let payload = VectorConversion.floatArrayToBytes(candidateValues)
        let framed = ByteString([0xA5]).appending(contentsOf: payload)
        let candidate = try VectorConversion.persistedVector(
            framed[1..<framed.count],
            expectedCount: candidateValues.count
        )
        let query = try Vector(float32: queryValues)

        let endpoints = try candidate.withElements {
            (elements) throws(VectorIndexError) -> (Float, Float) in
            (
                try elements.element(at: 0),
                try elements.element(at: elements.count - 1)
            )
        }
        #expect(endpoints.0 == candidateValues.first)
        #expect(endpoints.1 == candidateValues.last)

        let expected: [(VectorMetric, Double)] = [
            (.cosine, VectorConversion.cosineDistance(queryValues, candidateValues)),
            (.euclidean, VectorConversion.euclideanDistance(queryValues, candidateValues)),
            (.dotProduct, VectorConversion.dotProductDistance(queryValues, candidateValues)),
        ]
        for (metric, expectedDistance) in expected {
            let distance = try VectorConversion.distance(
                metric: metric,
                from: query,
                to: candidate
            )
            #expect(abs(distance - expectedDistance) < 0.000_001)
        }
    }

    @Test("Rejects non-finite persisted candidate elements")
    func rejectsNonFinitePersistedCandidateElements() throws {
        let candidate = try VectorConversion.persistedVector(
            VectorConversion.floatArrayToBytes([1, .nan, 3]),
            expectedCount: 3
        )
        let query = try Vector(float32: [1, 2, 3])

        #expect(throws: VectorIndexError.self) {
            _ = try VectorConversion.distance(
                metric: .euclidean,
                from: query,
                to: candidate
            )
        }
    }

    @Test("PQ borrows each persisted codebook and code payload once")
    func productQuantizationBorrowsPersistedPayloadsOnce() throws {
        let codebookPayload = VectorConversion.floatArrayToBytes([
            1, 0,
            -1, 0,
        ])
        let codebookOwner = BorrowCountingVectorBytesOwner(
            bytes: codebookPayload.copyBytes()
        )
        let codebook = try VectorConversion.persistedVector(
            ByteString(retaining: codebookOwner),
            expectedCount: 4
        )
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        var codebooksBuilder = try DatabaseRetainedArrayBuilder<
            PersistedVectorView
        >(
            workMeter: meter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                PersistedVectorView.self
            ),
            expectedCount: 1
        )
        let admission = try codebooksBuilder.prepareAppend(
            footprint: DatabaseIntermediateFootprint(rows: 1),
            at: .indexScan
        )
        codebooksBuilder.append(codebook, using: admission)
        let codebooks = try codebooksBuilder.finish()
            .moveToSharedOwnership(at: .indexScan)
        let quantizer = try PersistedProductQuantizer(
            dimensions: 2,
            subquantizerCount: 1,
            centroidCount: 2,
            codebooks: codebooks
        )
        let table = try quantizer.distanceTable(
            for: Vector(float32: [1, 0]),
            metric: .euclidean
        )

        #expect(codebookOwner.borrowCount == 1)

        let codeOwner = BorrowCountingVectorBytesOwner(bytes: [0])
        let distance = try quantizer.distance(
            for: ByteString(retaining: codeOwner),
            using: table
        )

        #expect(codeOwner.borrowCount == 1)
        #expect(distance == 0)
    }

    @Test("Flat reader borrows each persisted vector once")
    func flatReaderBorrowsPersistedVectorOnce() async throws {
        let database = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("zero-copy", "flat").pack())
        let owner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes([1, 0]).copyBytes()
        )
        try await database.withTransaction { transaction in
            try transaction.setValue(
                ByteString(retaining: owner),
                for: subspace.pack(Tuple("item"))
            )
        }
        let borrowCountBeforeSearch = owner.borrowCount

        let results = try await database.withTransaction { transaction in
            try await FlatVectorIndexReader(
                subspace: subspace,
                dimensions: 2,
                metric: .euclidean
            ).search(
                queryVector: Vector(float32: [1, 0]),
                k: 1,
                transaction: transaction
            )
        }

        #expect(results.count == 1)
        #expect(owner.borrowCount - borrowCountBeforeSearch == 1)
    }

    @Test("IVF reader borrows centroid and candidate payloads once")
    func ivfReaderBorrowsPersistedPayloadsOnce() async throws {
        let database = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("zero-copy", "ivf").pack())
        let centroidOwner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes([1, 0]).copyBytes()
        )
        let candidateOwner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes([1, 0]).copyBytes()
        )
        let centroidSubspace = subspace.subspace(
            IVFIndexStorageKey.centroids.rawValue
        )
        let listSubspace = subspace
            .subspace(IVFIndexStorageKey.lists.rawValue)
            .subspace(0)
        let metadataKey = subspace.pack(
            Tuple([IVFIndexStorageKey.metadata.rawValue])
        )
        let metadata = Tuple(
            IVFMetadata.formatVersion,
            Int64(1),
            Int64(2),
            true,
            Int64(1)
        ).pack()
        try await database.withTransaction { transaction in
            try transaction.setValue(metadata, for: metadataKey)
            try transaction.setValue(
                ByteString(retaining: centroidOwner),
                for: centroidSubspace.pack(Tuple(Int64(0)))
            )
            try transaction.setValue(
                ByteString(retaining: candidateOwner),
                for: listSubspace.pack(Tuple("item"))
            )
        }
        let centroidBorrowsBeforeSearch = centroidOwner.borrowCount
        let candidateBorrowsBeforeSearch = candidateOwner.borrowCount

        let results = try await database.withTransaction { transaction in
            try await IVFIndexReader(
                subspace: subspace,
                dimensions: 2,
                metric: .euclidean,
                parameters: IVFParameters(
                    nlist: 1,
                    nprobe: 1,
                    kmeansIterations: 1
                )
            ).search(
                queryVector: Vector(float32: [1, 0]),
                k: 1,
                transaction: transaction
            )
        }

        #expect(results.count == 1)
        #expect(centroidOwner.borrowCount - centroidBorrowsBeforeSearch == 1)
        #expect(candidateOwner.borrowCount - candidateBorrowsBeforeSearch == 1)
    }

    @Test("PQ validation keeps code and vector payloads borrowed")
    func pqValidationKeepsPersistedPayloadsBorrowed() async throws {
        let database = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("zero-copy", "pq").pack())
        var codebookValues = [Float](repeating: 0, count: 256 * 2)
        codebookValues[0] = 1
        let codebookOwner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes(codebookValues).copyBytes()
        )
        let codeOwner = BorrowCountingVectorBytesOwner(bytes: [0])
        let vectorOwner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes([1, 0]).copyBytes()
        )
        let codebookSubspace = subspace.subspace(
            PQIndexStorageKey.codebooks.rawValue
        )
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let vectorsSubspace = subspace.subspace(
            PQIndexStorageKey.vectors.rawValue
        )
        let metadataKey = subspace.pack(
            Tuple([PQIndexStorageKey.metadata.rawValue])
        )
        let metadata = Tuple(
            PQMetadata.formatVersion,
            Int64(1),
            Int64(2),
            true,
            Int64(1)
        ).pack()
        try await database.withTransaction { transaction in
            try transaction.setValue(metadata, for: metadataKey)
            try transaction.setValue(
                ByteString(retaining: codebookOwner),
                for: codebookSubspace.pack(Tuple(Int64(0)))
            )
            try transaction.setValue(
                ByteString(retaining: codeOwner),
                for: codesSubspace.pack(Tuple("item"))
            )
            try transaction.setValue(
                ByteString(retaining: vectorOwner),
                for: vectorsSubspace.pack(Tuple("item"))
            )
        }
        let codebookBorrowsBeforeSearch = codebookOwner.borrowCount
        let codeBorrowsBeforeSearch = codeOwner.borrowCount
        let vectorBorrowsBeforeSearch = vectorOwner.borrowCount

        let reader = try PQIndexReader(
            subspace: subspace,
            dimensions: 2,
            metric: .euclidean,
            parameters: PQParameters(m: 1, niter: 1)
        )
        let results = try await database.withTransaction { transaction in
            try await reader.search(
                queryVector: Vector(float32: [1, 0]),
                k: 1,
                transaction: transaction
            )
        }

        #expect(results.count == 1)
        #expect(codebookOwner.borrowCount - codebookBorrowsBeforeSearch == 2)
        #expect(codeOwner.borrowCount - codeBorrowsBeforeSearch == 1)
        #expect(vectorOwner.borrowCount - vectorBorrowsBeforeSearch == 1)
    }

    @Test("Canonical entity validation borrows persisted vector storage")
    func canonicalEntityValidationBorrowsPersistedVectorStorage() async throws {
        let database = InMemoryEngine()
        let subspace = Subspace(
            prefix: Tuple("zero-copy", "canonical-vector").pack()
        )
        let label: UInt64 = 0
        let vectorOwner = BorrowCountingVectorBytesOwner(
            bytes: VectorConversion.floatArrayToBytes([1, 0]).copyBytes()
        )
        try await database.withTransaction { transaction in
            try transaction.setValue(
                HNSWLabelCodec.tuple(label).pack(),
                for: subspace.subspace("l").pack(Tuple("item"))
            )
            try transaction.setValue(
                ByteString(retaining: vectorOwner),
                for: subspace.subspace("v").pack(
                    HNSWLabelCodec.tuple(label)
                )
            )
        }
        let validator = VectorCanonicalStateValidator(
            indexSubspace: subspace,
            fieldName: "embedding",
            dimensions: 2,
            algorithm: .hnsw(.default)
        )
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let matchingReservation = try meter.reserveIntermediate(
            bytes: 1_024,
            at: .storageRow
        )
        let matching = DatabaseRetainedPersistedModels.Entry(
            model: try PersistedModel(
                HNSWDocument(
                    id: "item",
                    title: "Matching",
                    embedding: Vector(float32: [1, 0])
                )
            ),
            retainedModelFootprint: DatabaseIntermediateFootprint(
                rows: 1,
                bytes: 1_024
            ),
            queryRowFootprint: DatabaseIntermediateFootprint(rows: 1),
            reservation: matchingReservation
        )
        let matchingModels = try retainedModels(
            matching,
            workMeter: meter
        )
        try await database.withTransaction { transaction in
            let isValid = try await validator.validate(
                primaryKey: Tuple("item"),
                entities: matchingModels,
                position: 0,
                transaction: transaction,
                snapshot: false,
                workMeter: meter
            )
            #expect(isValid)
        }

        let mismatchingReservation = try meter.reserveIntermediate(
            bytes: 1_024,
            at: .storageRow
        )
        let mismatching = DatabaseRetainedPersistedModels.Entry(
            model: try PersistedModel(
                HNSWDocument(
                    id: "item",
                    title: "Mismatching",
                    embedding: Vector(float32: [0, 1])
                )
            ),
            retainedModelFootprint: DatabaseIntermediateFootprint(
                rows: 1,
                bytes: 1_024
            ),
            queryRowFootprint: DatabaseIntermediateFootprint(rows: 1),
            reservation: mismatchingReservation
        )
        let mismatchingModels = try retainedModels(
            mismatching,
            workMeter: meter
        )
        await #expect(throws: VectorIndexError.self) {
            try await database.withTransaction { transaction in
                _ = try await validator.validate(
                    primaryKey: Tuple("item"),
                    entities: mismatchingModels,
                    position: 0,
                    transaction: transaction,
                    snapshot: false,
                    workMeter: meter
                )
            }
        }
        #expect(vectorOwner.borrowCount == 2)
    }

    private func retainedModels(
        _ entry: DatabaseRetainedPersistedModels.Entry,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPersistedModels {
        var builder = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPersistedModels.Entry?
        >(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPersistedModels.Entry?.self
            ),
            expectedCount: 1
        )
        let admission = try builder.prepareAppend(
            footprint: DatabaseIntermediateFootprint(rows: 1),
            at: .storageRow
        )
        builder.append(entry, using: admission)
        return try DatabaseRetainedPersistedModels(buffer: builder.finish())
    }
}

private final class BorrowCountingVectorBytesOwner: ByteStringOwner {
    let bytes: [UInt8]
    private let state = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }
    var retainedByteCount: Int? { bytes.capacity }
    var isStorageSelfContained: Bool { true }

    var borrowCount: Int {
        state.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}
