import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import StorageKit
import Synchronization
import Testing
@testable import GraphIndex

@Suite("SPARQL SubSelect cache storage sharing")
struct SPARQLSubqueryCacheStorageSharingTests {
    @Test("Cache and current reader share the original binding buffer")
    func cacheAndCurrentReaderShareBindingBuffer() throws {
        let meter = makeMeter(maximumIntermediateBytes: 4_096)
        let owner = CountingSubqueryByteOwner([1, 2, 3, 4])

        do {
            let cache = try SPARQLSubqueryResultCache.make(workMeter: meter)
            let key = makeKey(occurrenceIdentifier: 1)
            let original = try makeRetainedBindings(
                VariableBinding([
                    "?payload": .bytes(ByteString(retaining: owner)),
                ]),
                workMeter: meter
            )
            let originalAddress = bindingBufferAddress(original)

            let current = try cache.store(consume original, for: key)
            let currentAddress = bindingBufferAddress(current)
            guard let cached = cache.value(for: key) else {
                Issue.record("Expected one cached relation")
                return
            }
            let cachedAddress = bindingBufferAddress(cached)
            let originalByteAddress = owner.bufferAddress
            let retainedOriginalByteBuffer = cached.withElement(at: 0) {
                binding in
                guard case .bytes(let cachedBytes) = binding["?payload"] else {
                    return false
                }
                return cachedBytes.withUnsafeBytes { bytes in
                    bytes.baseAddress.map { UInt(bitPattern: $0) }
                        == originalByteAddress
                        && bytes.count == 4
                }
            }

            #expect(originalAddress != nil)
            #expect(currentAddress == originalAddress)
            #expect(cachedAddress == originalAddress)
            #expect(retainedOriginalByteBuffer)
            #expect(owner.borrowCount == 1)
            #expect(meter.retainedIntermediateRows == 1)

            let retainedBytes = meter.retainedIntermediateBytes
            guard let secondLookup = cache.value(for: key) else {
                Issue.record("Expected a second cached relation handle")
                return
            }
            #expect(bindingBufferAddress(secondLookup) == originalAddress)
            #expect(meter.retainedIntermediateRows == 1)
            #expect(meter.retainedIntermediateBytes == retainedBytes)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Existing-key admission discards only the incoming relation")
    func existingKeyDiscardsIncomingRelation() throws {
        let meter = makeMeter(maximumIntermediateBytes: 8_192)

        do {
            let cache = try SPARQLSubqueryResultCache.make(workMeter: meter)
            let key = makeKey(occurrenceIdentifier: 2)
            let first = try makeRetainedBindings(
                VariableBinding(["?value": .int64(1)]),
                workMeter: meter
            )
            let firstAddress = bindingBufferAddress(first)
            do {
                let current = try cache.store(consume first, for: key)
                #expect(bindingBufferAddress(current) == firstAddress)
            }

            let retainedRowsAfterFirstStore = meter.retainedIntermediateRows
            let retainedBytesAfterFirstStore = meter.retainedIntermediateBytes
            let second = try makeRetainedBindings(
                VariableBinding(["?value": .int64(2)]),
                workMeter: meter
            )
            let secondAddress = bindingBufferAddress(second)
            let peakBytesBeforeStore = meter.peakIntermediateBytes

            do {
                let existing = try cache.store(consume second, for: key)
                #expect(bindingBufferAddress(existing) == firstAddress)
                #expect(bindingBufferAddress(existing) != secondAddress)
            }

            #expect(meter.retainedIntermediateRows == retainedRowsAfterFirstStore)
            #expect(meter.retainedIntermediateBytes == retainedBytesAfterFirstStore)
            #expect(meter.peakIntermediateBytes == peakBytesBeforeStore)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cache preserves shared slice bounds without materialization")
    func cachePreservesSharedSliceBounds() throws {
        let meter = makeMeter(maximumIntermediateBytes: 8_192)

        do {
            let cache = try SPARQLSubqueryResultCache.make(workMeter: meter)
            let key = makeKey(occurrenceIdentifier: 4)
            var builder = try SPARQLRetainedBindingBuilder.make(
                workMeter: meter,
                stage: .resultMaterialization,
                expectedCount: 3
            )
            for value: Int64 in 1...3 {
                try builder.append(
                    VariableBinding(["?value": .int64(value)])
                )
            }
            let original = builder.finish()
            let originalAddress = bindingBufferAddress(original)
            let shared = try (consume original).sharing(
                at: .subqueryCache
            )
            let slice = (consume shared).applyingSlice(offset: 1, limit: 1)

            #expect(slice.count == 1)
            #expect(bindingBufferAddress(slice) == originalAddress)

            let current = try cache.store(consume slice, for: key)
            guard let cached = cache.value(for: key) else {
                Issue.record("Expected the shared slice in the cache")
                return
            }
            let currentValue = current.withElement(at: 0) { binding in
                binding["?value"]
            }
            let cachedValue = cached.withElement(at: 0) { binding in
                binding["?value"]
            }

            #expect(current.count == 1)
            #expect(cached.count == 1)
            #expect(currentValue == .int64(2))
            #expect(cachedValue == .int64(2))
            #expect(bindingBufferAddress(current) == originalAddress)
            #expect(bindingBufferAddress(cached) == originalAddress)
            #expect(meter.retainedIntermediateRows == 3)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Failed entry admission leaves the cache empty and releases input")
    func failedEntryAdmissionRollsBackInputOwnership() throws {
        let maximumBytes: UInt64 = 4_096
        let sharedOwnerBytes: UInt64 = 64
        let entryAdmissionBytes: UInt64 = 192
        let meter = makeMeter(maximumIntermediateBytes: maximumBytes)

        do {
            let cache = try SPARQLSubqueryResultCache.make(workMeter: meter)
            let key = makeKey(occurrenceIdentifier: 3)
            let input = try makeRetainedBindings(
                VariableBinding(["?value": .int64(3)]),
                workMeter: meter
            )
            let retainedBeforeBlocker = meter.retainedIntermediateBytes
            #expect(retainedBeforeBlocker + sharedOwnerBytes <= maximumBytes)
            let blockerBytes = maximumBytes
                - retainedBeforeBlocker
                - sharedOwnerBytes
            let blocker = try meter.reserveIntermediate(
                bytes: blockerBytes,
                at: .subqueryCache
            )

            var receivedError: DatabaseWorkLimitError?
            do {
                _ = try cache.store(consume input, for: key)
                Issue.record("Expected cache entry admission to fail")
            } catch let error as DatabaseWorkLimitError {
                receivedError = error
            } catch {
                Issue.record("Unexpected cache admission error: \(error)")
            }

            #expect(
                receivedError == .maximumIntermediateBytes(
                    stage: .subqueryCache,
                    consumed: maximumBytes,
                    requested: entryAdmissionBytes,
                    maximum: maximumBytes
                )
            )
            if let unexpected = cache.value(for: key) {
                _ = consume unexpected
                Issue.record("Failed admission must not publish a cache entry")
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(
                meter.retainedIntermediateBytes == 64 + blockerBytes
            )

            blocker.release()
            #expect(meter.retainedIntermediateBytes == 64)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cache owner admission happens before construction")
    func cacheOwnerAdmissionFailureDoesNotLeak() {
        let meter = makeMeter(maximumIntermediateBytes: 63)

        #expect {
            try SPARQLSubqueryResultCache.make(workMeter: meter)
        } throws: { error in
            error as? DatabaseWorkLimitError
                == .maximumIntermediateBytes(
                    stage: .subqueryCache,
                    consumed: 0,
                    requested: 64,
                    maximum: 63
                )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Mutation versions reject wraparound")
    func mutationVersionRejectsWraparound() throws {
        #expect(
            try SPARQLSubqueryResultCache.nextMutationVersion(
                after: UInt64.max - 1
            ) == UInt64.max
        )
        #expect {
            try SPARQLSubqueryResultCache.nextMutationVersion(
                after: UInt64.max
            )
        } throws: { error in
            error as? SPARQLSubqueryCacheError == .mutationVersionOverflow
        }
    }
}

private func makeMeter(
    maximumIntermediateBytes: UInt64
) -> DatabaseWorkMeter {
    DatabaseWorkMeter(
        budget: ExecutionBudget(
            maximumRows: 10,
            maximumWorkUnits: 100,
            maximumIntermediateRows: 10,
            maximumIntermediateBytes: maximumIntermediateBytes,
            timeoutMilliseconds: 30_000
        )
    )
}

private func makeKey(
    occurrenceIdentifier: UInt64
) -> SPARQLSubqueryCacheKey {
    SPARQLSubqueryCacheKey(
        occurrenceIdentifier: occurrenceIdentifier,
        graphScope: .defaultGraph
    )
}

private func makeRetainedBindings(
    _ binding: consuming VariableBinding,
    workMeter: DatabaseWorkMeter
) throws -> SPARQLRetainedBindings {
    var builder = try SPARQLRetainedBindingBuilder.make(
        workMeter: workMeter,
        stage: .resultMaterialization,
        expectedCount: 1
    )
    try builder.append(binding)
    return builder.finish()
}

private func bindingBufferAddress(
    _ bindings: borrowing SPARQLRetainedBindings
) -> UInt? {
    switch bindings {
    case .empty:
        return nil
    case .unique(let storage):
        return storage.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        }
    case .shared(let storage):
        return storage.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        }
    case .sharedSlice(let storage, _):
        return storage.withSpan { span in
            span.withUnsafeBufferPointer { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        }
    }
}

private final class CountingSubqueryByteOwner: ByteStringOwner, Sendable {
    private let bytes: [UInt8]
    private let borrowState = Mutex(0)

    init(_ bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    var borrowCount: Int {
        borrowState.withLock { $0 }
    }

    var bufferAddress: UInt? {
        bytes.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        borrowState.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}
