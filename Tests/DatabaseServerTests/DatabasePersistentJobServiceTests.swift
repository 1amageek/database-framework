import Core
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseValue
import DatabaseWire
import StorageKit
import Synchronization
import Testing

@Suite("Persistent Database Job Service Tests", .serialized)
struct DatabasePersistentJobServiceTests {
    @Test("Job resumes across runtime recreation and returns its typed payload")
    func resumesAcrossRuntimeRecreation() async throws {
        let compilationCounter = CompilationCounter()
        let jobContext = try await makePersistentJobServiceContext(
            operation: TwoSliceResumableOperation(
                compilationCounter: compilationCounter
            )
        )
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-resume"
        )
        let firstService = try await jobContext.makeService()

        let started = try await firstService.start(
            request,
            context: context
        ).response
        let replayed = try await firstService.start(
            request,
            context: context
        ).response
        #expect(replayed.jobID == started.jobID)

        try await firstService.runScheduledWork()
        let afterFirstSlice = try await firstService.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(afterFirstSlice.state == .pending)
        #expect(afterFirstSlice.completedWorkUnits == 1)
        #expect(afterFirstSlice.totalWorkUnits == 2)

        let recreatedService = try await jobContext.makeService()
        try await recreatedService.runScheduledWork()
        let completed = try await recreatedService.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(completed.state == .succeeded)
        #expect(completed.completedWorkUnits == 2)
        #expect(completed.executionCount == 2)
        #expect(completed.currentSliceAttempt == 1)

        let result = try await recreatedService.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .succeeded(
            let job,
            let payloadPage,
            let totalResponseBytes,
            let responseDigest,
            let continuation
        ) = result else {
            Issue.record("Expected a successful persistent job result")
            return
        }
        #expect(job == started.job)
        #expect(totalResponseBytes == UInt64(payloadPage.count))
        #expect(responseDigest.bytes.count == DatabaseJobResultDigest.byteCount)
        #expect(continuation == nil)
        #expect(
            try DatabaseEnvelopeCodec.decode(
                JobStepValue.self,
                from: payloadPage
            ) == JobStepValue(9)
        )
        #expect(compilationCounter.count == 1)

        let storedStep = try await jobContext.container.engine.withTransaction {
            transaction in
            try await transaction.getValue(for: TwoSliceResumableOperation.stateKey)
        }
        #expect(storedStep == [2])
        #expect(await jobContext.scheduler.scheduledCount() >= 2)
    }

    @Test("Large job results are paged without exceeding the chunk limit")
    func largeResultsArePagedAndDigestVerified() async throws {
        let payloadByteCount = 1_200_000
        let expectedPayload = DatabaseBytes(
            [UInt8](repeating: 0xab, count: payloadByteCount)
        )
        let jobContext = try await makePersistentJobServiceContext(
            operation: LargeResultResumableOperation(
                payload: expectedPayload
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(7),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "large-job-result"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        try await service.runScheduledWork()

        var continuation: JobResultOperation.Continuation?
        var pageCount = 0
        var receivedByteCount = 0
        var expectedDigest: DatabaseJobResultDigest?
        var digest = DatabaseJobResultDigestAccumulator(
            operation: job.operation
        )
        repeat {
            let result = try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: continuation
                ),
                context: context
            )
            guard case .succeeded(
                let responseJob,
                let page,
                let totalResponseBytes,
                let responseDigest,
                let nextContinuation
            ) = result else {
                Issue.record("Expected a successful result page")
                return
            }
            #expect(responseJob == job)
            #expect(totalResponseBytes == UInt64(payloadByteCount + 4))
            #expect(
                page.count
                    <= DatabasePersistentJobStorageLimits
                        .maximumResultChunkBytes
            )
            if let expectedDigest {
                #expect(responseDigest == expectedDigest)
            } else {
                expectedDigest = responseDigest
            }
            digest.update(page)
            receivedByteCount += page.count
            pageCount += 1
            continuation = nextContinuation
        } while continuation != nil

        #expect(pageCount == 3)
        #expect(receivedByteCount == payloadByteCount + 4)
        let finalExpectedDigest = try #require(expectedDigest)
        #expect(digest.finalize() == finalExpectedDigest)
        var independentlyComputedDigest = DatabaseJobResultDigestAccumulator(
            operation: job.operation
        )
        let exactPayloadByteCount = try #require(
            UInt32(exactly: payloadByteCount)
        )
        independentlyComputedDigest.update(
            DatabaseBytes([
                UInt8(truncatingIfNeeded: exactPayloadByteCount),
                UInt8(truncatingIfNeeded: exactPayloadByteCount >> 8),
                UInt8(truncatingIfNeeded: exactPayloadByteCount >> 16),
                UInt8(truncatingIfNeeded: exactPayloadByteCount >> 24),
            ])
        )
        independentlyComputedDigest.update(expectedPayload)
        #expect(
            independentlyComputedDigest.finalize() == finalExpectedDigest
        )
    }

    @Test("Result continuations are bound to one job and one digest")
    func resultContinuationBindingIsStrict() async throws {
        let expectedPayload = DatabaseBytes(
            [UInt8](repeating: 0x7a, count: 600_000)
        )
        let jobContext = try await makePersistentJobServiceContext(
            operation: LargeResultResumableOperation(
                payload: expectedPayload
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(7),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "result-continuation-binding"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let jobID = job.jobID
        try await service.runScheduledWork()
        let firstPage = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .succeeded(
            _,
            _,
            _,
            let responseDigest,
            let continuation
        ) = firstPage,
        let continuation else {
            Issue.record("Expected a paged result")
            return
        }

        let otherJobContinuation = try JobResultOperation.Continuation(
            job: DatabaseJobIdentity(
                jobID: DatabaseUUID(high: jobID.high ^ 1, low: jobID.low),
                operation: job.operation
            ),
            responseDigest: responseDigest,
            nextChunkIndex: continuation.nextChunkIndex
        )
        await #expect(
            throws: DatabaseJobRuntimeError.invalidResultContinuation
        ) {
            try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: otherJobContinuation
                ),
                context: context
            )
        }

        let otherKindJob = DatabaseJobIdentity(
            jobID: jobID,
            operation: try DatabaseJobOperationIdentifier(
                family: job.operation.family,
                kind: "database.test.other-kind"
            )
        )
        let otherKindContinuation = try JobResultOperation.Continuation(
            job: otherKindJob,
            responseDigest: responseDigest,
            nextChunkIndex: continuation.nextChunkIndex
        )
        await #expect(
            throws: DatabaseJobRuntimeError.invalidResultContinuation
        ) {
            try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: otherKindContinuation
                ),
                context: context
            )
        }

        let otherDigest = try DatabaseJobResultDigest(
            [UInt8](repeating: 0, count: DatabaseJobResultDigest.byteCount)
        )
        let otherDigestContinuation = try JobResultOperation.Continuation(
            job: job,
            responseDigest: otherDigest,
            nextChunkIndex: continuation.nextChunkIndex
        )
        await #expect(
            throws: DatabaseJobRuntimeError.invalidResultContinuation
        ) {
            try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: otherDigestContinuation
                ),
                context: context
            )
        }
    }

    @Test("Lifecycle requests bind a UUID to its exact operation kind")
    func lifecycleRejectsSameUUIDWithDifferentKind() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-operation-binding"
        )
        let service = try await jobContext.makeService()
        let persistedJob = try await service.start(
            request,
            context: context
        ).response.job
        let mismatchedJob = DatabaseJobIdentity(
            jobID: persistedJob.jobID,
            operation: try DatabaseJobOperationIdentifier(
                family: persistedJob.operation.family,
                kind: "database.test.other-kind"
            )
        )
        let expectedError = DatabaseJobRuntimeError.jobOperationMismatch(
            expected: mismatchedJob.operation,
            actual: persistedJob.operation
        )

        await #expect(throws: expectedError) {
            try await service.status(
                JobStatusOperation.Request(job: mismatchedJob),
                context: context
            )
        }
        await #expect(throws: expectedError) {
            try await service.result(
                JobResultOperation.Request(job: mismatchedJob),
                context: context
            )
        }

        let cancelRequest = JobCancelOperation.Request(job: mismatchedJob)
        let cancelContext = try operationContext(
            container: jobContext.container,
            operation: JobCancelOperation.self,
            request: cancelRequest,
            idempotencyKey: "job-operation-binding-cancel"
        )
        await #expect(throws: expectedError) {
            _ = try await service.cancel(
                cancelRequest,
                context: cancelContext
            )
        }
    }

    @Test("Missing and corrupted result chunks fail explicitly")
    func missingAndCorruptedResultChunksAreRejected() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: LargeResultResumableOperation(
                payload: DatabaseBytes(
                    [UInt8](repeating: 0x6b, count: 600_000)
                )
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(7),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "result-chunk-integrity"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let jobID = job.jobID
        try await service.runScheduledWork()
        let firstPage = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .succeeded(_, _, _, _, let continuation) = firstPage,
              let continuation else {
            Issue.record("Expected a paged result")
            return
        }
        let chunkKey = try await persistentJobChunkKey(
            container: jobContext.container,
            jobID: jobID,
            index: continuation.nextChunkIndex
        )
        try await jobContext.container.engine.withTransaction { transaction in
            try transaction.clear(key: chunkKey)
        }

        await #expect(
            throws: DatabaseJobRuntimeError.resultChunkMissing(
                jobID: jobID,
                index: continuation.nextChunkIndex
            )
        ) {
            try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: continuation
                ),
                context: context
            )
        }

        try await jobContext.container.engine.withTransaction { transaction in
            try transaction.setValue([0x00], for: chunkKey)
        }
        await #expect(throws: DatabaseJobRuntimeError.corruptedResult) {
            try await service.result(
                JobResultOperation.Request(
                    job: job,
                    continuation: continuation
                ),
                context: context
            )
        }
    }

    @Test("Persistent job storage limits reject unsafe configurations")
    func storageLimitsRejectUnsafeConfigurations() throws {
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_577
            ).validate()
        }
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                resultChunkBytes: 512 * 1_024 + 1
            ).validate()
        }
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                maximumResultBytes: 4 * 1_024 * 1_024,
                resultChunkBytes: 1
            ).validate(wireLimits: .default)
        }
        try DatabasePersistentJobStorageLimits().validate(
            wireLimits: .default
        )
    }

    @Test("Missing persistent job components are typed corruption failures")
    func missingPersistentComponentsAreRejected() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let service = try await jobContext.makeService()
        let cases: [(String, DatabaseJobRuntimeError)] = [
            ("specifications", .corruptedSpecification),
            ("plans", .corruptedPlan),
            ("states", .corruptedState),
        ]
        for (index, testCase) in cases.enumerated() {
            let request = try jobRequest()
            let context = try operationContext(
                container: jobContext.container,
                request: request,
                idempotencyKey: "missing-component-\(index)"
            )
            let job = try await service.start(
                request,
                context: context
            ).response.job
            let jobID = job.jobID
            let key = try await persistentJobKey(
                container: jobContext.container,
                component: testCase.0,
                jobID: jobID
            )
            try await jobContext.container.engine.withTransaction {
                transaction in
                try transaction.clear(key: key)
            }
            await #expect(throws: testCase.1) {
                try await service.status(
                    JobStatusOperation.Request(job: job),
                    context: context
                )
            }
        }
    }

    @Test("Succeeded status rejects a missing result manifest")
    func succeededStatusRequiresResultManifest() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "missing-result-manifest"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let jobID = job.jobID
        try await service.runScheduledWork()
        try await service.runScheduledWork()
        let manifestKey = try await persistentJobKey(
            container: jobContext.container,
            component: "result-manifests",
            jobID: jobID
        )
        try await jobContext.container.engine.withTransaction { transaction in
            try transaction.clear(key: manifestKey)
        }

        await #expect(throws: DatabaseJobRuntimeError.corruptedResult) {
            try await service.status(
                JobStatusOperation.Request(job: job),
                context: context
            )
        }
    }

    @Test("Persistent job operation identifiers cannot be tampered")
    func persistentOperationTamperingIsRejected() async throws {
        let alternateOperation = try DatabaseJobOperationIdentifier(
            family: .maintenanceExecute,
            kind: "database.test.tampered"
        )

        do {
            let jobContext = try await makePersistentJobServiceContext()
            let request = try jobRequest()
            let context = try operationContext(
                container: jobContext.container,
                request: request,
                idempotencyKey: "tampered-specification-operation"
            )
            let service = try await jobContext.makeService()
            let job = try await service.start(
                request,
                context: context
            ).response.job
            try await replacePersistentComponent(
                container: jobContext.container,
                component: "specifications",
                jobID: job.jobID,
                as: DatabasePersistentJobSpecification.self
            ) { specification in
                DatabasePersistentJobSpecification(
                    jobID: specification.jobID,
                    operation: alternateOperation,
                    requestDigest: specification.requestDigest,
                    requestID: specification.requestID,
                    traceID: specification.traceID,
                    maximumSliceWorkUnits: specification.maximumSliceWorkUnits,
                    sliceTimeoutMilliseconds: specification.sliceTimeoutMilliseconds,
                    retryPolicy: specification.retryPolicy,
                    planDigest: specification.planDigest,
                    createdAt: specification.createdAt
                )
            }
            await #expect(throws: DatabaseJobRuntimeError.corruptedPlan) {
                try await service.status(
                    JobStatusOperation.Request(job: job),
                    context: context
                )
            }
        }

        do {
            let jobContext = try await makePersistentJobServiceContext()
            let request = try jobRequest()
            let context = try operationContext(
                container: jobContext.container,
                request: request,
                idempotencyKey: "tampered-plan-operation"
            )
            let service = try await jobContext.makeService()
            let job = try await service.start(
                request,
                context: context
            ).response.job
            try await replacePersistentComponent(
                container: jobContext.container,
                component: "plans",
                jobID: job.jobID,
                as: DatabasePersistentJobPlan.self
            ) { plan in
                DatabasePersistentJobPlan(
                    jobID: plan.jobID,
                    operation: alternateOperation,
                    specificationDigest: plan.specificationDigest,
                    payload: plan.payload
                )
            }
            await #expect(throws: DatabaseJobRuntimeError.corruptedPlan) {
                try await service.status(
                    JobStatusOperation.Request(job: job),
                    context: context
                )
            }
        }

        do {
            let jobContext = try await makePersistentJobServiceContext()
            let request = try jobRequest()
            let context = try operationContext(
                container: jobContext.container,
                request: request,
                idempotencyKey: "tampered-result-operation"
            )
            let service = try await jobContext.makeService()
            let job = try await service.start(
                request,
                context: context
            ).response.job
            try await service.runScheduledWork()
            try await service.runScheduledWork()
            try await replacePersistentComponent(
                container: jobContext.container,
                component: "result-manifests",
                jobID: job.jobID,
                as: DatabasePersistentJobResultManifest.self
            ) { manifest in
                DatabasePersistentJobResultManifest(
                    jobID: manifest.jobID,
                    operation: alternateOperation,
                    specificationDigest: manifest.specificationDigest,
                    responseDigest: manifest.responseDigest,
                    totalBytes: manifest.totalBytes,
                    chunkBytes: manifest.chunkBytes,
                    chunkCount: manifest.chunkCount,
                    chunkDigests: manifest.chunkDigests,
                    createdAt: manifest.createdAt
                )
            }
            await #expect(throws: DatabaseJobRuntimeError.corruptedResult) {
                try await service.status(
                    JobStatusOperation.Request(job: job),
                    context: context
                )
            }
        }
    }

    @Test("A failing terminal hook rolls back its writes and records failure")
    func failingTerminalHookIsExplicitAndAtomic() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: FailingTerminalHookOperation()
        )
        let request = JobStartOperation.Request(
            operation: try FailingTerminalHookJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(8),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 1,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 1
            )
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "terminal-hook-failure"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job

        await #expect(throws: DatabaseJobTerminalHookExecutionError.self) {
            try await service.runScheduledWork()
        }

        let status = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(status.state == .failed)
        let result = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .failed(_, let failure) = result else {
            Issue.record("Expected terminal hook failure result")
            return
        }
        #expect(failure.code == "JOB_TERMINAL_HOOK_FAILED")
        let rolledBackMarker = try await jobContext.container.engine
            .withTransaction(configuration: .readOnly) { transaction in
                try await transaction.getValue(
                    for: FailingTerminalHookOperation.markerKey,
                    snapshot: true
                )
            }
        #expect(rolledBackMarker == nil)
    }

    @Test("Oversized plans roll back compile-time mutations")
    func oversizedPlanRollsBackCompilation() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedPlanResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedPlanJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(9),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "oversized-plan"
        )
        let service = try await jobContext.makeService()

        do {
            _ = try await service.start(request, context: context)
            Issue.record("Expected oversized plan failure")
        } catch let error as DatabaseJobRuntimeError {
            guard case .planTooLarge = error else {
                Issue.record("Unexpected job runtime error: \(error)")
                return
            }
        } catch {
            Issue.record("Unexpected oversized plan error: \(error)")
        }
        let marker = try await jobContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(
                for: OversizedPlanResumableOperation.markerKey,
                snapshot: true
            )
        }
        #expect(marker == nil)
    }

    @Test("Oversized state rolls back slice mutations and fails the job")
    func oversizedStateRollsBackSlice() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedStateResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedStateJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(10),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "oversized-state"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        try await service.runScheduledWork()

        let status = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(status.state == .failed)
        let result = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .failed(_, let failure) = result else {
            Issue.record("Expected oversized state failure")
            return
        }
        #expect(failure.category == .resourceLimit)
        #expect(failure.code == "JOB_RESOURCE_LIMIT")
        let marker = try await jobContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(
                for: OversizedStateResumableOperation.markerKey,
                snapshot: true
            )
        }
        #expect(marker == nil)
    }

    @Test("Oversized results roll back slice mutations and persist no success")
    func oversizedResultRollsBackSlice() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedResultResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedResultJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(11),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "oversized-result"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        try await service.runScheduledWork()

        let result = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .failed(_, let failure) = result else {
            Issue.record("Expected oversized result failure")
            return
        }
        #expect(failure.category == .resourceLimit)
        #expect(failure.code == "JOB_RESOURCE_LIMIT")
        let marker = try await jobContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(
                for: OversizedResultResumableOperation.markerKey,
                snapshot: true
            )
        }
        #expect(marker == nil)
    }

    @Test("Pending job cancellation is terminal")
    func cancelsPendingJob() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-cancel"
        )
        let service = try await jobContext.makeService()
        let started = try await service.start(
            request,
            context: context
        ).response

        let cancelRequest = JobCancelOperation.Request(job: started.job)
        let cancelContext = try operationContext(
            container: jobContext.container,
            operation: JobCancelOperation.self,
            request: cancelRequest,
            idempotencyKey: "job-cancel-request"
        )
        let cancelled = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(cancelled.accepted)
        #expect(cancelled.state == .cancelled)

        let replayed = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(replayed == cancelled)

        try await service.runScheduledWork()
        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .cancelled(let job) = result else {
            Issue.record("Expected a cancelled persistent job result")
            return
        }
        #expect(job == started.job)
    }

    @Test("Job start rejects unregistered resumable operation kinds")
    func rejectsNonResumableOperation() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.unregistered"
            ),
            requestPayload: [0x01],
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-recursion"
        )
        let service = try await jobContext.makeService()

        await #expect(throws: DatabaseResumableOperationRegistryError.self) {
            try await service.start(request, context: context)
        }
    }

    @Test("Expired final lease runs the operation terminal hook atomically")
    func exhaustedLeaseRunsTerminalHook() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try TwoSliceJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(1),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 2,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 10
            )
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-exhausted-lease"
        )
        let service = try await jobContext.makeService()
        let started = try await service.start(
            request,
            context: context
        ).response
        let store = try await DatabasePersistentJobStore(
            container: jobContext.container,
            wireLimits: .default
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.rawTransaction
            guard let snapshot = try await store.load(
                started.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingRecord
            }
            let firstLease = try snapshot.state.acquiringLease(
                owner: DatabaseUUID(high: 9, low: 1),
                token: DatabaseUUID(high: 19, low: 1),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            let finalLease = try firstLease.acquiringLease(
                owner: DatabaseUUID(high: 9, low: 2),
                token: DatabaseUUID(high: 19, low: 2),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            try store.storeState(
                firstLease,
                replacing: snapshot.state,
                transaction: transaction
            )
            try store.storeState(
                finalLease,
                replacing: firstLease,
                transaction: transaction
            )
        }

        try await service.runScheduledWork()

        let status = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        let terminalMarker = try await jobContext.container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.getValue(
                for: TwoSliceResumableOperation.terminalKey
            )
        }
        #expect(status.state == .failed)
        #expect(terminalMarker == [0xfa])
    }

    @Test("Expired final lease records a missing operation as a typed failure")
    func exhaustedLeaseRecordsMissingOperation() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try TwoSliceJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(1),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 1,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 10
            )
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-missing-operation"
        )
        let initialService = try await jobContext.makeService()
        let started = try await initialService.start(
            request,
            context: context
        ).response
        let store = try await DatabasePersistentJobStore(
            container: jobContext.container,
            wireLimits: .default
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.rawTransaction
            guard let snapshot = try await store.load(
                started.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingRecord
            }
            let exhausted = try snapshot.state.acquiringLease(
                owner: DatabaseUUID(high: 9, low: 3),
                token: DatabaseUUID(high: 19, low: 3),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            try store.storeState(
                exhausted,
                replacing: snapshot.state,
                transaction: transaction
            )
        }

        let missingRegistry = try DatabaseResumableOperationRegistry(
            operations: []
        )
        let factory = try DatabasePersistentJobServiceFactory(
            registry: missingRegistry,
            scheduler: jobContext.scheduler,
            clock: jobContext.clock,
            identifierGenerator: jobContext.identifiers
        )
        let recreatedService = try await factory.makeJobService(
            context: DatabaseServerServiceContext(
                container: jobContext.container,
                stateStore: jobContext.stateStore,
                coordinator: jobContext.coordinator,
                runtimeLimits: .default,
                wireLimits: .default
            )
        )

        try await recreatedService.runScheduledWork()

        let result = try await recreatedService.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .failed(let job, let failure) = result else {
            Issue.record("Expected a failed persistent job result")
            return
        }
        #expect(job == started.job)
        #expect(failure.code == "JOB_OPERATION_NOT_RESUMABLE")
        #expect(failure.category == .invalidRequest)
        #expect(failure.retryability == .never)
    }

    @Test("Scheduler failure preserves the canonical JobStart response for retry")
    func schedulerFailurePreservesStartReplay() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let scheduler = FailOnceScheduler()
        let factory = try DatabasePersistentJobServiceFactory(
            registry: jobContext.registry,
            scheduler: scheduler,
            clock: jobContext.clock,
            identifierGenerator: jobContext.identifiers
        )
        let service = try await factory.makeJobService(
            context: DatabaseServerServiceContext(
                container: jobContext.container,
                stateStore: jobContext.stateStore,
                coordinator: jobContext.coordinator,
                runtimeLimits: .default,
                wireLimits: .default
            )
        )
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "job-scheduler-retry"
        )

        do {
            _ = try await service.start(request, context: context)
            Issue.record("Expected scheduler failure")
        } catch PersistentJobScenarioError.schedulerFailure {
        } catch {
            Issue.record("Unexpected scheduler error: \(error)")
        }
        let replayed = try await service.start(
            request,
            context: context
        ).response
        let status = try await service.status(
            JobStatusOperation.Request(job: replayed.job),
            context: context
        )

        #expect(status.state == .pending)
        #expect(await scheduler.attemptCount() == 2)
    }

    @Test("Successful slices do not consume the retry attempt limit")
    func successfulSlicesDoNotConsumeRetryLimit() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: FiveSliceResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try FiveSliceJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(5),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 1,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 1
            )
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "five-successful-slices"
        )
        let service = try await jobContext.makeService()
        let started = try await service.start(
            request,
            context: context
        ).response

        for _ in 0..<5 {
            try await service.runScheduledWork()
        }

        let status = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(status.state == .succeeded)
        #expect(status.completedWorkUnits == 5)
        #expect(status.executionCount == 5)
        #expect(status.currentSliceAttempt == 1)
    }

    @Test("Only retryable failures advance the current slice attempt")
    func retryableFailureAdvancesOnlyCurrentSliceAttempt() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: RetryingResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try RetryingJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(3),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 3,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 1
            )
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "retry-one-slice"
        )
        let service = try await jobContext.makeService()
        let started = try await service.start(
            request,
            context: context
        ).response

        try await service.runScheduledWork()
        let afterSuccess = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(afterSuccess.executionCount == 1)
        #expect(afterSuccess.currentSliceAttempt == 0)

        try await service.runScheduledWork()
        let afterFailure = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(afterFailure.state == .pending)
        #expect(afterFailure.executionCount == 2)
        #expect(afterFailure.currentSliceAttempt == 1)

        try await service.runScheduledWork()
        let afterRetrySuccess = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(afterRetrySuccess.state == .pending)
        #expect(afterRetrySuccess.executionCount == 3)
        #expect(afterRetrySuccess.currentSliceAttempt == 0)

        try await service.runScheduledWork()
        let completed = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(completed.state == .succeeded)
        #expect(completed.executionCount == 4)
        #expect(completed.currentSliceAttempt == 1)
    }

    private func makePersistentJobServiceContext() async throws -> PersistentJobServiceContext {
        try await makePersistentJobServiceContext(operation: TwoSliceResumableOperation())
    }

    private func makePersistentJobServiceContext<Operation: DatabaseResumableOperation>(
        operation: Operation
    ) async throws -> PersistentJobServiceContext {
        let container = try await DBContainer(
            for: Schema(
                [DatabaseEndpointRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore
        )
        let clock = FixedDatabaseWallClock(
            initial: DatabaseTimestamp(secondsSinceUnixEpoch: 1_000)
        )
        let scheduler = RecordingDatabaseJobScheduler()
        let identifiers = SequentialDatabaseUUIDGenerator()
        let registry = try DatabaseResumableOperationRegistry(
            operations: [
                AnyDatabaseResumableOperation(operation),
            ]
        )
        return PersistentJobServiceContext(
            container: container,
            stateStore: stateStore,
            coordinator: coordinator,
            clock: clock,
            scheduler: scheduler,
            identifiers: identifiers,
            registry: registry
        )
    }

    private func jobRequest() throws -> JobStartOperation.Request {
        JobStartOperation.Request(
            operation: try TwoSliceJob.jobOperationIdentifier(),
            requestPayload: try encodedValue(1),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 3,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 10
            )
        )
    }

    private func encodedValue(_ value: UInt8) throws -> DatabaseBytes {
        try DatabaseEnvelopeCodec.encode(JobStepValue(value))
    }

    private func persistentJobKey(
        container: DBContainer,
        component: String,
        jobID: DatabaseUUID
    ) async throws -> Bytes {
        let root = try await container.engine.createOrOpenDirectory(
            path: ["database-framework", "persistent-jobs"]
        )
        return root.subspace(component).pack(Tuple(jobID))
    }

    private func persistentJobChunkKey(
        container: DBContainer,
        jobID: DatabaseUUID,
        index: UInt32
    ) async throws -> Bytes {
        let root = try await container.engine.createOrOpenDirectory(
            path: ["database-framework", "persistent-jobs"]
        )
        return root.subspace("result-chunks").pack(
            Tuple(jobID, Int64(index))
        )
    }

    private func replacePersistentComponent<Value: DatabaseWireValue>(
        container: DBContainer,
        component: String,
        jobID: DatabaseUUID,
        as type: Value.Type,
        transform: (Value) throws -> Value
    ) async throws {
        let key = try await persistentJobKey(
            container: container,
            component: component,
            jobID: jobID
        )
        try await container.engine.withTransaction { transaction in
            guard let stored = try await transaction.getValue(
                for: key,
                snapshot: false
            ) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            let decoded = try DatabaseEnvelopeCodec.decode(
                type,
                from: DatabaseBytes(retaining: stored)
            )
            let replacement = try DatabaseEnvelopeCodec.encode(
                transform(decoded)
            )
            try transaction.setValue(
                Bytes(retaining: replacement),
                for: key
            )
        }
    }

    private func operationContext(
        container: DBContainer,
        request: JobStartOperation.Request,
        idempotencyKey: String
    ) throws -> DatabaseOperationContext {
        try operationContext(
            container: container,
            operation: JobStartOperation.self,
            request: request,
            idempotencyKey: idempotencyKey
        )
    }

    private func operationContext<Operation: DatabaseOperation>(
        container: DBContainer,
        operation: Operation.Type,
        request: Operation.Request,
        idempotencyKey: String
    ) throws -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 7,
            metadata: DatabaseRequestMetadata(
                traceID: "trace",
                idempotencyKey: idempotencyKey
            ),
            requestPayload: try DatabaseEnvelopeCodec.encode(request)
        )
    }

    private struct PersistentJobServiceContext: Sendable {
        let container: DBContainer
        let stateStore: DatabaseMutationStateStore
        let coordinator: DatabaseTransactionalOperationCoordinator
        let clock: FixedDatabaseWallClock
        let scheduler: RecordingDatabaseJobScheduler
        let identifiers: SequentialDatabaseUUIDGenerator
        let registry: DatabaseResumableOperationRegistry

        func makeService() async throws -> any DatabaseJobService {
            let factory = try DatabasePersistentJobServiceFactory(
                registry: registry,
                scheduler: scheduler,
                clock: clock,
                identifierGenerator: identifiers
            )
            return try await factory.makeJobService(
                context: DatabaseServerServiceContext(
                    container: container,
                    stateStore: stateStore,
                    coordinator: coordinator,
                    runtimeLimits: .default,
                    wireLimits: .default
                )
            )
        }
    }

    private struct JobStepValue: DatabaseWireValue, Sendable, Hashable {
        let value: UInt8

        init(_ value: UInt8) {
            self.value = value
        }

        func encode(
            into writer: inout DatabaseWireWriter
        ) throws(DatabaseWireError) {
            writer.writeUInt8(value)
        }

        init(
            from reader: inout DatabaseWireReader
        ) throws(DatabaseWireError) {
            self.init(try reader.readUInt8())
        }
    }

    private struct JobPayload: DatabaseWireValue, Sendable, Hashable {
        let value: DatabaseBytes

        func encode(
            into writer: inout DatabaseWireWriter
        ) throws(DatabaseWireError) {
            try writer.writeBytes(value)
        }

        init(
            from reader: inout DatabaseWireReader
        ) throws(DatabaseWireError) {
            value = try reader.readBytes()
        }

        init(_ value: DatabaseBytes) {
            self.value = value
        }
    }

    private struct TwoSliceJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.two-slice"
            )
        }
    }

    private struct FiveSliceJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.five-slice"
            )
        }
    }

    private struct LargeResultJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobPayload

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.large-result"
            )
        }
    }

    private struct FailingTerminalHookJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.failing-terminal-hook"
            )
        }
    }

    private struct OversizedPlanJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.oversized-plan"
            )
        }
    }

    private struct OversizedStateJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.oversized-state"
            )
        }
    }

    private struct OversizedResultJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobPayload

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.oversized-result"
            )
        }
    }

    private struct RetryingJob: DatabaseJobDescriptor {
        typealias Request = JobStepValue
        typealias Response = JobStepValue

        static func jobOperationIdentifier()
            throws(DatabaseWireError) -> DatabaseJobOperationIdentifier {
            try DatabaseJobOperationIdentifier(
                family: .maintenanceExecute,
                kind: "database.test.retrying"
            )
        }
    }

    private final class CompilationCounter: Sendable {
        private let storage = Mutex(0)

        var count: Int {
            storage.withLock { $0 }
        }

        func recordCompilation() {
            storage.withLock { $0 += 1 }
        }
    }

    private struct TwoSliceResumableOperation: DatabaseResumableOperation {
        static let stateKey = Bytes("job-test-state".utf8)
        static let terminalKey = Bytes("job-test-terminal".utf8)

        typealias Job = TwoSliceJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue
        let compilationCounter: CompilationCounter

        init(compilationCounter: CompilationCounter = CompilationCounter()) {
            self.compilationCounter = compilationCounter
        }

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(1) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            compilationCounter.recordCompilation()
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(1), maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            switch state.value {
            case 0:
                try context.transaction.rawTransaction.setValue(
                    [1],
                    for: Self.stateKey
                )
                return .incomplete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 2,
                    state: JobStepValue(1)
                )
            case 1:
                try context.transaction.rawTransaction.setValue(
                    [2],
                    for: Self.stateKey
                )
                return .complete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 2,
                    result: JobStepValue(9)
                )
            default:
                throw PersistentJobScenarioError.invalidContinuation
            }
        }

        func handleTerminalState(
            plan: JobStepValue,
            state: JobStepValue,
            terminalState: DatabaseResumableOperationTerminalState,
            context: DatabaseResumableOperationContext
        ) async throws {
            _ = plan
            _ = state
            guard case .failed = terminalState else {
                return
            }
            try context.transaction.rawTransaction.setValue(
                [0xfa],
                for: Self.terminalKey
            )
        }
    }

    private struct FiveSliceResumableOperation: DatabaseResumableOperation {
        typealias Job = FiveSliceJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(5) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(5), maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            let completed = state.value
            guard completed < 5 else {
                throw PersistentJobScenarioError.invalidContinuation
            }
            let next = completed + 1
            if next == 5 {
                return .complete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 5,
                    result: JobStepValue(0x55)
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                totalWorkUnits: 5,
                state: JobStepValue(next)
            )
        }
    }

    private struct LargeResultResumableOperation: DatabaseResumableOperation {
        typealias Job = LargeResultJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue
        let payload: DatabaseBytes

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(7) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(7),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return .complete(
                completedWorkUnits: 1,
                totalWorkUnits: 1,
                result: JobPayload(payload)
            )
        }
    }

    private struct FailingTerminalHookOperation: DatabaseResumableOperation {
        static let markerKey = Bytes("job-terminal-hook-marker".utf8)

        typealias Job = FailingTerminalHookJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(8) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(8),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            throw PersistentJobScenarioError.forcedTerminalFailure
        }

        func handleTerminalState(
            plan: JobStepValue,
            state: JobStepValue,
            terminalState: DatabaseResumableOperationTerminalState,
            context: DatabaseResumableOperationContext
        ) async throws {
            _ = plan
            _ = state
            guard case .failed = terminalState else {
                return
            }
            try context.transaction.rawTransaction.setValue(
                [0xde],
                for: Self.markerKey
            )
            throw PersistentJobScenarioError.terminalHookFailure
        }
    }

    private struct OversizedPlanResumableOperation: DatabaseResumableOperation {
        static let markerKey = Bytes("job-oversized-plan-marker".utf8)

        typealias Job = OversizedPlanJob
        typealias Plan = JobPayload
        typealias State = JobStepValue

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(9) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try context.transaction.rawTransaction.setValue(
                [0x09],
                for: Self.markerKey
            )
            return DatabasePreparedResumableJob(
                plan: JobPayload(
                    DatabaseBytes(
                        [UInt8](
                            repeating: 0x09,
                            count: 256 * 1_024
                        )
                    )
                ),
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobPayload,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            _ = plan
            _ = state
            _ = maximumWorkUnits
            _ = context
            throw PersistentJobScenarioError.unexpectedExecution
        }
    }

    private struct OversizedStateResumableOperation: DatabaseResumableOperation {
        static let markerKey = Bytes("job-oversized-state-marker".utf8)

        typealias Job = OversizedStateJob
        typealias Plan = JobStepValue
        typealias State = JobPayload

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(10) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobPayload([]),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobPayload,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(10),
                  state.value.isEmpty,
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try context.transaction.rawTransaction.setValue(
                [0x0a],
                for: Self.markerKey
            )
            return .incomplete(
                completedWorkUnits: 1,
                totalWorkUnits: 2,
                state: JobPayload(
                    DatabaseBytes(
                        [UInt8](
                            repeating: 0x0a,
                            count: 64 * 1_024
                        )
                    )
                )
            )
        }
    }

    private struct OversizedResultResumableOperation: DatabaseResumableOperation {
        static let markerKey = Bytes("job-oversized-result-marker".utf8)

        typealias Job = OversizedResultJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(11) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(11),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try context.transaction.rawTransaction.setValue(
                [0x0b],
                for: Self.markerKey
            )
            return .complete(
                completedWorkUnits: 1,
                totalWorkUnits: 1,
                result: JobPayload(
                    DatabaseBytes(
                        [UInt8](
                            repeating: 0x0b,
                            count: 4 * 1_024 * 1_024
                        )
                    )
                )
            )
        }
    }

    private final class RetryingResumableOperation:
        DatabaseResumableOperation,
        Sendable {
        typealias Job = RetryingJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        private let shouldFailSecondSlice = Mutex(true)

        func compile(
            _ request: JobStepValue,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            guard request == JobStepValue(3) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Job.Response> {
            guard plan == JobStepValue(3), maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            switch state.value {
            case 0:
                return .incomplete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 3,
                    state: JobStepValue(1)
                )
            case 1:
                let shouldFail = shouldFailSecondSlice.withLock { value in
                    guard value else { return false }
                    value = false
                    return true
                }
                if shouldFail {
                    throw DatabaseRemoteError(
                        category: .resourceLimit,
                        code: "INJECTED_RETRYABLE_FAILURE",
                        message: "Injected retryable slice failure",
                        retryability: .immediate
                    )
                }
                return .incomplete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 3,
                    state: JobStepValue(2)
                )
            case 2:
                return .complete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 3,
                    result: JobStepValue(0x33)
                )
            default:
                throw PersistentJobScenarioError.invalidContinuation
            }
        }
    }

    private final class FixedDatabaseWallClock: DatabaseWallClock, Sendable {
        private let timestamp: Mutex<DatabaseTimestamp>

        init(initial: DatabaseTimestamp) {
            self.timestamp = Mutex(initial)
        }

        func now() -> DatabaseTimestamp {
            timestamp.withLock { $0 }
        }
    }

    private actor RecordingDatabaseJobScheduler: DatabaseJobScheduler {
        private var timestamps: [DatabaseTimestamp] = []

        func schedule(at timestamp: DatabaseTimestamp) async throws {
            timestamps.append(timestamp)
        }

        func scheduledCount() -> Int {
            timestamps.count
        }
    }

    private actor FailOnceScheduler: DatabaseJobScheduler {
        private var attempts = 0

        func schedule(at timestamp: DatabaseTimestamp) async throws {
            _ = timestamp
            attempts += 1
            if attempts == 1 {
                throw PersistentJobScenarioError.schedulerFailure
            }
        }

        func attemptCount() -> Int {
            attempts
        }
    }

    private final class SequentialDatabaseUUIDGenerator: DatabaseUUIDGenerator, Sendable {
        private let nextValue = Mutex<UInt64>(1)

        func generate() -> DatabaseUUID {
            nextValue.withLock { value in
                defer { value += 1 }
                return DatabaseUUID(high: 0, low: value)
            }
        }
    }

    private enum PersistentJobScenarioError: Error {
        case invalidPayload
        case invalidContinuation
        case forcedTerminalFailure
        case terminalHookFailure
        case unexpectedExecution
        case missingRecord
        case schedulerFailure
    }
}
