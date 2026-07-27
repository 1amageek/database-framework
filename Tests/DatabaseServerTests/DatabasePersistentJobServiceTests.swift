import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit
import Synchronization
import Testing

private let persistentJobTestStorageLimits =
    DatabasePersistentJobStorageLimits(maximumStorageValueBytes: 1_048_576)

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
        #expect(responseDigest.bytes.count == JobResultDigest.byteCount)
        #expect(continuation == nil)
        #expect(
            try completedStep(
                in: TwoSliceJob.operation().decodeCompletedResponse(payloadPage)
            ) == 9
        )
        #expect(compilationCounter.count == 1)

        let storedStep = try await storedMarker(
            identifiedBy: TwoSliceResumableOperation.stateMarkerID,
            in: jobContext.container
        )
        #expect(storedStep == 2)
        #expect(await jobContext.scheduler.scheduledCount() >= 2)
    }

    @Test("Large job results are paged without exceeding the chunk limit")
    func largeResultsArePagedAndDigestVerified() async throws {
        let payloadByteCount = 1_200_000
        let expectedPayload = ByteString(
            [UInt8](repeating: 0xab, count: payloadByteCount)
        )
        let expectedEncodedResponse = try LargeResultJob.operation()
            .encodeCompletedResponse(
                Self.commandResponse(bytes: expectedPayload)
            )
        let expectedResponseByteCount = expectedEncodedResponse.count
        let jobContext = try await makePersistentJobServiceContext(
            operation: LargeResultResumableOperation(
                payload: expectedPayload
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.operation().identifier,
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
        var expectedDigest: JobResultDigest?
        var digest = JobResultDigestAccumulator(
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
            #expect(
                totalResponseBytes == UInt64(expectedResponseByteCount)
            )
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
        #expect(receivedByteCount == expectedResponseByteCount)
        let finalExpectedDigest = try #require(expectedDigest)
        #expect(digest.finalize() == finalExpectedDigest)
        var independentlyComputedDigest = JobResultDigestAccumulator(
            operation: job.operation
        )
        independentlyComputedDigest.update(expectedEncodedResponse)
        #expect(
            independentlyComputedDigest.finalize() == finalExpectedDigest
        )
    }

    @Test("Result continuations are bound to one job and one digest")
    func resultContinuationBindingIsStrict() async throws {
        let expectedPayload = ByteString(
            [UInt8](repeating: 0x7a, count: 600_000)
        )
        let jobContext = try await makePersistentJobServiceContext(
            operation: LargeResultResumableOperation(
                payload: expectedPayload
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.operation().identifier,
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
            job: JobIdentity(
                jobID: DatabaseTypes.UUID(high: jobID.high ^ 1, low: jobID.low),
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

        let otherKindJob = JobIdentity(
            jobID: jobID,
            operation: try JobOperationIdentifier(
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

        let otherDigest = try JobResultDigest(
            [UInt8](repeating: 0, count: JobResultDigest.byteCount)
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
        let mismatchedJob = JobIdentity(
            jobID: persistedJob.jobID,
            operation: try JobOperationIdentifier(
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
            operation: DatabaseOperations.jobCancel,
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
                payload: ByteString(
                    [UInt8](repeating: 0x6b, count: 600_000)
                )
            )
        )
        let request = JobStartOperation.Request(
            operation: try LargeResultJob.operation().identifier,
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

    @Test("Persistent job storage limits honor the backend capability")
    func storageLimitsHonorBackendCapability() throws {
        try DatabasePersistentJobStorageLimits(
            maximumStorageValueBytes: 2 * 1_048_576
        ).validate(wireLimits: .default)
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 0
            ).validate()
        }
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_576,
                resultChunkBytes: 512 * 1_024 + 1
            ).validate()
        }
        #expect(throws: DatabaseJobRuntimeError.self) {
            try DatabasePersistentJobStorageLimits(
                maximumStorageValueBytes: 1_048_576,
                maximumResultBytes: 4 * 1_024 * 1_024,
                resultChunkBytes: 1
            ).validate(wireLimits: .default)
        }
        try persistentJobTestStorageLimits.validate(
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
        let alternateOperation = try JobOperationIdentifier(
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

    @Test("Unsuccessful outcome commits retry atomically without rerunning work")
    func unsuccessfulOutcomeCommitRetriesAtomically() async throws {
        let commitProbe = UnsuccessfulOutcomeCommitProbe(failureCount: 1)
        let jobContext = try await makePersistentJobServiceContext(
            operation: RetryingUnsuccessfulOutcomeOperation(
                commitProbe: commitProbe
            )
        )
        let request = JobStartOperation.Request(
            operation: try RetryingUnsuccessfulOutcomeJob.operation().identifier,
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
            idempotencyKey: "unsuccessful-outcome-retry"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job

        try await service.runScheduledWork()

        let prepared = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.executionCount == 1)
        #expect(prepared.unsuccessfulOutcomeCommitAttempt == 0)
        #expect(prepared.lastUnsuccessfulOutcomeCommitError == nil)
        await #expect(
            throws: DatabaseJobRuntimeError.resultNotReady(job.jobID)
        ) {
            try await service.result(
                JobResultOperation.Request(job: job),
                context: context
            )
        }

        await #expect(throws: DatabaseJobUnsuccessfulOutcomeCommitError.self) {
            try await service.runScheduledWork()
        }

        let awaitingRetry = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(awaitingRetry.state == .committingUnsuccessfulOutcome)
        #expect(awaitingRetry.executionCount == 1)
        #expect(awaitingRetry.unsuccessfulOutcomeCommitAttempt == 1)
        #expect(
            awaitingRetry.lastUnsuccessfulOutcomeCommitError?.code
                == "JOB_UNSUCCESSFUL_OUTCOME_COMMIT_FAILED"
        )
        let rolledBackMarker = try await storedMarker(
            identifiedBy: RetryingUnsuccessfulOutcomeOperation.markerID,
            in: jobContext.container
        )
        #expect(rolledBackMarker == nil)

        let scheduledRetryAt = try Timestamp(
            secondsSinceUnixEpoch: 1_000,
            nanoseconds: 100_000_000
        )
        #expect(
            await jobContext.scheduler.requestedTimestamps()
                .contains(scheduledRetryAt)
        )
        try await service.runScheduledWork()
        #expect(commitProbe.attemptCount == 1)

        try jobContext.clock.advance(milliseconds: 100)
        let recreatedService = try await jobContext.makeService()
        try await recreatedService.runScheduledWork()

        let completed = try await recreatedService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(completed.state == .failed)
        #expect(completed.executionCount == 1)
        #expect(completed.unsuccessfulOutcomeCommitAttempt == 2)
        #expect(completed.lastUnsuccessfulOutcomeCommitError == nil)
        let result = try await recreatedService.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .failed(_, let failure) = result else {
            Issue.record("Expected the original operation failure")
            return
        }
        #expect(failure.code == "SERVER_FAILURE")
        let committedMarker = try await storedMarker(
            identifiedBy: RetryingUnsuccessfulOutcomeOperation.markerID,
            in: jobContext.container
        )
        #expect(committedMarker == 0xde)
        #expect(commitProbe.attemptCount == 2)
    }

    @Test("Cancelled outcome commits retry atomically after restart")
    func cancelledOutcomeCommitRetriesAtomically() async throws {
        let commitProbe = UnsuccessfulOutcomeCommitProbe(failureCount: 1)
        let jobContext = try await makePersistentJobServiceContext(
            operation: RetryingUnsuccessfulOutcomeOperation(
                commitProbe: commitProbe
            )
        )
        let request = JobStartOperation.Request(
            operation: try RetryingUnsuccessfulOutcomeJob
                .operation().identifier,
            requestPayload: try encodedValue(8),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "cancelled-outcome-retry"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let cancelRequest = JobCancelOperation.Request(job: job)
        let cancelContext = try operationContext(
            container: jobContext.container,
            operation: DatabaseOperations.jobCancel,
            request: cancelRequest,
            idempotencyKey: "cancelled-outcome-retry-request"
        )

        let accepted = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(accepted.accepted)
        #expect(accepted.state == .committingUnsuccessfulOutcome)

        await #expect(throws: DatabaseJobUnsuccessfulOutcomeCommitError.self) {
            try await service.runScheduledWork()
        }
        let awaitingRetry = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(awaitingRetry.state == .committingUnsuccessfulOutcome)
        #expect(awaitingRetry.executionCount == 0)
        #expect(awaitingRetry.unsuccessfulOutcomeCommitAttempt == 1)
        let rolledBackMarker = try await storedMarker(
            identifiedBy: RetryingUnsuccessfulOutcomeOperation.markerID,
            in: jobContext.container
        )
        #expect(rolledBackMarker == nil)

        try jobContext.clock.advance(milliseconds: 100)
        let recreatedService = try await jobContext.makeService()
        try await recreatedService.runScheduledWork()

        let completed = try await recreatedService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(completed.state == .cancelled)
        #expect(completed.executionCount == 0)
        #expect(completed.unsuccessfulOutcomeCommitAttempt == 2)
        let result = try await recreatedService.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .cancelled = result else {
            Issue.record("Expected a cancelled persistent job result")
            return
        }
        let committedMarker = try await storedMarker(
            identifiedBy: RetryingUnsuccessfulOutcomeOperation.markerID,
            in: jobContext.container
        )
        #expect(committedMarker == 0xca)
        #expect(commitProbe.attemptCount == 2)
    }

    @Test("Expired outcome lease resumes without rerunning operation work")
    func expiredUnsuccessfulOutcomeLeaseResumesCommit() async throws {
        let commitProbe = UnsuccessfulOutcomeCommitProbe(failureCount: 0)
        let jobContext = try await makePersistentJobServiceContext(
            operation: RetryingUnsuccessfulOutcomeOperation(
                commitProbe: commitProbe
            )
        )
        let request = JobStartOperation.Request(
            operation: try RetryingUnsuccessfulOutcomeJob.operation().identifier,
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
            idempotencyKey: "expired-unsuccessful-outcome-lease"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        try await service.runScheduledWork()

        let store = try await DatabasePersistentJobStore(
            container: jobContext.container,
            wireLimits: .default,
            storageLimits: persistentJobTestStorageLimits
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                job.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingEntity
            }
            let abandonedLease = try snapshot.state.acquiringLease(
                owner: DatabaseTypes.UUID(high: 29, low: 1),
                token: DatabaseTypes.UUID(high: 39, low: 1),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            try store.storeState(
                abandonedLease,
                replacing: snapshot.state,
                transaction: transaction
            )
        }

        let recreatedService = try await jobContext.makeService()
        try await recreatedService.runScheduledWork()

        let status = try await recreatedService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(status.state == .failed)
        #expect(status.executionCount == 1)
        #expect(status.unsuccessfulOutcomeCommitAttempt == 2)
        #expect(commitProbe.attemptCount == 1)
        let committedMarker = try await storedMarker(
            identifiedBy: RetryingUnsuccessfulOutcomeOperation.markerID,
            in: jobContext.container
        )
        #expect(committedMarker == 0xde)
    }

    @Test("Expired running lease resumes from persisted operation state")
    func expiredRunningLeaseResumesOperation() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "expired-running-lease"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let store = try await DatabasePersistentJobStore(
            container: jobContext.container,
            wireLimits: .default,
            storageLimits: persistentJobTestStorageLimits
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                job.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingEntity
            }
            let abandonedLease = try snapshot.state.acquiringLease(
                owner: DatabaseTypes.UUID(high: 41, low: 1),
                token: DatabaseTypes.UUID(high: 42, low: 1),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            try store.storeState(
                abandonedLease,
                replacing: snapshot.state,
                transaction: transaction
            )
        }

        let recreatedService = try await jobContext.makeService()
        try await recreatedService.runScheduledWork()
        let resumed = try await recreatedService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(resumed.state == .pending)
        #expect(resumed.completedWorkUnits == 1)
        #expect(resumed.executionCount == 2)
        let committedSlice = try await storedMarker(
            identifiedBy: TwoSliceResumableOperation.stateMarkerID,
            in: jobContext.container
        )
        #expect(committedSlice == 1)
    }

    @Test("Concurrent runners commit each due revision once")
    func concurrentRunnersCommitEachRevisionOnce() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "concurrent-runners"
        )
        let firstService = try await jobContext.makeService()
        let secondService = try await jobContext.makeService()
        let job = try await firstService.start(
            request,
            context: context
        ).response.job

        async let firstRunner: Void = firstService.runScheduledWork()
        async let secondRunner: Void = secondService.runScheduledWork()
        _ = try await (firstRunner, secondRunner)

        let afterFirstRevision = try await firstService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(afterFirstRevision.state == .pending)
        #expect(afterFirstRevision.completedWorkUnits == 1)
        #expect(afterFirstRevision.executionCount == 1)
        let firstMarker = try await storedMarker(
            identifiedBy: TwoSliceResumableOperation.stateMarkerID,
            in: jobContext.container
        )
        #expect(firstMarker == 1)

        async let firstCompletion: Void = firstService.runScheduledWork()
        async let secondCompletion: Void = secondService.runScheduledWork()
        _ = try await (firstCompletion, secondCompletion)

        let completed = try await firstService.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(completed.state == .succeeded)
        #expect(completed.completedWorkUnits == 2)
        #expect(completed.executionCount == 2)
        let finalMarker = try await storedMarker(
            identifiedBy: TwoSliceResumableOperation.stateMarkerID,
            in: jobContext.container
        )
        #expect(finalMarker == 2)
    }

    @Test("Oversized plans roll back compile-time mutations")
    func oversizedPlanRollsBackCompilation() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedPlanResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedPlanJob.operation().identifier,
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
        let marker = try await storedMarker(
            identifiedBy: OversizedPlanResumableOperation.markerID,
            in: jobContext.container
        )
        #expect(marker == nil)
    }

    @Test("Oversized state rolls back slice mutations and fails the job")
    func oversizedStateRollsBackSlice() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedStateResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedStateJob.operation().identifier,
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
        let marker = try await storedMarker(
            identifiedBy: OversizedStateResumableOperation.markerID,
            in: jobContext.container
        )
        #expect(marker == nil)
    }

    @Test("Oversized results roll back slice mutations and persist no success")
    func oversizedResultRollsBackSlice() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: OversizedResultResumableOperation()
        )
        let request = JobStartOperation.Request(
            operation: try OversizedResultJob.operation().identifier,
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
        let marker = try await storedMarker(
            identifiedBy: OversizedResultResumableOperation.markerID,
            in: jobContext.container
        )
        #expect(marker == nil)
    }

    @Test("Oversized failures persist a bounded explicit unsuccessful outcome")
    func oversizedFailureDoesNotRerunOperationWork() async throws {
        let jobContext = try await makePersistentJobServiceContext(
            operation: AlwaysFailingResumableOperation(
                failure: RemoteOperationError(
                    category: .constraint,
                    code: String(repeating: "C", count: 200 * 1_024),
                    message: String(repeating: "x", count: 200 * 1_024),
                    retryability: .never
                )
            )
        )
        let request = JobStartOperation.Request(
            operation: try AlwaysFailingJob.operation().identifier,
            requestPayload: try encodedValue(12),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "oversized-failure"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job

        try await service.runScheduledWork()
        let prepared = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.executionCount == 1)

        try await service.runScheduledWork()
        let completed = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(completed.state == .failed)
        #expect(completed.executionCount == 1)
        let result = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .failed(_, let failure) = result else {
            Issue.record("Expected a bounded persistent failure")
            return
        }
        #expect(failure.category == .constraint)
        #expect(failure.code == "JOB_FAILURE_STORAGE_BUDGET_EXCEEDED")
        #expect(
            failure.details["originalCode"]
                == .string(String(repeating: "C", count: 1_024))
        )
    }

    @Test("Pending job cancellation commits its unsuccessful outcome")
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
            operation: DatabaseOperations.jobCancel,
            request: cancelRequest,
            idempotencyKey: "job-cancel-request"
        )
        let cancelled = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(cancelled.accepted)
        #expect(cancelled.state == .committingUnsuccessfulOutcome)

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

    @Test("In-flight cancellation is observed once without lease expiry")
    func inFlightCancellationTransitionsDirectlyToOutcomeCommit() async throws {
        let executionGate = OperationExecutionGate()
        let jobContext = try await makePersistentJobServiceContext(
            operation: TwoSliceResumableOperation(
                executionGate: executionGate
            )
        )
        let request = try jobRequest()
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "in-flight-cancel"
        )
        let service = try await jobContext.makeService()
        let started = try await service.start(
            request,
            context: context
        ).response
        let execution = Task {
            try await service.runScheduledWork()
        }
        await executionGate.waitUntilEntered()

        let cancelRequest = JobCancelOperation.Request(job: started.job)
        let firstCancelContext = try operationContext(
            container: jobContext.container,
            operation: DatabaseOperations.jobCancel,
            request: cancelRequest,
            idempotencyKey: "in-flight-cancel-first"
        )
        let firstCancel = try await service.cancel(
            cancelRequest,
            context: firstCancelContext
        ).response
        #expect(firstCancel.accepted)
        #expect(firstCancel.state == .running)

        let secondCancelContext = try operationContext(
            container: jobContext.container,
            operation: DatabaseOperations.jobCancel,
            request: cancelRequest,
            idempotencyKey: "in-flight-cancel-second"
        )
        let secondCancel = try await service.cancel(
            cancelRequest,
            context: secondCancelContext
        ).response
        #expect(!secondCancel.accepted)
        #expect(secondCancel.state == .running)

        await executionGate.release()
        try await execution.value

        let prepared = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.executionCount == 1)
        try await service.runScheduledWork()

        let result = try await service.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .cancelled(let job) = result else {
            Issue.record("Expected in-flight cancellation to become cancelled")
            return
        }
        #expect(job == started.job)
        let rolledBackSliceMarker = try await storedMarker(
            identifiedBy: TwoSliceResumableOperation.stateMarkerID,
            in: jobContext.container
        )
        #expect(rolledBackSliceMarker == nil)
        let unsuccessfulOutcomeMarker = try await storedMarker(
            identifiedBy:
                TwoSliceResumableOperation.unsuccessfulOutcomeMarkerID,
            in: jobContext.container
        )
        #expect(unsuccessfulOutcomeMarker == 0xca)
    }

    @Test("Checkpointed cancellation preserves committed slice progress")
    func checkpointedCancellationPreservesCommittedProgress() async throws {
        let executionGate = OperationExecutionGate()
        let executionCounter = SliceExecutionCounter()
        let jobContext = try await makePersistentJobServiceContext(
            operation: CheckpointedCancellationOperation(
                executionGate: executionGate,
                executionCounter: executionCounter
            )
        )
        let request = JobStartOperation.Request(
            operation: try CheckpointedCancellationJob
                .operation().identifier,
            requestPayload: try encodedValue(13),
            maximumSliceWorkUnits: 1
        )
        let context = try operationContext(
            container: jobContext.container,
            request: request,
            idempotencyKey: "checkpointed-cancel"
        )
        let service = try await jobContext.makeService()
        let job = try await service.start(
            request,
            context: context
        ).response.job
        let execution = Task {
            try await service.runScheduledWork()
        }
        await executionGate.waitUntilEntered()

        let cancelRequest = JobCancelOperation.Request(job: job)
        let cancelContext = try operationContext(
            container: jobContext.container,
            operation: DatabaseOperations.jobCancel,
            request: cancelRequest,
            idempotencyKey: "checkpointed-cancel-request"
        )
        let cancellation = try await service.cancel(
            cancelRequest,
            context: cancelContext
        ).response
        #expect(cancellation.accepted)
        #expect(cancellation.state == .running)

        await executionGate.release()
        try await execution.value

        let prepared = try await service.status(
            JobStatusOperation.Request(job: job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.completedWorkUnits == 1)
        #expect(prepared.executionCount == 1)
        #expect(executionCounter.count == 1)
        let checkpoint = try await jobContext.container.engine
            .withTransaction(configuration: .readOnly) { transaction in
                try await transaction.getValue(
                    for: CheckpointedCancellationOperation.checkpointKey,
                    snapshot: true
                )
            }
        #expect(checkpoint == [1])

        try await service.runScheduledWork()
        let result = try await service.result(
            JobResultOperation.Request(job: job),
            context: context
        )
        guard case .cancelled = result else {
            Issue.record("Expected checkpointed job cancellation")
            return
        }
        let committedCancellationState = try await storedMarker(
            identifiedBy:
                CheckpointedCancellationOperation
                    .unsuccessfulOutcomeStateMarkerID,
            in: jobContext.container
        )
        #expect(committedCancellationState == 1)
        #expect(executionCounter.count == 1)
    }

    @Test("Job start rejects unregistered resumable operation kinds")
    func rejectsNonResumableOperation() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try JobOperationIdentifier(
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

    @Test("Expired final lease commits the operation outcome atomically")
    func exhaustedLeaseCommitsUnsuccessfulOutcome() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try TwoSliceJob.operation().identifier,
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
            wireLimits: .default,
            storageLimits: persistentJobTestStorageLimits
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                started.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingEntity
            }
            let firstLease = try snapshot.state.acquiringLease(
                owner: DatabaseTypes.UUID(high: 9, low: 1),
                token: DatabaseTypes.UUID(high: 19, low: 1),
                expiresAt: expiredAt,
                updatedAt: expiredAt
            )
            let finalLease = try firstLease.acquiringLease(
                owner: DatabaseTypes.UUID(high: 9, low: 2),
                token: DatabaseTypes.UUID(high: 19, low: 2),
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

        let prepared = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.executionCount == 2)
        #expect(prepared.unsuccessfulOutcomeCommitAttempt == 0)
        let absentMarker = try await storedMarker(
            identifiedBy:
                TwoSliceResumableOperation.unsuccessfulOutcomeMarkerID,
            in: jobContext.container
        )
        #expect(absentMarker == nil)

        try await service.runScheduledWork()

        let status = try await service.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        let unsuccessfulOutcomeMarker = try await storedMarker(
            identifiedBy:
                TwoSliceResumableOperation.unsuccessfulOutcomeMarkerID,
            in: jobContext.container
        )
        #expect(status.state == .failed)
        #expect(status.executionCount == 2)
        #expect(status.unsuccessfulOutcomeCommitAttempt == 1)
        #expect(unsuccessfulOutcomeMarker == 0xfa)
    }

    @Test("Missing operation registration keeps outcome commit recoverable")
    func missingOperationRegistrationPreservesUnsuccessfulOutcome() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let request = JobStartOperation.Request(
            operation: try TwoSliceJob.operation().identifier,
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
            wireLimits: .default,
            storageLimits: persistentJobTestStorageLimits
        )
        let expiredAt = jobContext.clock.now()
        try await jobContext.container.newContext().withTransaction(
            configuration: .batch
        ) { transactionContext in
            let transaction = transactionContext.storageAccess
            guard let snapshot = try await store.load(
                started.jobID,
                transaction: transaction
            ) else {
                throw PersistentJobScenarioError.missingEntity
            }
            let exhausted = try snapshot.state.acquiringLease(
                owner: DatabaseTypes.UUID(high: 9, low: 3),
                token: DatabaseTypes.UUID(high: 19, low: 3),
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
            identifierGenerator: jobContext.identifiers,
            storageLimits: persistentJobTestStorageLimits
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

        let prepared = try await recreatedService.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(prepared.state == .committingUnsuccessfulOutcome)
        #expect(prepared.unsuccessfulOutcomeCommitAttempt == 0)

        await #expect(throws: DatabaseJobUnsuccessfulOutcomeCommitError.self) {
            try await recreatedService.runScheduledWork()
        }

        let awaitingRegistration = try await recreatedService.status(
            JobStatusOperation.Request(job: started.job),
            context: context
        )
        #expect(awaitingRegistration.state == .committingUnsuccessfulOutcome)
        #expect(awaitingRegistration.unsuccessfulOutcomeCommitAttempt == 1)
        #expect(
            awaitingRegistration.lastUnsuccessfulOutcomeCommitError?.code
                == "JOB_UNSUCCESSFUL_OUTCOME_COMMIT_FAILED"
        )
        await #expect(
            throws: DatabaseJobRuntimeError.resultNotReady(started.jobID)
        ) {
            try await recreatedService.result(
                JobResultOperation.Request(job: started.job),
                context: context
            )
        }

        try jobContext.clock.advance(milliseconds: 100)
        let restoredService = try await jobContext.makeService()
        try await restoredService.runScheduledWork()

        let result = try await restoredService.result(
            JobResultOperation.Request(job: started.job),
            context: context
        )
        guard case .failed(let job, let failure) = result else {
            Issue.record("Expected a failed persistent job result")
            return
        }
        #expect(job == started.job)
        #expect(failure.code == "JOB_ATTEMPTS_EXHAUSTED")
        #expect(failure.category == .internalFailure)
        #expect(failure.retryability == .never)
        let unsuccessfulOutcomeMarker = try await storedMarker(
            identifiedBy:
                TwoSliceResumableOperation.unsuccessfulOutcomeMarkerID,
            in: jobContext.container
        )
        #expect(unsuccessfulOutcomeMarker == 0xfa)
    }

    @Test("Scheduler failure preserves the canonical JobStart response for retry")
    func schedulerFailurePreservesStartReplay() async throws {
        let jobContext = try await makePersistentJobServiceContext()
        let scheduler = FailOnceScheduler()
        let factory = try DatabasePersistentJobServiceFactory(
            registry: jobContext.registry,
            scheduler: scheduler,
            clock: jobContext.clock,
            identifierGenerator: jobContext.identifiers,
            storageLimits: persistentJobTestStorageLimits
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
            operation: try FiveSliceJob.operation().identifier,
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
            operation: try RetryingJob.operation().identifier,
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
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
        let stateStore = try await DatabaseMutationStateStore(
            container: container
        )
        let coordinator = DatabaseTransactionalOperationCoordinator(
            stateStore: stateStore
        )
        let clock = FixedDatabaseWallClock(
            initial: try Timestamp(secondsSinceUnixEpoch: 1_000)
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
            operation: try TwoSliceJob.operation().identifier,
            requestPayload: try encodedValue(1),
            maximumSliceWorkUnits: 1,
            retryPolicy: .init(
                maximumAttempts: 3,
                initialBackoffMilliseconds: 1,
                maximumBackoffMilliseconds: 10
            )
        )
    }

    private func encodedValue(_ value: UInt8) throws -> ByteString {
        try DatabaseWireEncoder().encodeRequestPayload(
            DatabaseOperations.commandExecute,
            request: CommandRequest(
                command: CommandDeclaration(
                    identifier: try CommandIdentifier(
                        "database.test.persistent.job"
                    ),
                    access: .readOnly
                ),
                input: try FieldObject([
                    (key: "step", value: .uint8(value)),
                ])
            )
        )
    }

    private func storedMarker(
        identifiedBy id: String,
        in container: DBContainer
    ) async throws -> UInt8? {
        try await container.newContext().withTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let marker = try await transaction.fetch(
                DatabaseEndpointEntity.self,
                identifiedBy: id
            ) else {
                return nil
            }
            guard marker.title == PersistentJobMarker.title,
                  let value = UInt8(exactly: marker.priority) else {
                throw PersistentJobMarkerError.invalidStoredValue
            }
            return value
        }
    }

    private func persistentJobKey(
        container: DBContainer,
        component: String,
        jobID: DatabaseTypes.UUID
    ) async throws -> ByteString {
        let root = try await container.engine.resolveOrCreateNamespace(
            path: ["database-framework", "persistent-jobs"]
        )
        return root.subspace(component).pack(Tuple(jobID))
    }

    private func persistentJobChunkKey(
        container: DBContainer,
        jobID: DatabaseTypes.UUID,
        index: UInt32
    ) async throws -> ByteString {
        let root = try await container.engine.resolveOrCreateNamespace(
            path: ["database-framework", "persistent-jobs"]
        )
        return root.subspace("result-chunks").pack(
            Tuple(jobID, Int64(index))
        )
    }

    private func replacePersistentComponent<Value: ServerPayloadValue>(
        container: DBContainer,
        component: String,
        jobID: DatabaseTypes.UUID,
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
            let decoded = try ServerPayloadDecoder.decode(
                type,
                from: stored,
                limits: .default
            )
            let replacement = try ServerPayloadEncoder.encode(
                transform(decoded),
                limits: .default
            )
            try transaction.setValue(
                replacement,
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
            operation: DatabaseOperations.jobStart,
            request: request,
            idempotencyKey: idempotencyKey
        )
    }

    private func operationContext<Request, Response>(
        container: DBContainer,
        operation: DatabaseOperation<Request, Response>,
        request: Request,
        idempotencyKey: String
    ) throws -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 7,
            metadata: OperationRequestMetadata(
                traceID: "trace",
                idempotencyKey: idempotencyKey
            ),
            requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                operation,
                request: request
            )
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
                identifierGenerator: identifiers,
                storageLimits: persistentJobTestStorageLimits
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

    private struct JobStepValue: PersistentJobPayload, Hashable {
        let value: UInt8

        init(_ value: UInt8) {
            self.value = value
        }

        func persistentJobValue()
            throws(PersistentJobPayloadError) -> FieldValue {
            .uint8(value)
        }

        init(
            persistentJobValue: FieldValue
        ) throws(PersistentJobPayloadError) {
            guard case .uint8(let value) = persistentJobValue else {
                throw .invalidValue("Expected UInt8 job state")
            }
            self.init(value)
        }
    }

    private struct JobPayload: PersistentJobPayload, Hashable {
        let value: ByteString

        func persistentJobValue()
            throws(PersistentJobPayloadError) -> FieldValue {
            .bytes(value)
        }

        init(
            persistentJobValue: FieldValue
        ) throws(PersistentJobPayloadError) {
            guard case .bytes(let value) = persistentJobValue else {
                throw .invalidValue("Expected bytes job payload")
            }
            self.value = value
        }

        init(_ value: ByteString) {
            self.value = value
        }
    }

    private enum PersistentJobMarker {
        static let title = "persistent-job-marker"

        static func save(
            _ value: UInt8,
            identifiedBy id: String,
            using transaction: any DatabaseTransactionWriting
        ) async throws {
            var marker = DatabaseEndpointEntity()
            marker.id = id
            marker.title = title
            marker.priority = Int64(value)
            try await transaction.save(marker, precondition: .none)
        }
    }

    private enum PersistentJobMarkerError: Error {
        case invalidStoredValue
    }

    private protocol PersistentJobTestDescriptor {
        static var kind: String { get }
        static func operation()
            throws(DatabaseWireError)
            -> JobOperation<
                CommandExecuteOperation.Request,
                CommandExecuteOperation.Response
            >
    }

    private enum PersistentJobTestOperations {
        static func operation<Descriptor>(
            for descriptor: Descriptor.Type
        )
            throws(DatabaseWireError)
            -> JobOperation<
                CommandExecuteOperation.Request,
                CommandExecuteOperation.Response
            > where Descriptor: PersistentJobTestDescriptor {
            try DatabaseOperations.commandExecute.resumableJob(
                kind: descriptor.kind
            )
        }
    }

    private enum TwoSliceJob: PersistentJobTestDescriptor {
        static let kind = "database.test.two-slice"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum FiveSliceJob: PersistentJobTestDescriptor {
        static let kind = "database.test.five-slice"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum LargeResultJob: PersistentJobTestDescriptor {
        static let kind = "database.test.large-result"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum RetryingUnsuccessfulOutcomeJob:
        PersistentJobTestDescriptor {
        static let kind = "database.test.retrying-unsuccessful-outcome"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum OversizedPlanJob: PersistentJobTestDescriptor {
        static let kind = "database.test.oversized-plan"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum OversizedStateJob: PersistentJobTestDescriptor {
        static let kind = "database.test.oversized-state"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum OversizedResultJob: PersistentJobTestDescriptor {
        static let kind = "database.test.oversized-result"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum AlwaysFailingJob: PersistentJobTestDescriptor {
        static let kind = "database.test.always-failing"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum CheckpointedCancellationJob:
        PersistentJobTestDescriptor {
        static let kind = "database.test.checkpointed-cancellation"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private enum RetryingJob: PersistentJobTestDescriptor {
        static let kind = "database.test.retrying"
        static func operation() throws(DatabaseWireError)
            -> JobOperation<CommandRequest, CommandExecuteOperation.Response> {
            try PersistentJobTestOperations.operation(for: Self.self)
        }
    }

    private protocol PersistentJobTestOperation: DatabaseResumableOperation
    where Request == CommandRequest,
          Response == CommandExecuteOperation.Response {
        associatedtype TestDescriptor: PersistentJobTestDescriptor
        static func job()
            throws(DatabaseWireError)
            -> JobOperation<Request, Response>
    }

    private static func jobStep(
        in request: CommandRequest
    ) throws -> JobStepValue {
        guard case .uint8(let value) = request.input["step"] else {
            throw PersistentJobScenarioError.invalidPayload
        }
        return JobStepValue(value)
    }

    private static func commandResponse(
        step: UInt8
    ) -> CommandExecuteOperation.Response {
        .read(output: .uint8(step), continuation: nil)
    }

    private static func commandResponse(
        bytes: ByteString
    ) -> CommandExecuteOperation.Response {
        .read(output: .bytes(bytes), continuation: nil)
    }

    private func completedStep(
        in response: CommandExecuteOperation.Response
    ) throws -> UInt8 {
        guard case .read(let output, nil) = response,
              case .uint8(let value) = output else {
            throw PersistentJobScenarioError.invalidPayload
        }
        return value
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

    private final class SliceExecutionCounter: Sendable {
        private let storage = Mutex(0)

        var count: Int {
            storage.withLock { $0 }
        }

        func recordExecution() {
            storage.withLock { $0 += 1 }
        }
    }

    private actor OperationExecutionGate {
        private var entered = false
        private var released = false
        private var entryWaiters: [CheckedContinuation<Void, Never>] = []
        private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

        func waitUntilEntered() async {
            guard !entered else { return }
            await withCheckedContinuation { continuation in
                entryWaiters.append(continuation)
            }
        }

        func waitForRelease() async {
            if !entered {
                entered = true
                let waiters = entryWaiters
                entryWaiters.removeAll(keepingCapacity: false)
                for waiter in waiters {
                    waiter.resume()
                }
            }
            guard !released else { return }
            await withCheckedContinuation { continuation in
                releaseWaiters.append(continuation)
            }
        }

        func release() {
            guard !released else { return }
            released = true
            let waiters = releaseWaiters
            releaseWaiters.removeAll(keepingCapacity: false)
            for waiter in waiters {
                waiter.resume()
            }
        }
    }

    private struct TwoSliceResumableOperation:
        PersistentJobTestOperation {
        static let stateMarkerID = "job-test-state"
        static let unsuccessfulOutcomeMarkerID =
            "job-test-unsuccessful-outcome"

        typealias TestDescriptor = TwoSliceJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        let compilationCounter: CompilationCounter
        let executionGate: OperationExecutionGate?

        init(
            compilationCounter: CompilationCounter = CompilationCounter(),
            executionGate: OperationExecutionGate? = nil
        ) {
            self.compilationCounter = compilationCounter
            self.executionGate = executionGate
        }

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(1), maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            switch state.value {
            case 0:
                if let executionGate {
                    await executionGate.waitForRelease()
                }
                try await PersistentJobMarker.save(
                    1,
                    identifiedBy: Self.stateMarkerID,
                    using: context.transaction
                )
                return .incomplete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 2,
                    state: JobStepValue(1)
                )
            case 1:
                try await PersistentJobMarker.save(
                    2,
                    identifiedBy: Self.stateMarkerID,
                    using: context.transaction
                )
                return .complete(
                    completedWorkUnits: 1,
                    totalWorkUnits: 2,
                    result: DatabasePersistentJobServiceTests.commandResponse(step: 9)
                )
            default:
                throw PersistentJobScenarioError.invalidContinuation
            }
        }

        func applyUnsuccessfulOutcome(
            plan: JobStepValue,
            state: JobStepValue,
            outcome: DatabaseJobUnsuccessfulOutcome,
            context: DatabaseResumableOperationContext
        ) async throws {
            _ = plan
            _ = state
            let marker: UInt8
            switch outcome {
            case .failed:
                marker = 0xfa
            case .cancelled:
                marker = 0xca
            }
            try await PersistentJobMarker.save(
                marker,
                identifiedBy: Self.unsuccessfulOutcomeMarkerID,
                using: context.transaction
            )
        }
    }

    private struct CheckpointedCancellationOperation:
        PersistentJobTestOperation {
        static let checkpointKey = ByteString(utf8: "job-checkpointed-progress")
        static let unsuccessfulOutcomeStateMarkerID =
            "job-checkpointed-unsuccessful-outcome"

        typealias TestDescriptor = CheckpointedCancellationJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        let executionGate: OperationExecutionGate
        let executionCounter: SliceExecutionCounter

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
            guard request == JobStepValue(13) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return DatabasePreparedResumableJob(
                plan: request,
                initialState: JobStepValue(0),
                sliceTimeoutMilliseconds: 30_000
            )
        }

        func commitModel(
            for plan: JobStepValue
        ) -> DatabaseResumableOperationCommitModel {
            _ = plan
            return .operationCheckpointed
        }

        func runSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            _ = plan
            _ = state
            _ = maximumWorkUnits
            _ = context
            throw PersistentJobScenarioError.unexpectedExecution
        }

        func runCheckpointedSlice(
            plan: JobStepValue,
            state: JobStepValue,
            maximumWorkUnits: UInt64,
            context: DatabaseCheckpointedResumableOperationContext
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(13),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            await executionGate.waitForRelease()
            executionCounter.recordExecution()
            try await context.operationContext.container.engine
                .withTransaction(configuration: .batch) { transaction in
                    try transaction.setValue([1], for: Self.checkpointKey)
                }
            return .incomplete(
                completedWorkUnits: 1,
                totalWorkUnits: 2,
                state: JobStepValue(1)
            )
        }

        func applyUnsuccessfulOutcome(
            plan: JobStepValue,
            state: JobStepValue,
            outcome: DatabaseJobUnsuccessfulOutcome,
            context: DatabaseResumableOperationContext
        ) async throws {
            guard plan == JobStepValue(13),
                  state == JobStepValue(1),
                  outcome == .cancelled else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try await PersistentJobMarker.save(
                state.value,
                identifiedBy: Self.unsuccessfulOutcomeStateMarkerID,
                using: context.transaction
            )
        }
    }

    private struct FiveSliceResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        typealias TestDescriptor = FiveSliceJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
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
                    result: DatabasePersistentJobServiceTests.commandResponse(step: 0x55)
                )
            }
            return .incomplete(
                completedWorkUnits: 1,
                totalWorkUnits: 5,
                state: JobStepValue(next)
            )
        }
    }

    private struct LargeResultResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        typealias TestDescriptor = LargeResultJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        let payload: ByteString

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(7),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            return .complete(
                completedWorkUnits: 1,
                totalWorkUnits: 1,
                result: DatabasePersistentJobServiceTests.commandResponse(bytes: payload)
            )
        }
    }

    private struct RetryingUnsuccessfulOutcomeOperation:
        PersistentJobTestOperation {
        static let markerID = "job-unsuccessful-outcome-marker"

        typealias TestDescriptor = RetryingUnsuccessfulOutcomeJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        let commitProbe: UnsuccessfulOutcomeCommitProbe

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(8),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            throw PersistentJobScenarioError.forcedSliceFailure
        }

        func applyUnsuccessfulOutcome(
            plan: JobStepValue,
            state: JobStepValue,
            outcome: DatabaseJobUnsuccessfulOutcome,
            context: DatabaseResumableOperationContext
        ) async throws {
            _ = plan
            _ = state
            let marker: UInt8
            switch outcome {
            case .failed:
                marker = 0xde
            case .cancelled:
                marker = 0xca
            }
            try await PersistentJobMarker.save(
                marker,
                identifiedBy: Self.markerID,
                using: context.transaction
            )
            try commitProbe.recordAttempt()
        }
    }

    private struct OversizedPlanResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        static let markerID = "job-oversized-plan-marker"

        typealias TestDescriptor = OversizedPlanJob
        typealias Plan = JobPayload
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
            guard request == JobStepValue(9) else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try await PersistentJobMarker.save(
                0x09,
                identifiedBy: Self.markerID,
                using: context.transaction
            )
            return DatabasePreparedResumableJob(
                plan: JobPayload(
                    ByteString(
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            _ = plan
            _ = state
            _ = maximumWorkUnits
            _ = context
            throw PersistentJobScenarioError.unexpectedExecution
        }
    }

    private struct OversizedStateResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        static let markerID = "job-oversized-state-marker"

        typealias TestDescriptor = OversizedStateJob
        typealias Plan = JobStepValue
        typealias State = JobPayload

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(10),
                  state.value.isEmpty,
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try await PersistentJobMarker.save(
                0x0a,
                identifiedBy: Self.markerID,
                using: context.transaction
            )
            return .incomplete(
                completedWorkUnits: 1,
                totalWorkUnits: 2,
                state: JobPayload(
                    ByteString(
                        [UInt8](
                            repeating: 0x0a,
                            count: 64 * 1_024
                        )
                    )
                )
            )
        }
    }

    private struct OversizedResultResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        static let markerID = "job-oversized-result-marker"

        typealias TestDescriptor = OversizedResultJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(11),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            try await PersistentJobMarker.save(
                0x0b,
                identifiedBy: Self.markerID,
                using: context.transaction
            )
            return .complete(
                completedWorkUnits: 1,
                totalWorkUnits: 1,
                result: DatabasePersistentJobServiceTests.commandResponse(
                    bytes: ByteString(
                        [UInt8](
                            repeating: 0x0b,
                            count: 4 * 1_024 * 1_024
                        )
                    )
                )
            )
        }
    }

    private struct AlwaysFailingResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation {
        typealias TestDescriptor = AlwaysFailingJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        let failure: RemoteOperationError

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
            guard request == JobStepValue(12) else {
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
            guard plan == JobStepValue(12),
                  state == JobStepValue(0),
                  maximumWorkUnits == 1 else {
                throw PersistentJobScenarioError.invalidPayload
            }
            _ = context
            throw failure
        }
    }

    private final class RetryingResumableOperation:
        PersistentJobTestOperation,
        DatabaseUnsuccessfulOutcomeIndependentOperation,
        Sendable {
        typealias TestDescriptor = RetryingJob
        typealias Plan = JobStepValue
        typealias State = JobStepValue

        static func job() throws(DatabaseWireError)
            -> JobOperation<Request, Response> {
            try TestDescriptor.operation()
        }

        private let shouldFailSecondSlice = Mutex(true)

        func compile(
            _ request: CommandRequest,
            context: DatabaseResumableOperationStartContext
        ) async throws -> DatabasePreparedResumableJob<Plan, State> {
            let request = try DatabasePersistentJobServiceTests.jobStep(in: request)
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
        ) async throws -> DatabaseResumableOperationSlice<State, Response> {
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
                    throw RemoteOperationError(
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
                    result: DatabasePersistentJobServiceTests.commandResponse(step: 0x33)
                )
            default:
                throw PersistentJobScenarioError.invalidContinuation
            }
        }
    }

    private final class FixedDatabaseWallClock: DatabaseWallClock, Sendable {
        private let timestamp: Mutex<Timestamp>

        init(initial: Timestamp) {
            self.timestamp = Mutex(initial)
        }

        func now() -> Timestamp {
            timestamp.withLock { $0 }
        }

        func advance(milliseconds: UInt32) throws {
            try timestamp.withLock { timestamp in
                let addedNanoseconds = UInt64(milliseconds % 1_000)
                    * 1_000_000
                let nanoseconds = UInt64(timestamp.nanoseconds)
                    + addedNanoseconds
                timestamp = try Timestamp(
                    secondsSinceUnixEpoch:
                        timestamp.secondsSinceUnixEpoch
                        + Int64(milliseconds / 1_000)
                        + Int64(nanoseconds / 1_000_000_000),
                    nanoseconds: UInt32(nanoseconds % 1_000_000_000)
                )
            }
        }
    }

    private final class UnsuccessfulOutcomeCommitProbe: Sendable {
        private struct State: Sendable {
            var remainingFailures: Int
            var attemptCount: Int
        }

        private let state: Mutex<State>

        init(failureCount: Int) {
            self.state = Mutex(State(
                remainingFailures: failureCount,
                attemptCount: 0
            ))
        }

        var attemptCount: Int {
            state.withLock { $0.attemptCount }
        }

        func recordAttempt() throws {
            let shouldFail = state.withLock { state in
                state.attemptCount += 1
                guard state.remainingFailures > 0 else { return false }
                state.remainingFailures -= 1
                return true
            }
            if shouldFail {
                throw PersistentJobScenarioError.unsuccessfulOutcomeCommitFailure
            }
        }
    }

    private actor RecordingDatabaseJobScheduler: DatabaseJobScheduler {
        private var timestamps: [Timestamp] = []

        func ensureWakeUp(
            noLaterThan timestamp: Timestamp
        ) async throws {
            timestamps.append(timestamp)
        }

        func scheduledCount() -> Int {
            timestamps.count
        }

        func requestedTimestamps() -> [Timestamp] {
            timestamps
        }
    }

    private actor FailOnceScheduler: DatabaseJobScheduler {
        private var attempts = 0

        func ensureWakeUp(
            noLaterThan timestamp: Timestamp
        ) async throws {
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

        func generate() -> DatabaseTypes.UUID {
            nextValue.withLock { value in
                defer { value += 1 }
                return DatabaseTypes.UUID(high: 0, low: value)
            }
        }
    }

    private enum PersistentJobScenarioError: Error {
        case invalidPayload
        case invalidContinuation
        case forcedSliceFailure
        case unsuccessfulOutcomeCommitFailure
        case unexpectedExecution
        case missingEntity
        case schedulerFailure
    }
}
