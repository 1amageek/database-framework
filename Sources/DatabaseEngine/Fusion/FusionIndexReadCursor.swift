import DatabaseKit
import DatabaseTypes
import StorageKit

package struct FusionIndexReadCursor: Sendable {
    private let state: FusionIndexReadCursorState

    internal init(state: FusionIndexReadCursorState) {
        self.state = state
    }

    package func next() async throws -> FusionIndexReadRow? {
        try await state.next()
    }

    package func finish() async throws {
        try await state.finish()
    }
}

internal actor FusionIndexReadCursorState {
    private enum State {
        case open(KeyValueCursor)
        case finished((any Error)?)
    }

    private let session: FusionIndexReadSession
    private var state: State

    internal init(
        session: FusionIndexReadSession,
        cursor: consuming KeyValueCursor,
        reservation: DatabaseIntermediateReservation
    ) {
        self.session = session
        self.state = .open(cursor)
        self.reservation = reservation
    }

    private let reservation: DatabaseIntermediateReservation

    internal func next() async throws -> FusionIndexReadRow? {
        switch state {
        case .finished(.some(let error)):
            throw error
        case .finished(nil):
            return nil
        case .open:
            break
        }

        try session.beginCursorOperation()
        defer { session.endCursorOperation() }

        guard case .open(var cursor) = state else {
            preconditionFailure("Cursor state changed without suspension")
        }
        try session.admitCursorRead()
        let row: (ByteString, ByteString)?
        do {
            row = try await cursor.next()
        } catch let cleanupError as StorageRangeTerminalCleanupError {
            // KeyValueCursor reached the end of iteration and only automatic
            // cleanup failed. Preserve that distinction without re-finishing.
            complete()
            throw cleanupError
        } catch let cleanupError as StorageRangeCleanupError {
            // KeyValueCursor has already completed terminal backend cleanup
            // and preserved both failures. Re-finishing it would duplicate
            // the cleanup error inside a second wrapper.
            complete()
            throw cleanupError
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                complete()
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            complete()
            throw iterationError
        }
        do {
            try session.checkpointCursorOperation()
        } catch {
            let cancellationError = error
            do {
                try await cursor.finish()
            } catch {
                complete()
                throw StorageRangeCleanupError(
                    iterationError: cancellationError,
                    cleanupError: error
                )
            }
            complete()
            throw cancellationError
        }
        guard let row else {
            do {
                try await cursor.finish()
            } catch {
                complete()
                throw error
            }
            complete()
            try session.checkpointCursorOperation()
            return nil
        }
        do {
            try session.validateCursorKey(row.0)
        } catch {
            let validationError = error
            do {
                try await cursor.finish()
            } catch {
                complete()
                throw StorageRangeCleanupError(
                    iterationError: validationError,
                    cleanupError: error
                )
            }
            complete()
            throw validationError
        }
        let retained: FusionIndexReadRow
        do {
            retained = try session.retainRow(
                key: row.0,
                value: row.1
            )
        } catch {
            let admissionError = error
            do {
                try await cursor.finish()
            } catch {
                complete()
                throw StorageRangeCleanupError(
                    iterationError: admissionError,
                    cleanupError: error
                )
            }
            complete()
            throw admissionError
        }
        state = .open(cursor)
        return retained
    }

    internal func finish() async throws {
        switch state {
        case .finished(.some(let error)):
            throw error
        case .finished(nil):
            return
        case .open:
            break
        }

        try session.beginCursorOperation()
        defer { session.endCursorOperation() }
        guard case .open(var cursor) = state else {
            preconditionFailure("Cursor state changed without suspension")
        }
        do {
            try await cursor.finish()
            complete()
            try session.checkpointCursorOperation()
        } catch {
            complete()
            throw error
        }
    }

    internal func finishForInvalidation() async throws {
        let invalidationError = FusionExecutionContractError
            .indexReadSessionInvalidated(
                index: session.index.descriptor.name
            )
        switch state {
        case .open(var cursor):
            do {
                try await cursor.finish()
                complete(with: invalidationError)
            } catch {
                complete(with: invalidationError)
                throw error
            }
        case .finished:
            return
        }
    }

    private func complete(with error: (any Error)? = nil) {
        state = .finished(error)
        session.removeCursor(self)
        reservation.release()
    }
}
