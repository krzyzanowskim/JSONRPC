import XCTest
import JSONRPC

#if compiler(>=5.9)

final class JSONRPCSessionWriteTests: XCTestCase {
	private struct WriteFailed: Error {}

	/// Messages are written in the order the calls reach the session.
	///
	/// A notification whose encoding blocks keeps the session busy, so the request and the
	/// notification calls both queue up behind it, in that order.
	func testRequestIsWrittenBeforeLaterNotification() async throws {
		let writes = Recorder()
		let gate = EncodingGate()
		let pair = DataChannel.DataSequence.makeStream()
		let session = JSONRPCSession(channel: DataChannel(writeHandler: { writes.append($0) }, dataSequence: pair.stream))

		let blocker = Task { try await session.sendNotification(gate, method: "blocker") }
		try await gate.waitUntilEncoding()

		let request = Task { _ = try? await session.sendDataRequest("first", method: "request") }
		try await Task.sleep(nanoseconds: 50_000_000)
		let notification = Task { try await session.sendNotification("second", method: "notification") }
		try await Task.sleep(nanoseconds: 50_000_000)

		gate.open()
		try await blocker.value
		try await notification.value

		let methods = try await writes.waitForMethods(count: 3)
		XCTAssertEqual(methods, ["blocker", "request", "notification"])

		request.cancel()
	}

	func testWritesAreSerialized() async throws {
		let tracker = ConcurrencyTracker()
		let pair = DataChannel.DataSequence.makeStream()
		let channel = DataChannel(
			writeHandler: { _ in
				tracker.begin()
				try await Task.sleep(nanoseconds: 1_000_000)
				tracker.end()
			},
			dataSequence: pair.stream
		)
		let session = JSONRPCSession(channel: channel)

		try await withThrowingTaskGroup(of: Void.self) { group in
			for i in 0..<50 {
				group.addTask { try await session.sendNotification("\(i)", method: "note") }
			}
			try await group.waitForAll()
		}

		XCTAssertEqual(tracker.completed, 50)
		XCTAssertEqual(tracker.maxInFlight, 1)
	}

	func testNotificationWriteErrorIsThrown() async throws {
		let pair = DataChannel.DataSequence.makeStream()
		let session = JSONRPCSession(channel: DataChannel(writeHandler: { _ in throw WriteFailed() }, dataSequence: pair.stream))

		do {
			try await session.sendNotification("hello", method: "note")
			XCTFail("expected an error")
		} catch is WriteFailed {
		}
	}

	func testRequestWriteErrorFailsRequest() async throws {
		let pair = DataChannel.DataSequence.makeStream()
		let session = JSONRPCSession(channel: DataChannel(writeHandler: { _ in throw WriteFailed() }, dataSequence: pair.stream))

		do {
			_ = try await session.sendDataRequest("hello", method: "request")
			XCTFail("expected an error")
		} catch is WriteFailed {
		}
	}

	func testClosedChannelThrowsPublicTransportError() async throws {
		let pair = DataChannel.DataSequence.makeStream()
		let session = JSONRPCSession(channel: DataChannel(writeHandler: { _ in }, dataSequence: pair.stream))

		pair.continuation.finish()

		// wait for the session to observe the closed read side
		for await _ in await session.eventSequence {}

		do {
			try await session.sendNotification("hello", method: "note")
			XCTFail("expected an error")
		} catch ProtocolTransportError.dataStreamClosed {
		}
	}
}

/// Encodes as a string, but blocks the encoding thread until `open()` is called, or for at most 5 seconds
/// so a broken test fails instead of hanging.
private final class EncodingGate: Encodable, @unchecked Sendable {
	private let lock = NSLock()
	private var started = false
	private let gate = DispatchSemaphore(value: 0)

	func encode(to encoder: Encoder) throws {
		lock.withLock { started = true }
		_ = gate.wait(timeout: .now() + 5)

		var container = encoder.singleValueContainer()
		try container.encode("gate")
	}

	func waitUntilEncoding() async throws {
		while !lock.withLock({ started }) {
			try await Task.sleep(nanoseconds: 1_000_000)
		}
	}

	func open() {
		gate.signal()
	}
}

private final class ConcurrencyTracker: @unchecked Sendable {
	private let lock = NSLock()
	private var inFlight = 0
	private var _maxInFlight = 0
	private var _completed = 0

	var maxInFlight: Int { lock.withLock { _maxInFlight } }
	var completed: Int { lock.withLock { _completed } }

	func begin() {
		lock.withLock {
			inFlight += 1
			_maxInFlight = max(_maxInFlight, inFlight)
		}
	}

	func end() {
		lock.withLock {
			inFlight -= 1
			_completed += 1
		}
	}
}

#endif
