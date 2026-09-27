import XCTest
import JSONRPC

#if compiler(>=5.9)

final class JSONRPCSessionWriteTests: XCTestCase {
	private func sleep(milliseconds: UInt64) async throws {
		try await Task.sleep(nanoseconds: milliseconds * 1_000_000)
	}

	/// A request must reach the wire before a notification that is sent after it.
	///
	/// The session actor is kept busy decoding a large inbound message, so the request call is
	/// provably queued on the actor ahead of the notification.
	func testRequestIsWrittenBeforeLaterNotification() async throws {
		let busyMessage = try JSONEncoder().encode(
			JSONRPCNotification(method: "busy", params: String(repeating: "x", count: 64 << 20))
		)

		for iteration in 0..<5 {
			let log = WriteLog()
			let pair = DataChannel.DataSequence.makeStream()
			let session = JSONRPCSession(channel: DataChannel(writeHandler: { log.append($0) }, dataSequence: pair.stream))

			try await sleep(milliseconds: 5)
			pair.continuation.yield(busyMessage)
			try await sleep(milliseconds: 5)

			let request = Task { _ = try? await session.sendDataRequest("first", method: "request") }
			try await sleep(milliseconds: 10)
			try await session.sendNotification("second", method: "notification")

			let methods = try await log.waitForMethods(count: 2)
			XCTAssertEqual(methods, ["request", "notification"], "iteration \(iteration)")

			request.cancel()
			pair.continuation.finish()
		}
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
		struct WriteFailed: Error {}

		let pair = DataChannel.DataSequence.makeStream()
		let session = JSONRPCSession(channel: DataChannel(writeHandler: { _ in throw WriteFailed() }, dataSequence: pair.stream))

		do {
			try await session.sendNotification("hello", method: "note")
			XCTFail("expected an error")
		} catch is WriteFailed {
		}
	}

	func testRequestWriteErrorFailsRequest() async throws {
		struct WriteFailed: Error {}

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

private final class WriteLog: @unchecked Sendable {
	private let lock = NSLock()
	private var items = [Data]()

	func append(_ data: Data) {
		lock.lock()
		items.append(data)
		lock.unlock()
	}

	func waitForMethods(count: Int, timeout: TimeInterval = 5) async throws -> [String] {
		let deadline = Date().addingTimeInterval(timeout)

		while Date() < deadline {
			let snapshot = lock.withLock { items }

			if snapshot.count >= count {
				return try snapshot.map { data in
					let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
					return object?["method"] as? String ?? ""
				}
			}

			try await Task.sleep(nanoseconds: 5_000_000)
		}

		XCTFail("timed out waiting for \(count) writes")
		return []
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
		lock.lock()
		inFlight += 1
		_maxInFlight = max(_maxInFlight, inFlight)
		lock.unlock()
	}

	func end() {
		lock.lock()
		inFlight -= 1
		_completed += 1
		lock.unlock()
	}
}

#endif
