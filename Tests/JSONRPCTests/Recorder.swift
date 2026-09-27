import Foundation
import XCTest

/// Thread-safe log of the data passed to a channel's write handler or a framing callback.
final class Recorder: @unchecked Sendable {
	private let lock = NSLock()
	private var items = [Data]()

	func append(_ data: Data) {
		lock.withLock { items.append(data) }
	}

	var data: [Data] {
		lock.withLock { items }
	}

	var strings: [String] {
		data.map { String(decoding: $0, as: UTF8.self) }
	}

	/// Waits until `count` JSON-RPC messages were recorded and returns their `method` fields.
	func waitForMethods(count: Int, timeout: TimeInterval = 5) async throws -> [String] {
		let deadline = Date().addingTimeInterval(timeout)

		while Date() < deadline {
			let snapshot = data

			if snapshot.count >= count {
				return try snapshot.map { message in
					let object = try JSONSerialization.jsonObject(with: message) as? [String: Any]
					return object?["method"] as? String ?? ""
				}
			}

			try await Task.sleep(nanoseconds: 5_000_000)
		}

		XCTFail("timed out waiting for \(count) writes")
		return []
	}
}
