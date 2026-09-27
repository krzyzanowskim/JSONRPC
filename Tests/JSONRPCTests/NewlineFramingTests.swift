import XCTest
import JSONRPC

#if compiler(>=5.9)

final class NewlineFramingTests: XCTestCase {
	/// Feeds `chunks` into a raw channel, wraps it with newline framing, and collects the framed messages.
	private func frame(_ chunks: [Data], maxLineLength: Int = 256 << 20, skipped: LineLog = LineLog()) async -> [String] {
		let pair = DataChannel.DataSequence.makeStream()
		let channel = DataChannel(writeHandler: { _ in }, dataSequence: pair.stream)
			.withNewlineFraming(maxLineLength: maxLineLength, onSkippedLine: { skipped.append($0) })

		for chunk in chunks {
			pair.continuation.yield(chunk)
		}

		pair.continuation.finish()

		var messages = [String]()

		for await message in channel.dataSequence {
			messages.append(String(decoding: message, as: UTF8.self))
		}

		return messages
	}

	private func chunked(_ data: Data, sizes: () -> Int) -> [Data] {
		var chunks = [Data]()
		var offset = 0

		while offset < data.count {
			let end = min(data.count, offset + sizes())
			chunks.append(data.subdata(in: offset..<end))
			offset = end
		}

		return chunks
	}

	func testByteByByteChunks() async {
		let wire = Data(#"{"a":1}"#.utf8) + Data("\n".utf8) + Data(#"{"b":"x\ny"}"#.utf8) + Data("\r\n".utf8)

		let messages = await frame(wire.map { Data([$0]) })

		XCTAssertEqual(messages, [#"{"a":1}"#, #"{"b":"x\ny"}"#])
	}

	func testRandomChunking() async {
		var wire = Data()
		var expected = [String]()

		for i in 0..<500 {
			let message = #"{"id":\#(i),"p":"\#(String(repeating: "z", count: i % 97))"}"#
			expected.append(message)
			wire += Data((message + "\n").utf8)
		}

		let messages = await frame(chunked(wire, sizes: { Int.random(in: 1...300) }))

		XCTAssertEqual(messages, expected)
	}

	func testMultipleMessagesPerChunk() async {
		let messages = await frame([Data("{\"a\":1}\n{\"b\":2}\n{\"c\":".utf8), Data("3}\n".utf8)])

		XCTAssertEqual(messages, [#"{"a":1}"#, #"{"b":2}"#, #"{"c":3}"#])
	}

	func testNonJSONLinesAreSkipped() async {
		let skipped = LineLog()

		let messages = await frame(
			[Data("Debugger attached.\n\n   \n{\"a\":1}\nWARN: something\n[1,2]\n{\"tail\":true}".utf8)],
			skipped: skipped
		)

		XCTAssertEqual(messages, [#"{"a":1}"#, "[1,2]", #"{"tail":true}"#])
		XCTAssertEqual(skipped.lines, ["Debugger attached.", "   ", "WARN: something"])
	}

	func testOverlongLineIsDropped() async {
		let skipped = LineLog()
		let long = "{\"p\":\"" + String(repeating: "a", count: 1000)

		let messages = await frame(
			[Data(long.utf8), Data("\"}\n{\"ok\":1}\n".utf8)],
			maxLineLength: 512,
			skipped: skipped
		)

		// the remainder of the dropped line doesn't start with `{`, so it is skipped too
		XCTAssertEqual(messages, [#"{"ok":1}"#])
		XCTAssertEqual(skipped.lines.first?.utf8.count, 256)
	}

	func testWriteAppendsNewline() async throws {
		let written = LineLog()
		let pair = DataChannel.DataSequence.makeStream()
		let channel = DataChannel(writeHandler: { written.append($0) }, dataSequence: pair.stream).withNewlineFraming()

		try await channel.writeHandler(Data(#"{"x":1}"#.utf8))

		XCTAssertEqual(written.lines, ["{\"x\":1}\n"])
	}

	/// Two sessions talking over a pipe that splits every write into random small chunks.
	func testSessionRoundTripOverRechunkingPipe() async throws {
		let clientToServer = DataChannel.DataSequence.makeStream()
		let serverToClient = DataChannel.DataSequence.makeStream()

		@Sendable func rechunk(_ data: Data, into continuation: DataChannel.DataSequence.Continuation) {
			var offset = 0

			while offset < data.count {
				let end = min(data.count, offset + Int.random(in: 1...7))
				continuation.yield(data.subdata(in: offset..<end))
				offset = end
			}
		}

		let client = JSONRPCSession(channel: DataChannel(
			writeHandler: { rechunk($0, into: clientToServer.continuation) },
			dataSequence: serverToClient.stream
		).withNewlineFraming())

		let server = JSONRPCSession(channel: DataChannel(
			writeHandler: { rechunk($0, into: serverToClient.continuation) },
			dataSequence: clientToServer.stream
		).withNewlineFraming())

		let serverTask = Task {
			for await event in await server.eventSequence {
				if case let .request(request, handler, _) = event {
					await handler(.success("echo:\(request.method)"))
				}
			}
		}

		for i in 0..<200 {
			let response: String = try await client.response(to: "m\(i)", params: ["k": "v\n\(i)"])
			XCTAssertEqual(response, "echo:m\(i)")
		}

		serverTask.cancel()
	}

	func testLargeMessage() async throws {
		let message = try JSONEncoder().encode(
			JSONRPCNotification(method: "big", params: String(repeating: "a", count: 5 << 20))
		)
		var wire = message
		wire.append(0x0A)
		wire += Data(#"{"after":1}"#.utf8) + Data([0x0A])

		for chunkSize in [64 << 10, 4 << 10, wire.count] {
			let messages = await frame(chunked(wire, sizes: { chunkSize }))

			XCTAssertEqual(messages.count, 2)
			XCTAssertEqual(messages.first?.utf8.count, message.count)
		}
	}
}

final class LineLog: @unchecked Sendable {
	private let lock = NSLock()
	private var items = [String]()

	func append(_ data: Data) {
		lock.lock()
		items.append(String(decoding: data, as: UTF8.self))
		lock.unlock()
	}

	var lines: [String] {
		lock.lock()
		defer { lock.unlock() }
		return items
	}
}

#endif
