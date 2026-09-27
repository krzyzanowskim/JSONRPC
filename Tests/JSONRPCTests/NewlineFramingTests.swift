import XCTest
import JSONRPC

#if compiler(>=5.9)

final class NewlineFramingTests: XCTestCase {
	/// Feeds `chunks` into a raw channel, wraps it with newline framing, and collects the framed messages.
	private func frame(_ chunks: [Data], maxLineLength: Int = 256 << 20, skipped: Recorder = Recorder()) async -> [String] {
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

	private func chunked(_ data: Data, nextChunkSize: () -> Int) -> [Data] {
		var chunks = [Data]()
		var offset = 0

		while offset < data.count {
			let end = min(data.count, offset + nextChunkSize())
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

		let messages = await frame(chunked(wire, nextChunkSize: { Int.random(in: 1...300) }))

		XCTAssertEqual(messages, expected)
	}

	func testMultipleMessagesPerChunk() async {
		let messages = await frame([Data("{\"a\":1}\n{\"b\":2}\n{\"c\":".utf8), Data("3}\n".utf8)])

		XCTAssertEqual(messages, [#"{"a":1}"#, #"{"b":2}"#, #"{"c":3}"#])
	}

	func testNonJSONLinesAreSkipped() async {
		let skipped = Recorder()

		let messages = await frame(
			[Data("Debugger attached.\n\n   \n{\"a\":1}\nWARN: something\n[1,2]\n{\"tail\":true}".utf8)],
			skipped: skipped
		)

		XCTAssertEqual(messages, [#"{"a":1}"#, "[1,2]", #"{"tail":true}"#])
		XCTAssertEqual(skipped.strings, ["Debugger attached.", "   ", "WARN: something"])
	}

	func testOverlongLineIsDroppedUpToItsNewline() async {
		let skipped = Recorder()
		let long = "{\"p\":\"" + String(repeating: "a", count: 1000)

		// the rest of the overlong line looks like a JSON message on its own
		let messages = await frame(
			[Data(long.utf8), Data("{\"inner\":1}\"}\n{\"ok\":1}\n".utf8)],
			maxLineLength: 512,
			skipped: skipped
		)

		XCTAssertEqual(messages, [#"{"ok":1}"#])
		XCTAssertEqual(skipped.data.map(\.count), [256])
	}

	func testOverlongLineInOneChunkIsDropped() async {
		let skipped = Recorder()
		let long = "{\"p\":\"" + String(repeating: "a", count: 1000) + "\"}"

		let messages = await frame([Data((long + "\n{\"ok\":1}\n").utf8)], maxLineLength: 512, skipped: skipped)

		XCTAssertEqual(messages, [#"{"ok":1}"#])
		XCTAssertEqual(skipped.data.map(\.count), [256])
	}

	func testSkippedLineShorterThanReportPrefixIsReportedWhole() async {
		let skipped = Recorder()
		let long = #"{"p":"aaaaaaaaaaaaa"}"#

		let messages = await frame([Data((long + "\n" + #"{"ok":1}"# + "\n").utf8)], maxLineLength: 8, skipped: skipped)

		XCTAssertEqual(messages, [#"{"ok":1}"#])
		XCTAssertEqual(skipped.strings, [long])
	}

	func testLineAtMaxLengthIsKept() async {
		let line = "{\"p\":\"" + String(repeating: "a", count: 504) + "\"}"
		XCTAssertEqual(line.utf8.count, 512)

		let messages = await frame([Data((line + "\n").utf8)], maxLineLength: 512)

		XCTAssertEqual(messages, [line])
	}

	func testOverlongUnterminatedLineIsNotFlushed() async {
		let messages = await frame([Data(("{\"p\":\"" + String(repeating: "a", count: 1000)).utf8)], maxLineLength: 512)

		XCTAssertEqual(messages, [])
	}

	/// Compares the framing with a naive splitter of the whole input, over random lines and random chunking.
	func testMatchesReferenceSplitter() async {
		var rng = SplitMix64(seed: 0xAC9)

		for iteration in 0..<300 {
			let maxLineLength = [4, 8, 40, 300, 256 << 20][Int.random(in: 0..<5, using: &rng)]
			let pieces = [#"{"a":1}"#, "[1]", "  {\"b\":2}", "log line", "", "\r", #"{"c":"x"}"# + "\r",
						  #"{"p":""# + String(repeating: "q", count: 290) + #""}"#, String(repeating: "z", count: 700)]

			var wire = Data()
			for _ in 0..<Int.random(in: 0...12, using: &rng) {
				wire += Data(pieces[Int.random(in: 0..<pieces.count, using: &rng)].utf8)
				wire.append(0x0A)
			}
			if Bool.random(using: &rng) {
				wire += Data(pieces[Int.random(in: 0..<pieces.count, using: &rng)].utf8)
			}

			let skipped = Recorder()
			let chunks = chunked(wire, nextChunkSize: { Int.random(in: 1...64, using: &rng) })
			let messages = await frame(chunks, maxLineLength: maxLineLength, skipped: skipped)
			let expected = referenceFrame(wire, maxLineLength: maxLineLength)

			XCTAssertEqual(messages, expected.messages, "iteration \(iteration)")
			XCTAssertEqual(skipped.strings, expected.skipped, "iteration \(iteration)")
		}
	}

	private func referenceFrame(_ wire: Data, maxLineLength: Int) -> (messages: [String], skipped: [String]) {
		var messages = [String]()
		var skipped = [String]()

		for raw in wire.split(separator: 0x0A, omittingEmptySubsequences: false) {
			if raw.count > maxLineLength {
				skipped.append(String(decoding: raw.prefix(256), as: UTF8.self))
				continue
			}

			let line = raw.last == 0x0D ? raw.dropLast() : raw
			let first = line.first { $0 != 0x20 && $0 != 0x09 }

			if first == UInt8(ascii: "{") || first == UInt8(ascii: "[") {
				messages.append(String(decoding: line, as: UTF8.self))
			} else if !line.isEmpty {
				skipped.append(String(decoding: line, as: UTF8.self))
			}
		}

		return (messages, skipped)
	}

	func testWriteAppendsNewline() async throws {
		let written = Recorder()
		let pair = DataChannel.DataSequence.makeStream()
		let channel = DataChannel(writeHandler: { written.append($0) }, dataSequence: pair.stream).withNewlineFraming()

		try await channel.writeHandler(Data(#"{"x":1}"#.utf8))

		XCTAssertEqual(written.strings, ["{\"x\":1}\n"])
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
			let messages = await frame(chunked(wire, nextChunkSize: { chunkSize }))

			XCTAssertEqual(messages.count, 2)
			XCTAssertEqual(messages.first?.utf8.count, message.count)
		}
	}
}

/// Small deterministic generator, so a failing fuzz iteration can be reproduced.
private struct SplitMix64: RandomNumberGenerator {
	private var state: UInt64

	init(seed: UInt64) {
		state = seed
	}

	mutating func next() -> UInt64 {
		state &+= 0x9E3779B97F4A7C15
		var z = state
		z = (z ^ (z >> 30)) &* 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) &* 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
}

#endif
