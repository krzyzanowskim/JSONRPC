import Foundation

extension DataChannel {
	/// Wraps a byte-stream channel (e.g. a child process's stdio pipes) with
	/// newline-delimited JSON framing, as used by the Agent Client Protocol, MCP stdio
	/// and Codex app-server.
	///
	/// - Writes: appends `\n` to each message. `JSONEncoder` escapes newlines inside
	///   strings, so a compact encoding never contains a raw `0x0A`.
	/// - Reads: splits the byte stream on `\n`, strips a trailing `\r`, and yields one
	///   `Data` per line. Blank lines and lines that do not start with `{` or `[`
	///   (stray log output on stdout) are passed to `onSkippedLine` and dropped.
	///   A final unterminated line is flushed when the upstream sequence ends.
	///   A line longer than `maxLineLength` is dropped; its first 256 bytes are passed to `onSkippedLine`.
	public func withNewlineFraming(
		maxLineLength: Int = 256 << 20,
		onSkippedLine: (@Sendable (Data) -> Void)? = nil
	) -> DataChannel {
		let upstreamWrite = writeHandler
		let upstream = dataSequence

		let framedWrite: WriteHandler = { data in
			var line = data
			line.append(0x0A)
			try await upstreamWrite(line)
		}

		let (stream, continuation) = DataSequence.makeStream()

		let reader = Task {
			var buffer = Data()
			var scanned = 0 // bytes of `buffer` already known to contain no newline

			func emit(_ line: Data) {
				var line = line
				if line.last == 0x0D {
					line.removeLast()
				}

				let first = line.first { $0 != 0x20 && $0 != 0x09 }

				if first == UInt8(ascii: "{") || first == UInt8(ascii: "[") {
					continuation.yield(line)
				} else if !line.isEmpty {
					onSkippedLine?(line)
				}
			}

			for await chunk in upstream {
				buffer.append(chunk)

				var lineStart = 0

				buffer.withUnsafeBytes { raw in
					guard let base = raw.baseAddress else { return }

					while scanned < raw.count, let hit = memchr(base + scanned, 0x0A, raw.count - scanned) {
						let newline = base.distance(to: UnsafeRawPointer(hit))

						emit(Data(bytes: base + lineStart, count: newline - lineStart))
						lineStart = newline + 1
						scanned = lineStart
					}
				}

				if lineStart > 0 {
					buffer.removeFirst(lineStart)
				}

				scanned = buffer.count

				if buffer.count > maxLineLength {
					onSkippedLine?(buffer.prefix(256))
					buffer.removeAll(keepingCapacity: false)
					scanned = 0
				}
			}

			if !buffer.isEmpty {
				emit(buffer)
			}

			continuation.finish()
		}

		continuation.onTermination = { _ in reader.cancel() }

		return DataChannel(writeHandler: framedWrite, dataSequence: stream)
	}
}
