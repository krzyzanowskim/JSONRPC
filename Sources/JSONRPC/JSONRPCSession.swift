import Foundation

public enum ProtocolTransportError: Error {
	case undecodableMessage(Data)
	case unexpectedResponse(Data)
	case abandonedRequest
	case dataStreamClosed
}

private struct JSONRPCRequestReplyEncodableShim: Encodable {
	let id: JSONId
	let result: JSONRPCEvent.RequestResult

	private enum CodingKeys: String, CodingKey {
		case id
		case error
		case result
		case jsonrpc
	}

	func encode(to encoder: Encoder) throws {
		var container = encoder.container(keyedBy: CodingKeys.self)

		try container.encode("2.0", forKey: .jsonrpc)

		try container.encode(id, forKey: .id)

		switch result {
		case .failure(let error):
			try container.encode(error, forKey: .error)
		case .success(let value):
			try container.encode(value, forKey: .result)
		}
	}
}

public enum JSONRPCEvent: Sendable {
	public typealias RequestResult = Result<Encodable & Sendable, AnyJSONRPCResponseError>
	public typealias RequestHandler = @Sendable (RequestResult) async -> Void

	case request(AnyJSONRPCRequest, RequestHandler, Data)
	case notification(AnyJSONRPCNotification, Data)
	case error(Error)
}

/// A message waiting for the writer task, with a handler called once the write finished or failed.
private struct PendingWrite: Sendable {
	let data: Data
	let completion: @Sendable (Error?) -> Void
}

public actor JSONRPCSession {
	public typealias EventSequence = AsyncStream<JSONRPCEvent>
	public typealias DataResult = Result<(AnyJSONRPCResponse, Data), Error>
	private typealias MessageResponder = @Sendable (DataResult) -> Void
	private typealias WriteSequence = AsyncStream<PendingWrite>

	private var id: Int
	private let decoder = JSONDecoder()
	private let encoder = JSONEncoder()
	private let channel: DataChannel
	private var readTask: Task<Void, Never>?
	private let writeContinuation: WriteSequence.Continuation
	private let eventContinuation: EventSequence.Continuation
	private var responders = [String: MessageResponder]()
	private var channelClosed = false

	public let eventSequence: EventSequence

	public init(channel: DataChannel) {
		self.id = 1
		self.channel = channel

		// this is annoying, but temporary
//#if compiler(>=5.9)
//		(self.eventSequence, self.eventContinuation) = EventSequence.makeStream()
//#else
		var escapedEventContinuation: EventSequence.Continuation?

		self.eventSequence = EventSequence { escapedEventContinuation = $0 }
		self.eventContinuation = escapedEventContinuation!
//#endif

		// All writes go through one task, in the order they were enqueued on the actor.
		// Writing from separate tasks would let a later message overtake an earlier one,
		// and let concurrent writes interleave on the channel.
		let (writeSequence, writeContinuation) = WriteSequence.makeStream()
		let writeHandler = channel.writeHandler

		self.writeContinuation = writeContinuation
		Task {
			for await pendingWrite in writeSequence {
				do {
					try await writeHandler(pendingWrite.data)
					pendingWrite.completion(nil)
				} catch {
					pendingWrite.completion(error)
				}
			}
		}

		Task {
			await startMonitoringChannel()
		}
	}

	deinit {
		eventContinuation.finish()
		// lets already queued writes drain, then ends the writer task
		writeContinuation.finish()
		readTask?.cancel()

		for (_, responder) in responders {
			responder(.failure(ProtocolTransportError.dataStreamClosed))
		}
	}

	private func generateID() -> JSONId {
		let issuedId = JSONId.numericId(id)

		id += 1

		return issuedId
	}

	/// Encodes `value` and queues it for writing. The write position is fixed when this is called.
	private func enqueueWrite<T>(_ value: T, completion: @escaping @Sendable (Error?) -> Void) where T: Encodable {
		if channelClosed {
			completion(ProtocolTransportError.dataStreamClosed)
			return
		}

		let data: Data

		do {
			data = try encoder.encode(value)
		} catch {
			completion(error)
			return
		}

		if case .terminated = writeContinuation.yield(PendingWrite(data: data, completion: completion)) {
			completion(ProtocolTransportError.dataStreamClosed)
		}
	}

	private func encodeAndWrite<T>(_ value: T) async throws where T: Encodable {
		try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
			enqueueWrite(value) { error in
				if let error {
					continuation.resume(throwing: error)
				} else {
					continuation.resume()
				}
			}
		}
	}

	private func requestWriteFailed(key: String, error: Error) {
		// the responder may have already been resolved, by the channel closing or cancellation
		responders.removeValue(forKey: key)?(.failure(error))
	}

	private func readSequenceFinished() {
		for (_, responder) in responders {
			responder(.failure(ProtocolTransportError.dataStreamClosed))
		}

		self.responders.removeAll()
		channelClosed = true

		eventContinuation.finish()
	}

	private func cancelRequest(_ key: String) {
		guard let responder = responders.removeValue(forKey: key) else {
			return
		}

		responder(.failure(CancellationError()))
	}

	private func startMonitoringChannel() {
		precondition(readTask == nil)

		let dataSequence = channel.dataSequence

		let task = Task { [weak self] in
			for await data in dataSequence {
				await self?.handleData(data)
			}

			await self?.readSequenceFinished()
		}

		self.readTask = task
	}

	private func handleData(_ data: Data) {
		do {
			try self.decodeAndDispatch(data: data)
		} catch {
			eventContinuation.yield(.error(error))
		}
	}

	private func decodeAndDispatch(data: Data) throws {
		let msg = try decoder.decode(JSONRPCMessage.self, from: data)

		switch msg {
		case .notification(let method, let params):
			let note = AnyJSONRPCNotification(method: method, params: params)

			eventContinuation.yield(.notification(note, data))
		case .response(let id):
			let resp = AnyJSONRPCResponse(id: id, result: nil)

			try dispatchResponse(resp, originalData: data)
		case .request(let id, let method, let params):
			let req = AnyJSONRPCRequest(id: id, method: method, params: params)

			let handler: JSONRPCEvent.RequestHandler = { [weak self] in
				let resp = JSONRPCRequestReplyEncodableShim(id: id, result: $0)

				do {
					try await self?.encodeAndWrite(resp)
				} catch {
					self?.eventContinuation.yield(.error(error))
				}
			}

			eventContinuation.yield(.request(req, handler, data))
		case .undecodableId(let error):
			eventContinuation.yield(.error(error))
		}
	}

	private func dispatchResponse(_ message: AnyJSONRPCResponse, originalData data: Data) throws {
		let key = message.id.description

		guard let responder = responders.removeValue(forKey: key) else {
			throw ProtocolTransportError.unexpectedResponse(data)
		}

		responder(.success((message, data)))
	}
}

extension JSONRPCSession {
	public func sendDataRequest<Request>(_ params: Request, method: String) async throws -> (AnyJSONRPCResponse, Data)
	where Request: Encodable {
		if channelClosed {
			throw ProtocolTransportError.dataStreamClosed
		}

		try Task.checkCancellation()

		let issuedId = generateID()

		return try await withTaskCancellationHandler {
			try await withCheckedThrowingContinuation({ continuation in
				// make sure not to capture self
				self.sendDataRequest(params, method: method, id: issuedId) { [weak self] result in
					guard self != nil else {
						continuation.resume(throwing: ProtocolTransportError.abandonedRequest)
						return
					}

					continuation.resume(with: result)
				}
			})
		} onCancel: {
			Task { await self.cancelRequest(issuedId.description) }
		}
	}

	public func sendRequest<Request, Response>(_ params: Request, method: String) async throws -> JSONRPCResponse<Response>
	where Request: Encodable, Response: Decodable {
		let (_, data) = try await sendDataRequest(params, method: method)

		return try decoder.decode(JSONRPCResponse<Response>.self, from: data)
	}

	public func sendNotification<Note>(_ params: Note, method: String) async throws where Note: Encodable {
		if channelClosed {
			throw ProtocolTransportError.dataStreamClosed
		}

		let notification = JSONRPCNotification(method: method, params: params)
		
		try await encodeAndWrite(notification)
	}

	public func sendNotification(method: String) async throws {
		let unusedParams: String? = nil

		try await sendNotification(unusedParams, method: method)
	}

	public func response<Request, Response>(to method: String, params: Request) async throws -> Response
	where Request: Encodable, Response: Decodable {
		let (_, data) = try await sendDataRequest(params, method: method)

		let response = try decoder.decode(JSONRPCResponse<Response>.self, from: data)

		return try response.content.get()
	}

	public func response<Response>(to method: String) async throws -> Response
	where Response: Decodable {
		let unusedParams: String? = nil

		return try await response(to: method, params: unusedParams)
	}
}

extension JSONRPCSession {
	private func sendDataRequest<Request>(
		_ params: Request, method: String, id issuedId: JSONId,
		responseHandler: @escaping MessageResponder
	) where Request: Encodable {
		let request = JSONRPCRequest(id: issuedId, method: method, params: params)

		// make sure to store the responser *first*, before sending the message. This prevents a race where the response comes in so fast we aren't yet waiting for it
		let key = issuedId.description

		precondition(responders[key] == nil)

		self.responders[key] = responseHandler

		enqueueWrite(request) { [weak self] error in
			guard let error else { return }

			Task { await self?.requestWriteFailed(key: key, error: error) }
		}
	}
}
