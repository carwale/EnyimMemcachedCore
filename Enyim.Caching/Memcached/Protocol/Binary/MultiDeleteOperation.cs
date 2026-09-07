using System;
using System.Collections.Generic;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class MultiDeleteOperation : BinaryMultiItemOperation, IMultiDeleteOperation
	{
		private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(MultiDeleteOperation));

		private Dictionary<int, string> idToKey;
		private int noopId;

		public MultiDeleteOperation(IList<string> keys) : base(keys) { }

		protected override BinaryRequest Build(string key)
		{
			return new BinaryRequest(OpCode.DeleteQ)
			{
				Key = key
			};
		}

		protected internal override IList<ArraySegment<byte>> GetBuffer()
		{
			var keys = this.Keys;

			if (keys == null || keys.Count == 0)
			{
				if (log.IsWarnEnabled) log.Warn("Empty multi-delete!");

				return new ArraySegment<byte>[0];
			}

			if (log.IsDebugEnabled)
				log.DebugFormat("Building multi-delete for {0} keys", keys.Count);

			this.idToKey = new Dictionary<int, string>();

			var buffers = new List<ArraySegment<byte>>(keys.Count * 2 + 1);

			foreach (var key in keys)
			{
				var request = this.Build(key);
				request.CreateBuffer(buffers);
				idToKey[request.CorrelationId] = key;
			}

			var noop = new BinaryRequest(OpCode.NoOp);
			this.noopId = noop.CorrelationId;
			noop.CreateBuffer(buffers);

			return buffers;
		}

		protected internal override IOperationResult ReadResponse(PooledSocket socket)
		{
			var result = new BinaryOperationResult();
			var response = new BinaryResponse();

			while (true)
			{
				response.Read(socket);
				this.StatusCode = response.StatusCode;

				// No header was read (socket already dead). CorrelationId may be leftover.
				if (response.StatusCode < 0)
					return result.Fail("Connection closed before multi-delete completed");

				if (response.CorrelationId == this.noopId)
					return result.Pass();

				// DeleteQ replies only on error. KEY_NOT_FOUND must not stop the batch
				// or leave unread packets (including NoOp) on the socket.
				if (log.IsDebugEnabled)
				{
					string key;
					idToKey.TryGetValue(response.CorrelationId, out key);
					log.DebugFormat("DeleteQ status {0} for key '{1}'", response.StatusCode, key);
				}
			}
		}

		protected internal override System.Threading.Tasks.Task<IOperationResult> ReadResponseAsync(PooledSocket socket)
		{
			throw new NotImplementedException();
		}

		protected internal override bool ReadResponseAsync(PooledSocket socket, Action<bool> next)
		{
			this.currentSocket = socket;
			this.asyncReader = new BinaryResponse();
			this.asyncLoopState = null;
			this.afterAsyncRead = next;

			return this.DoReadAsync();
		}

		private PooledSocket currentSocket;
		private BinaryResponse asyncReader;
		private bool? asyncLoopState;
		private Action<bool> afterAsyncRead;

		private bool DoReadAsync()
		{
			bool ioPending;
			var reader = this.asyncReader;

			while (this.asyncLoopState == null)
			{
				var readSuccess = reader.ReadAsync(this.currentSocket, this.EndReadAsync, out ioPending);
				this.StatusCode = reader.StatusCode;

				if (ioPending) return readSuccess;

				this.ApplyReadResult(reader);
			}

			this.afterAsyncRead((bool)this.asyncLoopState);
			return true;
		}

		private void EndReadAsync(bool readSuccess)
		{
			this.ApplyReadResult(this.asyncReader);
			this.DoReadAsync();
		}

		private void ApplyReadResult(BinaryResponse reader)
		{
			this.StatusCode = reader.StatusCode;

			if (reader.StatusCode < 0)
				this.asyncLoopState = false;
			else if (reader.CorrelationId == this.noopId)
				this.asyncLoopState = true;
			// else DeleteQ error (including KEY_NOT_FOUND): keep reading
		}
	}
}
