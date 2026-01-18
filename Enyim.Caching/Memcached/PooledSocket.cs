//#define DEBUG_IO
using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Dawn.Net.Sockets;
using System.Runtime.InteropServices;
using System.Reflection;

namespace Enyim.Caching.Memcached
{
    [DebuggerDisplay("[ Address: {endpoint}, IsAlive = {IsAlive} ]")]
    public partial class PooledSocket : IDisposable
    {
        /// <summary>
        /// Feature flag to enable modern .NET 6.0 native socket APIs.
        /// Set USE_MODERN_SOCKET environment variable to "true" or "1" to enable.
        /// Cached at startup for performance.
        /// </summary>
        private static readonly bool UseModernSocket = 
            bool.TryParse(Environment.GetEnvironmentVariable("USE_MODERN_SOCKET"), out var value) && value;

        private readonly ILogger _logger;

        private bool isAlive;
        private Socket socket;
        private EndPoint endpoint;

        private Stream inputStream;
        private AsyncSocketHelper helper;
        public DateTime LastConnectionTimestamp { get; set; }

        public PooledSocket(EndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout, ILogger logger)
        {
            _logger = logger;

            this.isAlive = true;

            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // TODO test if we're better off using nagle
            //PHP: OPT_TCP_NODELAY
            //socket.NoDelay = true;

            var timeout = connectionTimeout == TimeSpan.MaxValue
                            ? Timeout.Infinite
                            : (int)connectionTimeout.TotalMilliseconds;

            var rcv = receiveTimeout == TimeSpan.MaxValue
                ? Timeout.Infinite
                : (int)receiveTimeout.TotalMilliseconds;

            socket.ReceiveTimeout = rcv;
            socket.SendTimeout = rcv;
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            // Use async connection when modern socket is enabled, otherwise use legacy blocking approach
            if (UseModernSocket)
            {
                ConnectWithTimeoutAsync(socket, endpoint, timeout, CancellationToken.None).GetAwaiter().GetResult();
            }
            else
            {
                ConnectWithTimeoutLegacy(socket, endpoint, timeout);
            }

            this.socket = socket;
            this.endpoint = endpoint;

            this.inputStream = new BasicNetworkStream(socket);            
        }

        private async Task ConnectWithTimeoutAsync(Socket socket, EndPoint endpoint, int timeout, CancellationToken cancellationToken)
        {
            // Resolve DNS endpoint if needed (non-Windows platforms)
            if (endpoint is DnsEndPoint && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var dnsEndPoint = ((DnsEndPoint)endpoint);
                var host = dnsEndPoint.Host;
                var addresses = await Dns.GetHostAddressesAsync(dnsEndPoint.Host, cancellationToken).ConfigureAwait(false);
                var address = addresses.FirstOrDefault(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
                if (address == null)
                {
                    throw new ArgumentException(String.Format("Could not resolve host '{0}'.", host));
                }
                _logger.LogDebug($"Resolved '{host}' to '{address}'");
                endpoint = new IPEndPoint(address, dnsEndPoint.Port);
            }

            // Use CancellationTokenSource for timeout handling
            using (var cts = timeout == Timeout.Infinite 
                ? new CancellationTokenSource() 
                : new CancellationTokenSource(timeout))
            {
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken))
                {
                    try
                    {
                        await socket.ConnectAsync(endpoint, linkedCts.Token).ConfigureAwait(false);
                        
                        if (!socket.Connected)
                        {
                            socket.Dispose();
                            throw new TimeoutException("Could not connect to " + endpoint);
                        }

                        LastConnectionTimestamp = DateTime.UtcNow;
                    }
                    catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                    {
                        socket.Dispose();
                        throw new TimeoutException("Could not connect to " + endpoint + " within " + timeout + "ms");
                    }
                    catch (SocketException ex)
                    {
                        socket.Dispose();
                        throw new IOException($"Failed to connect to {endpoint}: {ex.SocketErrorCode}", ex);
                    }
                }
            }
        }

        private void ConnectWithTimeoutLegacy(Socket socket, EndPoint endpoint, int timeout)
        {
            // Resolve DNS endpoint if needed (non-Windows platforms)
            if (endpoint is DnsEndPoint && !RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var dnsEndPoint = ((DnsEndPoint)endpoint);
                var host = dnsEndPoint.Host;
                var addresses = Dns.GetHostAddresses(dnsEndPoint.Host);
                var address = addresses.FirstOrDefault(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
                if (address == null)
                {
                    throw new ArgumentException(String.Format("Could not resolve host '{0}'.", host));
                }
                _logger.LogDebug($"Resolved '{host}' to '{address}'");
                endpoint = new IPEndPoint(address, dnsEndPoint.Port);
            }

            var completed = new AutoResetEvent(false);
            var args = new SocketAsyncEventArgs();
            args.RemoteEndPoint = endpoint;
            args.Completed += OnConnectCompleted;
            args.UserToken = completed;
            socket.ConnectAsync(args);
            if (!completed.WaitOne(timeout) || !socket.Connected)
            {
                using (socket)
                {
                    throw new TimeoutException("Could not connect to " + endpoint);
                }
            } else {
                 LastConnectionTimestamp = DateTime.UtcNow;
            }
        }

        private void OnConnectCompleted(object sender, SocketAsyncEventArgs args)
        {
            LastConnectionTimestamp = DateTime.UtcNow;
            EventWaitHandle handle = (EventWaitHandle)args.UserToken;
            handle.Set();
        }

        public Action<PooledSocket> CleanupCallback { get; set; }

        public int Available
        {
            get { return this.socket.Available; }
        }

        public void Reset()
        {
            // discard any buffered data
            this.inputStream.Flush();

            if (this.helper != null) this.helper.DiscardBuffer();

            int available = this.socket.Available;

            if (available > 0)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning("Socket bound to {0} has {1} unread data! This is probably a bug in the code. InstanceID was {2}.", this.socket.RemoteEndPoint, available, this.InstanceId);

                byte[] data = new byte[available];

                this.Read(data, 0, available);

                if (_logger.IsEnabled(LogLevel.Warning))
                    _logger.LogWarning(Encoding.ASCII.GetString(data));
            }

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Socket {0} was reset", this.InstanceId);
        }

        /// <summary>
        /// The ID of this instance. Used by the <see cref="T:MemcachedServer"/> to identify the instance in its inner lists.
        /// </summary>
        public readonly Guid InstanceId = Guid.NewGuid();

        public bool IsAlive
        {
            get { return this.isAlive; }
        }

        /// <summary>
        /// Releases all resources used by this instance and shuts down the inner <see cref="T:Socket"/>. This instance will not be usable anymore.
        /// </summary>
        /// <remarks>Use the IDisposable.Dispose method if you want to release this instance back into the pool.</remarks>
        public void Destroy()
        {
            this.Dispose(true);
        }

        ~PooledSocket()
        {
            try { this.Dispose(true); }
            catch { }
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                GC.SuppressFinalize(this);

                try
                {
                    if (socket != null)
                        try { this.socket.Dispose(); }
                        catch { }

                    if (this.inputStream != null)
                        this.inputStream.Dispose();

                    this.inputStream = null;
                    this.socket = null;
                    this.CleanupCallback = null;
                }
                catch (Exception e)
                {
                    _logger.LogError(nameof(PooledSocket), e);
                }
            }
            else
            {
                Action<PooledSocket> cc = this.CleanupCallback;

                if (cc != null)
                    cc(this);
            }
        }

        void IDisposable.Dispose()
        {
            this.Dispose(false);
        }

        private void CheckDisposed()
        {
            if (this.socket == null)
                throw new ObjectDisposedException("PooledSocket");
        }

        /// <summary>
        /// Reads the next byte from the server's response.
        /// </summary>
        /// <remarks>This method blocks and will not return until the value is read.</remarks>
        public int ReadByte()
        {
            this.CheckDisposed();

            try
            {
                return this.inputStream.ReadByte();
            }
            catch (IOException)
            {
                this.isAlive = false;

                throw;
            }
        }

        public async Task<byte[]> ReadBytesAsync(int count)
        {
            this.CheckDisposed();

            if (!this.IsAlive)
                throw new InvalidOperationException("Socket is not alive");

            if (UseModernSocket)
            {
                // Modern .NET 6.0 native socket API path
                var buffer = new byte[count];
                int totalRead = 0;

                while (totalRead < count)
                {
                    try
                    {
                        var memory = new Memory<byte>(buffer, totalRead, count - totalRead);
                        var bytesRead = await this.socket.ReceiveAsync(memory, SocketFlags.None, CancellationToken.None)
                            .ConfigureAwait(false);

                        if (bytesRead == 0)
                        {
                            this.isAlive = false;
                            throw new IOException("Socket connection closed unexpectedly");
                        }

                        totalRead += bytesRead;
                    }
                    catch (SocketException ex)
                    {
                        this.isAlive = false;
                        throw new IOException($"Socket error: {ex.SocketErrorCode}", ex);
                    }
                }

                return buffer;
            }
            else
            {
                // Legacy SocketAwaitable path
                using (var awaitable = new SocketAwaitable())
                {
                    awaitable.Buffer = new ArraySegment<byte>(new byte[count], 0, count);
                    await this.socket.ReceiveAsync(awaitable);
                    return awaitable.Transferred.Array;
                }
            }
        }

        /// <summary>
        /// Reads data from the server into the specified buffer.
        /// </summary>
        /// <param name="buffer">An array of <see cref="T:System.Byte"/> that is the storage location for the received data.</param>
        /// <param name="offset">The location in buffer to store the received data.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <remarks>This method blocks and will not return until the specified amount of bytes are read.</remarks>
        public void Read(byte[] buffer, int offset, int count)
        {
            this.CheckDisposed();

            int read = 0;
            int shouldRead = count;

            while (read < count)
            {
                try
                {
                    int currentRead = this.inputStream.Read(buffer, offset, shouldRead);
                    if (currentRead < 1)
                        continue;

                    read += currentRead;
                    offset += currentRead;
                    shouldRead -= currentRead;
                }
                catch (IOException)
                {
                    this.isAlive = false;
                    throw;
                }
            }
        }

        public void Write(byte[] data, int offset, int length)
        {
            this.CheckDisposed();

            SocketError status;

            this.socket.Send(data, offset, length, SocketFlags.None, out status);

            if (status != SocketError.Success)
            {
                this.isAlive = false;

                ThrowHelper.ThrowSocketWriteError(this.endpoint, status);
            }
        }

        public void Write(IList<ArraySegment<byte>> buffers)
        {
            this.CheckDisposed();

            SocketError status;

#if DEBUG
            int total = 0;
            for (int i = 0, C = buffers.Count; i < C; i++)
                total += buffers[i].Count;

            if (this.socket.Send(buffers, SocketFlags.None, out status) != total)
                System.Diagnostics.Debugger.Break();
#else
            this.socket.Send(buffers, SocketFlags.None, out status);
#endif

            if (status != SocketError.Success)
            {
                this.isAlive = false;

                ThrowHelper.ThrowSocketWriteError(this.endpoint, status);
            }
        }

        /// <summary>
        /// Writes data to the socket asynchronously using modern .NET 6.0 native APIs or legacy SocketAwaitable.
        /// </summary>
        public async Task WriteAsync(IList<ArraySegment<byte>> buffers, CancellationToken cancellationToken = default)
        {
            this.CheckDisposed();

            if (!this.IsAlive)
                throw new InvalidOperationException("Socket is not alive");

            if (UseModernSocket)
            {
                // Modern .NET 6.0 native socket API path
                try
                {
                    // Convert ArraySegment<byte> list to Memory<byte> segments and send each
                    foreach (var segment in buffers)
                    {
                        if (segment.Array != null && segment.Count > 0)
                        {
                            var memory = new Memory<byte>(segment.Array, segment.Offset, segment.Count);
                            int totalSent = 0;

                            while (totalSent < memory.Length)
                            {
                                var remaining = memory.Slice(totalSent);
                                var bytesSent = await this.socket.SendAsync(remaining, SocketFlags.None, cancellationToken)
                                    .ConfigureAwait(false);

                                if (bytesSent == 0)
                                {
                                    this.isAlive = false;
                                    ThrowHelper.ThrowSocketWriteError(this.endpoint, SocketError.ConnectionReset);
                                }

                                totalSent += bytesSent;
                            }
                        }
                    }
                }
                catch (SocketException ex)
                {
                    this.isAlive = false;
                    ThrowHelper.ThrowSocketWriteError(this.endpoint, ex.SocketErrorCode);
                }
                catch (OperationCanceledException)
                {
                    this.isAlive = false;
                    ThrowHelper.ThrowSocketWriteError(this.endpoint, SocketError.TimedOut);
                }
            }
            else
            {
                // Legacy SocketAwaitable path
                using (var awaitable = new SocketAwaitable())
                {
                    awaitable.Arguments.BufferList = buffers;
                    try
                    {
                        await this.socket.SendAsync(awaitable);
                    }
                    catch
                    {
                        this.isAlive = false;
                        ThrowHelper.ThrowSocketWriteError(this.endpoint, awaitable.Arguments.SocketError);
                    }

                    if (awaitable.Arguments.SocketError != SocketError.Success)
                    {
                        this.isAlive = false;
                        ThrowHelper.ThrowSocketWriteError(this.endpoint, awaitable.Arguments.SocketError);
                    }
                }
            }
        }

        /// <summary>
        /// Legacy method name - use WriteAsync instead.
        /// </summary>
        [Obsolete("Use WriteAsync instead")]
        public async Task WriteSync(IList<ArraySegment<byte>> buffers)
        {
            await WriteAsync(buffers).ConfigureAwait(false);
        }

        /// <summary>
        /// Receives data asynchronously. Returns true if the IO is pending. Returns false if the socket already failed or the data was available in the buffer.
        /// p.Next will only be called if the call completes asynchronously.
        /// </summary>
        public bool ReceiveAsync(AsyncIOArgs p)
        {
            this.CheckDisposed();

            if (!this.IsAlive)
            {
                p.Fail = true;
                p.Result = null;

                return false;
            }

            if (this.helper == null)
                this.helper = new AsyncSocketHelper(this);

            return this.helper.Read(p);
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
