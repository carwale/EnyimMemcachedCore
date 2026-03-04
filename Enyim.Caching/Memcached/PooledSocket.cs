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
using System.ComponentModel;

namespace Enyim.Caching.Memcached
{
    [DebuggerDisplay("[ Address: {endpoint}, IsAlive = {IsAlive} ]")]
    public partial class PooledSocket : IDisposable
    {
        private readonly ILogger _logger;
        private readonly bool _enableTimeoutDiagnostics;

        private bool isAlive;
        private Socket socket;
        private EndPoint endpoint;

        private Stream inputStream;
        private AsyncSocketHelper helper;
        public DateTime LastConnectionTimestamp { get; set; }

        public PooledSocket(EndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout, ILogger logger, bool enableTimeoutDiagnostics = false)
        {
            _logger = logger;
            _enableTimeoutDiagnostics = enableTimeoutDiagnostics;

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
            ConnectWithTimeout(socket, endpoint, timeout);

            this.socket = socket;
            this.endpoint = endpoint;

            this.inputStream = new BasicNetworkStream(socket);            
        }

        private async void ConnectWithTimeout(Socket socket, EndPoint endpoint, int timeout)
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
            bool completedInTime = completed.WaitOne(timeout);

            if (!completedInTime)
            {
                RunTimeoutDiagnostics(endpoint);
                using (socket)
                {
                    throw new TimeoutException($"Connection timed out after {timeout}ms while connecting to {endpoint}");
                }
            }

            if (!socket.Connected)
            {
                var socketError = args.SocketError;
                using (socket)
                {
                    switch (socketError)
                    {
                        case SocketError.TimedOut:
                            throw new TimeoutException($"Connection timed out to {endpoint}");
                        case SocketError.ConnectionRefused:
                            throw new SocketException((int)SocketError.ConnectionRefused);
                        case SocketError.ConnectionReset:
                            throw new SocketException((int)SocketError.ConnectionReset);
                        default:
                            throw new SocketException((int)socketError);
                    }
                }
            }

            LastConnectionTimestamp = DateTime.UtcNow;
        }

        private void OnConnectCompleted(object sender, SocketAsyncEventArgs args)
        {
            LastConnectionTimestamp = DateTime.UtcNow;
            EventWaitHandle handle = (EventWaitHandle)args.UserToken;
            handle.Set();
        }

        private static void GetHostAndPort(EndPoint endpoint, out string host, out int port)
        {
            if (endpoint is IPEndPoint ip)
            {
                host = ip.Address.ToString();
                port = ip.Port;
                return;
            }
            if (endpoint is DnsEndPoint dns)
            {
                host = dns.Host;
                port = dns.Port;
                return;
            }
            host = endpoint?.ToString() ?? "?";
            port = 0;
        }

        private void RunTimeoutDiagnostics(EndPoint endpoint)
        {
            GetHostAndPort(endpoint, out string host, out int port);

            _logger.LogWarning("Connection timeout to {Endpoint}. Running diagnostics.", endpoint);

            try
            {
                RunInProcessDiagnostics(endpoint, host);
                if (_enableTimeoutDiagnostics)
                {
                    RunShellDiagnostics(host, port);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Timeout diagnostics failed.");
            }
        }

        private void RunInProcessDiagnostics(EndPoint endpoint, string host)
        {
            if (endpoint is DnsEndPoint)
            {
                try
                {
                    var addresses = Dns.GetHostAddresses(host);
                    var ipv4 = addresses.FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork);
                    _logger.LogWarning("[TimeoutDiagnostics] DNS resolve for {Host}: {Count} address(es), IPv4={IPv4}", host, addresses.Length, ipv4?.ToString() ?? "none");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[TimeoutDiagnostics] DNS resolve for {Host} failed.", host);
                }
            }
        }

        private void RunShellDiagnostics(string host, int port)
        {
            const int commandTimeoutMs = 5000;
            bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            RunShellCommand("ping", isWindows ? $"-n 3 {host}" : $"-c 3 {host}", commandTimeoutMs, "Ping");
            RunShellCommand(isWindows ? "powershell" : "nc", isWindows ? $"-Command \"Test-NetConnection -ComputerName {host} -Port {port} -WarningAction SilentlyContinue | Format-List\"" : $"-vz -w 2 {host} {port}", commandTimeoutMs, "PortCheck");
            RunShellCommand(isWindows ? "tracert" : "traceroute", isWindows ? $"-d -h 10 {host}" : $"-m 10 {host}", commandTimeoutMs, "Traceroute");
        }

        private void RunShellCommand(string fileName, string arguments, int timeoutMs, string label)
        {
            _logger.LogWarning("[TimeoutDiagnostics] {Label}: Running: {FileName} {Arguments}", label, fileName, arguments);
            try
            {
                var startInfo = new ProcessStartInfo
                {
                    FileName = fileName,
                    Arguments = arguments,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                using (var process = Process.Start(startInfo))
                {
                    if (process == null)
                    {
                        _logger.LogWarning("[TimeoutDiagnostics] {Label}: Could not start process.", label);
                        return;
                    }
                    var output = new StringBuilder();
                    var err = new StringBuilder();
                    process.OutputDataReceived += (_, e) => { if (e.Data != null) output.AppendLine(e.Data); };
                    process.ErrorDataReceived += (_, e) => { if (e.Data != null) err.AppendLine(e.Data); };
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();
                    if (!process.WaitForExit(timeoutMs))
                    {
                        try { process.Kill(); } catch { }
                        _logger.LogWarning("[TimeoutDiagnostics] {Label}: Timed out after {Ms}ms.", label, timeoutMs);
                        return;
                    }
                    if (output.Length > 0 || err.Length > 0)
                        _logger.LogWarning("[TimeoutDiagnostics] {Label}: ExitCode={ExitCode}. Stdout: {Stdout} Stderr: {Stderr}", label, process.ExitCode, output.ToString().Trim(), err.ToString().Trim());
                    else
                        _logger.LogWarning("[TimeoutDiagnostics] {Label}: ExitCode={ExitCode}.", label, process.ExitCode);
                }
            }
            catch (Win32Exception ex) when (ex.NativeErrorCode == 2)
            {
                _logger.LogWarning("[TimeoutDiagnostics] {Label}: Command not found ({FileName}).", label, fileName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[TimeoutDiagnostics] {Label}: Failed to run.", label);
            }
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

        /// <summary>
        /// Reads exactly <paramref name="count"/> bytes from the socket asynchronously.
        /// Loops until all requested bytes are received (matches sync Read behavior).
        /// </summary>
        public async Task<byte[]> ReadBytesAsync(int count)
        {
            this.CheckDisposed();

            var buffer = new byte[count];
            int totalRead = 0;

            using (var awaitable = new SocketAwaitable())
            {
                while (totalRead < count)
                {
                    int toRead = count - totalRead;
                    awaitable.Buffer = new ArraySegment<byte>(buffer, totalRead, toRead);

                    try
                    {
                        await this.socket.ReceiveAsync(awaitable);
                    }
                    catch (Exception)
                    {
                        this.isAlive = false;
                        throw;
                    }

                    if (awaitable.Arguments.SocketError != SocketError.Success)
                    {
                        this.isAlive = false;
                        throw new IOException(
                            string.Format("Failed to read from the socket '{0}'. Error: {1}",
                                this.endpoint,
                                awaitable.Arguments.SocketError));
                    }

                    int received = awaitable.Transferred.Count;
                    if (received <= 0)
                    {
                        this.isAlive = false;
                        throw new IOException("Connection closed or read returned 0.");
                    }

                    totalRead += received;
                }
            }

            return buffer;
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

        public async Task WriteAsync(IList<ArraySegment<byte>> buffers)
        {
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
