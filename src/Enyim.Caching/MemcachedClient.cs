﻿using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;
using Enyim.Caching.Memcached.Results.Factories;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

#if NET6_0
using Enyim.Caching.Tracing;
# endif

#if NET6_0
using Enyim.Caching.Tracing;
# endif

namespace Enyim.Caching
{
    /// <summary>
    /// Memcached client.
    /// </summary>
    public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient, IDistributedCache
    {
        /// <summary>
        /// Represents a value which indicates that an item should never expire.
        /// </summary>
        public static readonly TimeSpan Infinite = TimeSpan.Zero;
        private ILogger<MemcachedClient> _logger;

        private IServerPool _pool;
        private IMemcachedKeyTransformer _keyTransformer;
        private ITranscoder _transcoder;

        public IStoreOperationResultFactory StoreOperationResultFactory { get; set; }
        public IGetOperationResultFactory GetOperationResultFactory { get; set; }
        public IMutateOperationResultFactory MutateOperationResultFactory { get; set; }
        public IConcatOperationResultFactory ConcatOperationResultFactory { get; set; }
        public IRemoveOperationResultFactory RemoveOperationResultFactory { get; set; }

        protected IServerPool Pool { get { return _pool; } }
        protected IMemcachedKeyTransformer KeyTransformer { get { return _keyTransformer; } }
        protected ITranscoder Transcoder { get { return _transcoder; } }

        public MemcachedClient(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<MemcachedClient>();

            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }
            _keyTransformer = configuration.CreateKeyTransformer() ?? new DefaultKeyTransformer();
            _transcoder = configuration.CreateTranscoder() ?? new DefaultTranscoder();

            _pool = configuration.CreatePool();
            StartPool();

            StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
            GetOperationResultFactory = new DefaultGetOperationResultFactory();
            MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
            ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
            RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();
        }

        [Obsolete]
        private MemcachedClient(IServerPool pool, IMemcachedKeyTransformer keyTransformer, ITranscoder transcoder)
        {
            if (pool == null) throw new ArgumentNullException("pool");
            if (keyTransformer == null) throw new ArgumentNullException("keyTransformer");
            if (transcoder == null) throw new ArgumentNullException("transcoder");

            _keyTransformer = keyTransformer;
            _transcoder = transcoder;

            _pool = pool;
            StartPool();
        }

        private void StartPool()
        {
            _pool.NodeFailed += (n) => { var f = NodeFailed; if (f != null) f(n); };
            _pool.Start();
        }

        public event Action<IMemcachedNode> NodeFailed;

        public bool Add(string key, object value)
        {
            return Store(StoreMode.Add, key, value);
        }

        public bool Add(string key, object value, int cacheSeconds)
        {
            return Store(StoreMode.Add, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Add(string key, object value, uint cacheSeconds)
        {
            return Store(StoreMode.Add, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Add(string key, object value, TimeSpan timeSpan)
        {
            return Store(StoreMode.Add, key, value, timeSpan);
        }

        public async Task<bool> AddAsync(string key, object value)
        {
            return await StoreAsync(StoreMode.Add, key, value).ConfigureAwait(false);
        }

        public async Task<bool> AddAsync(string key, object value, int cacheSeconds)
        {
            return await StoreAsync(StoreMode.Add, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> AddAsync(string key, object value, uint cacheSeconds)
        {
            return await StoreAsync(StoreMode.Add, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> AddAsync(string key, object value, TimeSpan timeSpan)
        {
            return await StoreAsync(StoreMode.Add, key, value, timeSpan).ConfigureAwait(false);
        }

        public bool Set(string key, object value, int cacheSeconds)
        {
            return Store(StoreMode.Set, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Set(string key, object value, uint cacheSeconds)
        {
            return Store(StoreMode.Set, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Set(string key, object value, TimeSpan timeSpan)
        {
            return Store(StoreMode.Set, key, value, timeSpan);
        }

        public async Task<bool> SetAsync(string key, object value, int cacheSeconds)
        {
            return await StoreAsync(StoreMode.Set, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> SetAsync(string key, object value, uint cacheSeconds)
        {
            return await StoreAsync(StoreMode.Set, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> SetAsync(string key, object value, TimeSpan timeSpan)
        {
            return await StoreAsync(StoreMode.Set, key, value, timeSpan).ConfigureAwait(false);
        }

        public bool Replace(string key, object value, int cacheSeconds)
        {
            return Store(StoreMode.Replace, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Replace(string key, object value, uint cacheSeconds)
        {
            return Store(StoreMode.Replace, key, value, TimeSpan.FromSeconds(cacheSeconds));
        }

        public bool Replace(string key, object value, TimeSpan timeSpan)
        {
            return Store(StoreMode.Replace, key, value, timeSpan);
        }

        public async Task<bool> ReplaceAsync(string key, object value, int cacheSeconds)
        {
            return await StoreAsync(StoreMode.Replace, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> ReplaceAsync(string key, object value, uint cacheSeconds)
        {
            return await StoreAsync(StoreMode.Replace, key, value, TimeSpan.FromSeconds(cacheSeconds)).ConfigureAwait(false);
        }

        public async Task<bool> ReplaceAsync(string key, object value, TimeSpan timeSpan)
        {
            return await StoreAsync(StoreMode.Replace, key, value, timeSpan).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <returns>The retrieved item, or <value>null</value> if the key was not found.</returns>
        public object Get(string key)
        {
            object tmp;

            return TryGet(key, out tmp) ? tmp : null;
        }

        /// <summary>
        /// Retrieves the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <returns>The retrieved item, or <value>default(T)</value> if the key was not found.</returns>
        public T Get<T>(string key)
        {
            var result = PerformGet<T>(key);
            return result.Success ? result.Value : default(T);
        }

        public IGetOperationResult<T> PerformGet<T>(string key)
        {
            if (!CreateGetCommand<T>(key, out var result, out var node, out var command))
            {
                return result;
            }

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformTryGet", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            try
            {
                var commandResult = node.Execute(command);
#if NET6_0
                activity.SetSuccess();
#endif
                return BuildGetCommandResult<T>(result, command, commandResult);

            }
            catch (Exception ex)
            {

                _logger.LogError(0, ex, $"{nameof(PerformGet)}(\"{key}\")");
#if NET6_0
                activity.SetException(ex);
#endif
                result.Fail(ex.Message);
                return result;
            }
        }

        private bool CreateGetCommand(string key, out IGetOperationResult result, out IMemcachedNode node, out IGetOperation command)
        {
            result = new DefaultGetOperationResultFactory().Create();
            var hashedKey = _keyTransformer.Transform(key);

            node = _pool.Locate(hashedKey);
            if (node == null)
            {
                var errorMessage = $"Unable to locate node with \"{key}\" key";
                _logger.LogError(errorMessage);
                result.Fail(errorMessage);
                command = null;
                return false;
            }

            command = _pool.OperationFactory.Get(hashedKey);
            return true;
        }

        private bool CreateGetCommand<T>(string key, out IGetOperationResult<T> result, out IMemcachedNode node, out IGetOperation command)
        {
            result = new DefaultGetOperationResultFactory<T>().Create();
            var hashedKey = _keyTransformer.Transform(key);

            node = _pool.Locate(hashedKey);
            if (node == null)
            {
                var errorMessage = $"Unable to locate node with \"{key}\" key";
                _logger.LogError(errorMessage);
                result.Fail(errorMessage);
                command = null;
                return false;
            }

            command = _pool.OperationFactory.Get(hashedKey);
            return true;
        }

        private IGetOperationResult BuildGetCommandResult(IGetOperationResult result, IGetOperation command, IOperationResult commandResult)
        {
            if (commandResult.Success)
            {
                var decompressedBytes = ZSTDCompression.Decompress(command.Result.Data, _logger);
                command.Result = new CacheItem(command.Result.Flags, decompressedBytes);
                result.Value = _transcoder.Deserialize(command.Result);
                result.Cas = command.CasValue;
                result.Pass();
            }
            else
            {
                commandResult.Combine(result);
            }

            return result;
        }

        private IGetOperationResult<T> BuildGetCommandResult<T>(IGetOperationResult<T> result, IGetOperation command, IOperationResult commandResult)
        {
            if (commandResult.Success)
            {
                var decompressedBytes = ZSTDCompression.Decompress(command.Result.Data, _logger);
                command.Result = new CacheItem(command.Result.Flags, decompressedBytes);
                result.Value = _transcoder.Deserialize<T>(command.Result);
                result.Cas = command.CasValue;
                result.Pass();
            }
            else
            {
                commandResult.Combine(result);
            }

            return result;
        }


        public async Task<IGetOperationResult> GetAsync(string key)
        {
            if (!CreateGetCommand(key, out var result, out var node, out var command))
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation($"Failed to CreateGetCommand for '{key}' key");
                }

                return result;
            }

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("GetAsync", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            try
            {
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);
#if NET6_0
                activity.SetSuccess();
#endif
                return BuildGetCommandResult(result, command, commandResult);
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, $"{nameof(GetAsync)}(\"{key}\")");
#if NET6_0
                activity.SetException(ex);
#endif
                result.Fail(ex.Message, ex);
                return result;
            }
        }

        public async Task<IGetOperationResult<T>> GetAsync<T>(string key)
        {
            if (!CreateGetCommand<T>(key, out var result, out var node, out var command))
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation($"Failed to CreateGetCommand for '{key}' key");
                }

                return result;
            }

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("GetAsync", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            try
            {
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false); ;
#if NET6_0
                activity.SetSuccess();
#endif
                return BuildGetCommandResult<T>(result, command, commandResult);
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, $"{nameof(GetAsync)}(\"{key}\")");
#if NET6_0
                activity.SetException(ex);
#endif
                result.Fail(ex.Message, ex);
                return result;
            }
        }

        public async Task<T> GetValueAsync<T>(string key)
        {
            var result = await GetAsync<T>(key).ConfigureAwait(false);
            return result.Success ? result.Value : default(T);
        }

        public async Task<T> GetValueOrCreateAsync<T>(string key, int cacheSeconds, Func<Task<T>> generator)
        {
            var result = await GetAsync<T>(key).ConfigureAwait(false);
            if (result.Success)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug($"Cache is hit. Key is '{key}'.");
                }

                return result.Value;
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug($"Cache is missed. Key is '{key}'.");
            }

            var value = await (generator?.Invoke()).ConfigureAwait(false);
            if (value != null)
            {
                try
                {
                    await AddAsync(key, value, cacheSeconds).ConfigureAwait(false);

                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug($"Added value into cache. Key is '{key}'. " + value);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"{nameof(AddAsync)}(\"{key}\", ..., {cacheSeconds})");
                    throw;
                }
            }
            return value;
        }

        /// <summary>
        /// Tries to get an item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <param name="value">The retrieved item or null if not found.</param>
        /// <returns>The <value>true</value> if the item was successfully retrieved.</returns>
        public bool TryGet(string key, out object value)
        {
            ulong cas = 0;

            return PerformTryGet(key, out cas, out value).Success;
        }

        /// <summary>
        /// Tries to get an item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <param name="value">The retrieved item or null if not found.</param>
        /// <returns>The <value>true</value> if the item was successfully retrieved.</returns>
        public bool TryGet<T>(string key, out T value)
        {
            ulong cas = 0;

            return PerformTryGet(key, out cas, out value).Success;
        }

        public CasResult<object> GetWithCas(string key)
        {
            return GetWithCas<object>(key);
        }

        public CasResult<T> GetWithCas<T>(string key)
        {
            return TryGetWithCas<T>(key, out var value)
                ? value
                : new CasResult<T> { Cas = value.Cas, Result = default };
        }

        public bool TryGetWithCas(string key, out CasResult<object> value)
        {
            var retval = PerformTryGet(key, out ulong cas, out object tmp);

            value = new CasResult<object> { Cas = cas, Result = tmp };

            return retval.Success;
        }

        public bool TryGetWithCas<T>(string key, out CasResult<T> value)
        {
            var retVal = PerformTryGet(key, out var cas, out T tmp);

            value = new CasResult<T> { Cas = cas, Result = tmp };

            return retVal.Success;
        }

        protected virtual IGetOperationResult PerformTryGet(string key, out ulong cas, out object value)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = GetOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformTryGet", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            _logger.LogInformation($"Inside PerformTryGet");

            cas = 0;
            value = null;

            if (node != null)
            {
                var command = _pool.OperationFactory.Get(hashedKey);
                var commandResult = node.Execute(command);

                if (commandResult.Success)
                {
                    var decompressedBytes = ZSTDCompression.Decompress(command.Result.Data, _logger);
                    command.Result = new CacheItem(command.Result.Flags, decompressedBytes);
                    result.Value = value = _transcoder.Deserialize(command.Result);
                    result.Cas = cas = command.CasValue;

#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }
                else
                {
#if NET6_0
                    activity.SetException(result.Exception);
#endif
                    commandResult.Combine(result);
                    return result;
                }
            }

            result.Value = value;
            result.Cas = cas;
#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            result.Fail("Unable to locate node");
            return result;
        }

        protected virtual IGetOperationResult PerformTryGet<T>(string key, out ulong cas, out T value)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = GetOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformTryGet", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            cas = 0;
            value = default;

            if (node != null)
            {
                var command = _pool.OperationFactory.Get(hashedKey);
                var commandResult = node.Execute(command);

                if (commandResult.Success)
                {
                    var decompressedBytes = ZSTDCompression.Decompress(command.Result.Data, _logger);
                    command.Result = new CacheItem(command.Result.Flags, decompressedBytes);
                    result.Value = value = _transcoder.Deserialize<T>(command.Result);
                    result.Cas = cas = command.CasValue;

#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }

#if NET6_0
                activity.SetException(result.Exception);
#endif
                commandResult.Combine(result);
                return result;
            }

            result.Value = value;
            result.Cas = cas;

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            result.Fail("Unable to locate node");
            return result;
        }


        #region [ Store                        ]

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value)
        {
            ulong tmp = 0;
            int status;

            return PerformStore(mode, key, value, 0, ref tmp, out status).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            ulong tmp = 0;
            int status;

            return PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor, null), ref tmp, out status).Success;
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value)
        {
            return (await PerformStoreAsync(mode, key, value, 0).ConfigureAwait(false)).Success;
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            return (await PerformStoreAsync(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt)).ConfigureAwait(false)).Success;
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return (await PerformStoreAsync(mode, key, value, MemcachedClient.GetExpiration(validFor, null)).ConfigureAwait(false)).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            ulong tmp = 0;
            int status;

            return PerformStore(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt), ref tmp, out status).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
        {
            var result = PerformStore(mode, key, value, 0, cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };

        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
        {
            var result = PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor, null), cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
        {
            var result = PerformStore(mode, key, value, MemcachedClient.GetExpiration(null, expiresAt), cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure. The text protocol does not support this operation, you need to Store then GetWithCas.</remarks>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value)
        {
            var result = PerformStore(mode, key, value, 0, 0);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        private IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ulong cas)
        {
            ulong tmp = cas;
            int status;

            var retval = PerformStore(mode, key, value, expires, ref tmp, out status);
            retval.StatusCode = status;

            if (retval.Success)
            {
                retval.Cas = tmp;
            }
            return retval;
        }

        protected virtual IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ref ulong cas, out int statusCode)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = StoreOperationResultFactory.Create();


#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformStore", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            statusCode = -1;

            //Removed null check on value parameter, in order to allow storing null

            if (node != null)
            {
                CacheItem item;

                try
                {
                    item = _transcoder.Serialize(value);
                    item.Data = ZSTDCompression.Compress(item.Data, _logger);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "PerformStore failed with '{key}' key", key);

                    result.Fail("PerformStore failed", ex);
                    return result;
                }

                var command = _pool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
                var commandResult = node.Execute(command);

                result.Cas = cas = command.CasValue;
                result.StatusCode = statusCode = command.StatusCode;

                if (commandResult.Success)
                {
#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }

#if NET6_0
                activity.SetException(result.Exception);
#endif
                commandResult.Combine(result);
                return result;
            }

            //if (performanceMonitor != null) performanceMonitor.Store(mode, 1, false);

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            result.Fail("Unable to locate node");
            return result;
        }

        protected async virtual Task<IStoreOperationResult> PerformStoreAsync(StoreMode mode, string key, object value, uint expires)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = StoreOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformStoreAsync", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            int statusCode = -1;
            ulong cas = 0;

            //Removed null check on value parameter, in order to allow storing null

            if (node != null)
            {
                CacheItem item;

                try
                {
                    item = _transcoder.Serialize(value);
                    item.Data = ZSTDCompression.Compress(item.Data, _logger);
                }
                catch (Exception e)
                {
                    _logger.LogError(new EventId(), e, $"{nameof(PerformStoreAsync)} for '{key}' key");

                    result.Fail("PerformStore failed", e);
                    return result;
                }

                var command = _pool.OperationFactory.Store(mode, hashedKey, item, expires, cas);
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

                result.Cas = cas = command.CasValue;
                result.StatusCode = statusCode = command.StatusCode;

                if (commandResult.Success)
                {
#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }

#if NET6_0
                activity.SetException(result.Exception);
#endif
                commandResult.Combine(result);
                return result;
            }

            //if (performanceMonitor != null) performanceMonitor.Store(mode, 1, false);

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            result.Fail("Unable to locate memcached node");
            return result;
        }

        #endregion
        #region [ Mutate                       ]

        #region [ Increment                    ]

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Increment(string key, ulong defaultValue, ulong delta)
        {
            return PerformMutate(MutationMode.Increment, key, defaultValue, delta, 0).Value;
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null)).Value;
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt)).Value;
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            var result = CasMutate(MutationMode.Increment, key, defaultValue, delta, 0, cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            var result = CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            var result = CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        #endregion
        #region [ Decrement                    ]
        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Decrement(string key, ulong defaultValue, ulong delta)
        {
            return PerformMutate(MutationMode.Decrement, key, defaultValue, delta, 0).Value;
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null)).Value;
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt)).Value;
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            var result = CasMutate(MutationMode.Decrement, key, defaultValue, delta, 0, cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            var result = CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor, null), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            var result = CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(null, expiresAt), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        #endregion

        #region Touch
        public async Task<IOperationResult> TouchAsync(string key, DateTime expiresAt)
        {
            return await PerformMutateAsync(MutationMode.Touch, key, 0, 0, GetExpiration(null, expiresAt)).ConfigureAwait(false);
        }

        public async Task<IOperationResult> TouchAsync(string key, TimeSpan validFor)
        {
            return await PerformMutateAsync(MutationMode.Touch, key, 0, 0, GetExpiration(validFor, null)).ConfigureAwait(false);
        }
        #endregion

        private IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires)
        {
            ulong tmp = 0;

            return PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
        }

        private IMutateOperationResult CasMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
        {
            var tmp = cas;
            var retval = PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
            retval.Cas = tmp;
            return retval;
        }

        protected virtual IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ref ulong cas)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = MutateOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformMutate", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            if (node != null)
            {
                var command = _pool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
                var commandResult = node.Execute(command);

                result.Cas = cas = command.CasValue;
                result.StatusCode = command.StatusCode;

                if (commandResult.Success)
                {
                    result.Value = command.Result;
#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }
                else
                {
#if NET6_0
                    activity.SetException(result.Exception);
#endif
                    result.InnerResult = commandResult;
                    result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
                }

            }

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            // TODO not sure about the return value when the command fails
            result.Fail("Unable to locate node");
            return result;
        }

        protected virtual async Task<IMutateOperationResult> PerformMutateAsync(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires)
        {
            ulong cas = 0;
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = MutateOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformMutateAsync", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            if (node != null)
            {
                var command = _pool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

                result.Cas = cas = command.CasValue;
                result.StatusCode = command.StatusCode;

                if (commandResult.Success)
                {
                    result.Value = command.Result;
#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                    return result;
                }
                else
                {
#if NET6_0
                    activity.SetException(result.Exception);
#endif
                    result.InnerResult = commandResult;
                    result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
                }

            }

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            // TODO not sure about the return value when the command fails
            result.Fail("Unable to locate node");
            return result;
        }


        #endregion
        #region [ Concatenate                  ]

        /// <summary>
        /// Appends the data to the end of the specified item's data on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="data">The data to be appended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public bool Append(string key, ArraySegment<byte> data)
        {
            ulong cas = 0;

            return PerformConcatenate(ConcatenationMode.Append, key, ref cas, data).Success;
        }

        /// <summary>
        /// Inserts the data before the specified item's data on the server.
        /// </summary>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public bool Prepend(string key, ArraySegment<byte> data)
        {
            ulong cas = 0;

            return PerformConcatenate(ConcatenationMode.Prepend, key, ref cas, data).Success;
        }

        /// <summary>
        /// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
        {
            ulong tmp = cas;
            var success = PerformConcatenate(ConcatenationMode.Append, key, ref tmp, data);

            return new CasResult<bool> { Cas = tmp, Result = success.Success };
        }

        /// <summary>
        /// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
        {
            ulong tmp = cas;
            var success = PerformConcatenate(ConcatenationMode.Prepend, key, ref tmp, data);

            return new CasResult<bool> { Cas = tmp, Result = success.Success };
        }

        protected virtual IConcatOperationResult PerformConcatenate(ConcatenationMode mode, string key, ref ulong cas, ArraySegment<byte> data)
        {
            var hashedKey = _keyTransformer.Transform(key);
            var node = _pool.Locate(hashedKey);
            var result = ConcatOperationResultFactory.Create();

#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformConcatenate", new[]
            {
                new KeyValuePair<string, object?>("net.peer.query.key", key),
                new KeyValuePair<string, object?>("net.peer.name", node.EndPoint),
                new KeyValuePair<string, object?>("net.peer.isActive", node.IsAlive)
            });
#endif

            if (node != null)
            {
                var command = _pool.OperationFactory.Concat(mode, hashedKey, cas, data);
                var commandResult = node.Execute(command);

                if (commandResult.Success)
                {
                    result.Cas = cas = command.CasValue;
                    result.StatusCode = command.StatusCode;
#if NET6_0
                    activity.SetSuccess();
#endif
                    result.Pass();
                }
                else
                {
#if NET6_0
                    activity.SetException(result.Exception);
#endif
                    result.InnerResult = commandResult;
                    result.Fail("Concat operation failed, see InnerResult or StatusCode for details");
                }

                return result;
            }

#if NET6_0
            activity.SetException(new Exception("Unable to locate node"));
#endif
            result.Fail("Unable to locate node");
            return result;
        }

        #endregion

        /// <summary>
        /// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
        /// </summary>
        public void FlushAll()
        {
#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("FlushALL");

#endif
            foreach (var node in _pool.GetWorkingNodes())
            {
                var command = _pool.OperationFactory.Flush();

                node.Execute(command);
            }
#if NET6_0
            activity.SetSuccess();
#endif
        }

        public async Task FlushAllAsync()
        {
#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("FlushAllAsync");
#endif
            var tasks = new List<Task>();

            foreach (var node in _pool.GetWorkingNodes())
            {
                var command = _pool.OperationFactory.Flush();

                tasks.Add(node.ExecuteAsync(command));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false); ;

#if NET6_0
            activity.SetSuccess();
#endif
        }

        /// <summary>
        /// Returns statistics about the servers.
        /// </summary>
        /// <returns></returns>
        public ServerStats Stats()
        {
            return Stats(null);
        }

        public ServerStats Stats(string type)
        {
#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("Stats");
#endif
            var results = new Dictionary<EndPoint, Dictionary<string, string>>();
            var tasks = new List<Task>();

            foreach (var node in _pool.GetWorkingNodes())
            {
                var cmd = _pool.OperationFactory.Stats(type);
                var action = new Func<IOperation, IOperationResult>(node.Execute);
                var endpoint = node.EndPoint;

                tasks.Add(Task.Run(() =>
                {
                    action(cmd);
                    lock (results)
                        results[endpoint] = cmd.Result;
                }));
            }

            if (tasks.Count > 0)
            {
                Task.WaitAll(tasks.ToArray());
            }

#if NET6_0
            activity.SetSuccess();
#endif

            return new ServerStats(results);
        }

        /// <summary>
        /// Removes the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to delete.</param>
        /// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
        public bool Remove(string key)
        {
            return ExecuteRemove(key).Success;
        }

        public async Task<bool> RemoveAsync(string key)
        {
            return (await ExecuteRemoveAsync(key).ConfigureAwait(false)).Success;
        }

        public async Task<bool> RemoveMultiAsync(params string[] keys)
        {
            if (keys.Length > 0)
            {
                var tasks = new Task<IRemoveOperationResult>[keys.Length];

                for (var i = 0; i < keys.Length; i++)
                {
                    tasks[i] = ExecuteRemoveAsync(keys[i]);
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);

                foreach (var task in tasks)
                {
                    if (!(await task.ConfigureAwait(false)).Success) return false;
                }

                return true;
            }

            return false;
        }

        /// <summary>
        /// Retrieves multiple items from the cache.
        /// </summary>
        /// <param name="keys">The list of identifiers for the items to retrieve.</param>
        /// <returns>a Dictionary holding all items indexed by their key.</returns>
        public IDictionary<string, T> Get<T>(IEnumerable<string> keys)
        {
            return PerformMultiGet<T>(keys, (mget, kvp) =>
            {
                var decompressedBytes = ZSTDCompression.Decompress(kvp.Value.Data, _logger);
                var decompressedCacheItem = new CacheItem(kvp.Value.Flags, decompressedBytes);
                return _transcoder.Deserialize<T>(decompressedCacheItem);
            });
        }

        public async Task<IDictionary<string, T>> GetAsync<T>(IEnumerable<string> keys)
        {
            return await PerformMultiGetAsync<T>(keys, (mget, kvp) =>
            {
                var decompressedBytes = ZSTDCompression.Decompress(kvp.Value.Data, _logger);
                var decompressedCacheItem = new CacheItem(kvp.Value.Flags, decompressedBytes);
                return _transcoder.Deserialize<T>(decompressedCacheItem);
            });
        }

        public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
        {
            return PerformMultiGet(keys, (mget, kvp) =>
            {
                var decompressedBytes = ZSTDCompression.Decompress(kvp.Value.Data, _logger);
                var decompressedCacheItem = new CacheItem(kvp.Value.Flags, decompressedBytes);
                return new CasResult<object>
                {
                    Result = _transcoder.Deserialize(decompressedCacheItem),
                    Cas = mget.Cas[kvp.Key]
                };
            });
        }

        public async Task<IDictionary<string, CasResult<object>>> GetWithCasAsync(IEnumerable<string> keys)
        {
            return await PerformMultiGetAsync(keys, (mget, kvp) =>
            {
                var decompressedBytes = ZSTDCompression.Decompress(kvp.Value.Data, _logger);
                var decompressedCacheItem = new CacheItem(kvp.Value.Flags, decompressedBytes);
                return new CasResult<object>
                {
                    Result = _transcoder.Deserialize(decompressedCacheItem),
                    Cas = mget.Cas[kvp.Key]
                };
            }).ConfigureAwait(false);
        }
        protected virtual IDictionary<string, T> PerformMultiGet<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
        {
            // transform the keys and index them by hashed => original
            // the mget results will be mapped using this index
#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformMultiGet");
#endif
            var hashed = new Dictionary<string, string>();
            foreach (var key in keys) hashed[_keyTransformer.Transform(key)] = key;

            var byServer = GroupByServer(hashed.Keys);

            var retval = new Dictionary<string, T>(hashed.Count);
            var tasks = new List<Task>();

            //execute each list of keys on their respective node
            foreach (var slice in byServer)
            {
                var node = slice.Key;

#if NET6_0
                activity.AddTagsForKeys(node, keys);
#endif

                var nodeKeys = slice.Value;
                var mget = _pool.OperationFactory.MultiGet(nodeKeys);

                // run gets in parallel
                var action = new Func<IOperation, IOperationResult>(node.Execute);

                //execute the mgets in parallel
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        if (action(mget).Success)
                        {
                            // deserialize the items in the dictionary
                            foreach (var kvp in mget.Result)
                            {
                                string original;
                                if (hashed.TryGetValue(kvp.Key, out original))
                                {
                                    var result = collector(mget, kvp);
                                    // the lock will serialize the merge,
                                    // but at least the commands were not waiting on each other
                                    lock (retval) retval[original] = result;
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
#if NET6_0
                        activity.SetException(e);
#endif
                        _logger.LogError(0, e, "PerformMultiGet");
                        throw;
                    }
                }));
            }

            // wait for all nodes to finish
            if (tasks.Count > 0)
            {
                Task.WaitAll(tasks.ToArray());
            }

#if NET6_0
            activity.SetSuccess();
#endif

            return retval;
        }

        protected virtual async Task<IDictionary<string, T>> PerformMultiGetAsync<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
        {
            // transform the keys and index them by hashed => original
            // the mget results will be mapped using this index
#if NET6_0
            using var activity = ActivitySourceHelper.StartActivity("PerformMultiGetAsync");
#endif
            var hashed = new Dictionary<string, string>();
            foreach (var key in keys)
            {
                hashed[_keyTransformer.Transform(key)] = key;
            }

            var byServer = GroupByServer(hashed.Keys);

            var retval = new Dictionary<string, T>(hashed.Count);
            var tasks = new List<Task>();

            //execute each list of keys on their respective node
            foreach (var slice in byServer)
            {
                var node = slice.Key;
#if NET6_0
                activity.AddTagsForKeys(node, keys);
#endif
                var nodeKeys = slice.Value;
                var mget = _pool.OperationFactory.MultiGet(nodeKeys);
                var task = Task.Run(async () =>
                {
                    if ((await node.ExecuteAsync(mget).ConfigureAwait(false)).Success)
                    {
                        foreach (var kvp in mget.Result)
                        {
                            if (hashed.TryGetValue(kvp.Key, out var original))
                            {
                                lock (retval) retval[original] = collector(mget, kvp);
                            }
                        }
                    }
                });
                tasks.Add(task);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false); ;
#if NET6_0
            activity.SetSuccess();
#endif
            return retval;
        }

        protected Dictionary<IMemcachedNode, IList<string>> GroupByServer(IEnumerable<string> keys)
        {
            var retval = new Dictionary<IMemcachedNode, IList<string>>();

            foreach (var k in keys)
            {
                var node = _pool.Locate(k);
                if (node == null) continue;

                IList<string> list;
                if (!retval.TryGetValue(node, out list))
                    retval[node] = list = new List<string>(4);

                list.Add(k);
            }

            return retval;
        }

        /// <summary>
        /// Waits for all WaitHandles and works in both STA and MTA mode.
        /// </summary>
        /// <param name="waitHandles"></param>
        private static void SafeWaitAllAndDispose(WaitHandle[] waitHandles)
        {
            try
            {
                //Not support .NET Core
                //if (Thread.CurrentThread.GetApartmentState() == ApartmentState.MTA)
                //    WaitHandle.WaitAll(waitHandles);
                //else
                for (var i = 0; i < waitHandles.Length; i++)
                    waitHandles[i].WaitOne();
            }
            finally
            {
                for (var i = 0; i < waitHandles.Length; i++)
                    waitHandles[i].Dispose();
            }
        }

        #region [ Expiration helper            ]

        protected const int MaxSeconds = 60 * 60 * 24 * 30;
        protected static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);

        protected static uint GetExpiration(
            TimeSpan? validFor,
            DateTime? expiresAt = null,
            DateTimeOffset? absoluteExpiration = null,
            TimeSpan? relativeToNow = null)
        {
            if (validFor != null && expiresAt != null)
                throw new ArgumentException("You cannot specify both validFor and expiresAt.");

            if (validFor == null && expiresAt == null && absoluteExpiration == null && relativeToNow == null)
            {
                return 0;
            }

            if (absoluteExpiration != null)
            {
                return (uint)absoluteExpiration.Value.ToUnixTimeSeconds();
            }

            if (relativeToNow != null)
            {
                return (uint)(DateTimeOffset.UtcNow + relativeToNow.Value).ToUnixTimeSeconds();
            }

            // convert timespans to absolute dates
            if (validFor != null)
            {
                // infinity
                if (validFor == TimeSpan.Zero || validFor == TimeSpan.MaxValue) return 0;

                if (validFor.Value.TotalSeconds <= MaxSeconds)
                {
                    return (uint)validFor.Value.TotalSeconds;
                }

                expiresAt = DateTime.Now.Add(validFor.Value);
            }

            DateTime dt = expiresAt.Value;

            if (dt < UnixEpoch) throw new ArgumentOutOfRangeException("expiresAt", "expiresAt must be >= 1970/1/1");

            // accept MaxValue as infinite
            if (dt == DateTime.MaxValue) return 0;

            uint retval = (uint)(dt.ToUniversalTime() - UnixEpoch).TotalSeconds;

            return retval;
        }

        #endregion
        #region [ IDisposable                  ]

        ~MemcachedClient()
        {
            try { ((IDisposable)this).Dispose(); }
            catch { }
        }

        void IDisposable.Dispose()
        {
            Dispose();
        }

        /// <summary>
        /// Releases all resources allocated by this instance
        /// </summary>
        /// <remarks>You should only call this when you are not using static instances of the client, so it can close all conections and release the sockets.</remarks>
        public void Dispose()
        {
            GC.SuppressFinalize(this);

            if (_pool != null)
            {
                try { _pool.Dispose(); }
                finally { _pool = null; }
            }
        }

        #endregion
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
