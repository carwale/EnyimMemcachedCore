using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// This is a ketama-like consistent hashing based node locator. Used when no other <see cref="T:IMemcachedNodeLocator"/> is specified for the pool.
    /// </summary>
    public sealed class DefaultNodeLocator : IMemcachedNodeLocator, IDisposable
    {

        // holds all server keys for mapping an item key to the server consistently
        private uint[] _keys;
        // used to lookup a server based on its key
        private Dictionary<uint, IMemcachedNode> _servers;
        private Dictionary<IMemcachedNode, bool> _deadServers;
        private List<IMemcachedNode> _allServers;
        private ReaderWriterLockSlim _serverAccessLock;

        private Func<string, ulong> _hash;

        private RendezvousHasher _hrw;

        public DefaultNodeLocator() : this(MurmurHash3.Hash)
        {
        }

        public DefaultNodeLocator(Func<string, ulong> hash)
        {
            _servers = new Dictionary<uint, IMemcachedNode>(new UIntEqualityComparer());
            _deadServers = new Dictionary<IMemcachedNode, bool>();
            _allServers = new List<IMemcachedNode>();
            _hash = hash;
            _serverAccessLock = new ReaderWriterLockSlim();
        }

        private void BuildIndex(List<IMemcachedNode> nodes)
        {
            // build the rendezvous hasher here
            _hrw = new RendezvousHasher(nodes, _hash);

        }

        void IMemcachedNodeLocator.Initialize(IList<IMemcachedNode> nodes)
        {
            _serverAccessLock.EnterWriteLock();

            try
            {
                _allServers = nodes.ToList();
                BuildIndex(_allServers);
            }
            finally
            {
                _serverAccessLock.ExitWriteLock();
            }
        }

        IMemcachedNode IMemcachedNodeLocator.Locate(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            _serverAccessLock.EnterUpgradeableReadLock();

            try { return Locate(key); }
            finally { _serverAccessLock.ExitUpgradeableReadLock(); }
        }

        IEnumerable<IMemcachedNode> IMemcachedNodeLocator.GetWorkingNodes()
        {
            _serverAccessLock.EnterReadLock();

            try { return _allServers.Except(_deadServers.Keys).ToArray(); }
            finally { _serverAccessLock.ExitReadLock(); }
        }

        private IMemcachedNode Locate(string key)
        {
            var node = FindNode(key);
            if (node == null || node.IsAlive)
                return node;

            // move the current node to the dead list and rebuild the indexes
            _serverAccessLock.EnterWriteLock();

            try
            {
                // check if it's still dead or it came back
                // while waiting for the write lock
                if (!node.IsAlive)
                    _deadServers[node] = true;

                BuildIndex(_allServers.Except(_deadServers.Keys).ToList());
            }
            finally
            {
                _serverAccessLock.ExitWriteLock();
            }

            // try again with the dead server removed from the lists
            return Locate(key);
        }

        public static ulong XorShiftMult64(ulong x)
        {
            // Combine operations for readability (C# allows for more relaxed expression formatting)
            return x ^ (x >> 12) ^ (x << 25) ^ (x >> 27) * 2685821657736338717UL;
        }

        /// <summary>
        /// locates a node by its key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private IMemcachedNode FindNode(string key)
        {
            if (_hrw.Nodes.Count == 0) return null;

            if (_hrw.Nodes.Count == 1) return _hrw.Nodes.First().Value;

            ulong keyHash = _hrw.Hash(key);

            int nodeId = 0;

            ulong maxHash = XorShiftMult64(keyHash ^ _hrw.Nhash[0]);

            for (int i = 1; i < _hrw.Nhash.Count; i++)
            {
                ulong hash = XorShiftMult64(keyHash ^ _hrw.Nhash[i]);
                if (hash > maxHash)
                {
                    nodeId = i;
                    maxHash = hash;
                }
            }

            var nodeIp = _hrw.Nstr[nodeId];

            if (!_hrw.Nodes.ContainsKey(nodeIp))
            {
                // if the node is not in the list, it's dead
                return null;
            }

            return _hrw.Nodes[nodeIp];
        }

        #region [ IDisposable                  ]

        void IDisposable.Dispose()
        {
            using (_serverAccessLock)
            {
                _serverAccessLock.EnterWriteLock();

                try
                {
                    // kill all pending operations (with an exception)
                    // it's not nice, but disposeing an instance while being used is bad practice
                    _allServers = null;
                    _servers = null;
                    _keys = null;
                    _deadServers = null;
                }
                finally
                {
                    _serverAccessLock.ExitWriteLock();
                }
            }

            _serverAccessLock = null;
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
