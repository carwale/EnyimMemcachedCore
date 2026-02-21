using System;
using System.Collections.Generic;

namespace HashRingResolver
{
    /// <summary>
    /// Ketama-style consistent hash ring that maps cache keys to memcached server addresses.
    /// Matches Enyim.Caching DefaultNodeLocator behavior when server addresses use "host:port" format.
    /// </summary>
    public sealed class HashRingResolver
    {
        private readonly ulong[] _keys;
        private readonly Dictionary<ulong, string> _servers;

        /// <summary>
        /// Builds the hash ring from the given server addresses (format: "host:port", e.g. "192.168.1.1:11211").
        /// </summary>
        /// <param name="serverAddresses">List of memcached server addresses in host:port form.</param>
        /// <param name="serverAddressMutations">Number of points per server on the ring (default 1000, must match client).</param>
        public HashRingResolver(IReadOnlyList<string> serverAddresses, int serverAddressMutations = 1000)
        {
            if (serverAddresses == null || serverAddresses.Count == 0)
            {
                throw new ArgumentException("At least one server address is required.", nameof(serverAddresses));
            }

            var keys = new ulong[serverAddresses.Count * serverAddressMutations];
            _servers = new Dictionary<ulong, string>();

            int nodeIdx = 0;
            foreach (string address in serverAddresses)
            {
                if (string.IsNullOrWhiteSpace(address))
                {
                    throw new ArgumentException("Server address cannot be null or empty.", nameof(serverAddresses));
                }

                var tmpKeys = GenerateKeys(address, serverAddressMutations);
                for (var i = 0; i < tmpKeys.Length; i++)
                {
                    _servers[tmpKeys[i]] = address;
                }

                tmpKeys.CopyTo(keys, nodeIdx);
                nodeIdx += serverAddressMutations;
            }

            Array.Sort(keys);
            _keys = keys;
        }

        /// <summary>
        /// Returns the memcached server address (host:port) that owns the given key.
        /// </summary>
        /// <param name="key">The cache key.</param>
        /// <returns>The server address string, or null if the ring is empty.</returns>
        public string GetServerForKey(string key)
        {
            ArgumentNullException.ThrowIfNull(key);

            if (_keys.Length == 0)
            {
                return null;
            }

            ulong itemKeyHash = MurmurHash3.Hash(key);
            int foundIndex = Array.BinarySearch(_keys, itemKeyHash);

            if (foundIndex < 0)
            {
                foundIndex = ~foundIndex;

                if (foundIndex == 0)
                {
                    foundIndex = _keys.Length - 1;
                }
                else if (foundIndex >= _keys.Length)
                {
                    foundIndex = 0;
                }
            }

            if (foundIndex < 0 || foundIndex >= _keys.Length)
            {
                return null;
            }

            return _servers[_keys[foundIndex]];
        }

        private static ulong[] GenerateKeys(string address, int numberOfKeys)
        {
            const int KeyLength = 4;
            const int PartCount = 1;

            var k = new ulong[PartCount * numberOfKeys];

            for (int i = 0; i < numberOfKeys; i++)
            {
                var data = MurmurHash3.Hash(string.Concat(address, "-", i));

                for (int h = 0; h < PartCount; h++)
                {
                    k[i * PartCount + h] = BitConverter.ToUInt64(BitConverter.GetBytes(data), h * KeyLength);
                }
            }

            return k;
        }
    }
}
