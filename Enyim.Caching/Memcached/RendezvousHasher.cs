using System;
using System.Net;
using System.Collections.Generic;
using Enyim.Caching.Memcached.Protocol;
using Enyim.Caching.Memcached.Results;
using System.Threading.Tasks;
using System.Linq;

namespace Enyim.Caching.Memcached
{
    public class RendezvousHasher
    {
        public Dictionary<string, IMemcachedNode> Nodes { get; set; }
        public List<string> Nstr { get; set; }
        public List<ulong> Nhash { get; set; }
        public Func<string, ulong> Hash { get; set; }
        public RendezvousHasher(List<IMemcachedNode> nodes, Func<string, ulong> hashFunction)
        {
            Nodes = new Dictionary<string, IMemcachedNode>();
            Nhash = new List<ulong>();
            Hash = hashFunction;
            Nstr = new List<string>();

            // Assuming you want to assign positions based on order in the list
            for (int i = 0; i < nodes.Count; i++)
            {
                Nstr.Add(nodes[i].EndPoint.ToString());
                Nodes.Add(nodes[i].EndPoint.ToString(), nodes[i]);
                Nhash.Add(Hash(nodes[i].EndPoint.ToString()));
            }
        }
    }
}