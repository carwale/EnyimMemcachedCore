using System;
using System.Linq;

namespace HashRingResolver
{
    internal static class Program
    {
        private const string ServersOption = "--servers";
        private const string KeyOption = "--key";

        static int Main(string[] args)
        {
            if (!TryParseArgs(args, out string[] servers, out string key, out string error))
            {
                Console.Error.WriteLine(error);
                return 1;
            }

            try
            {
                var resolver = new HashRingResolver(servers);
                string server = resolver.GetServerForKey(key);
                if (server == null)
                {
                    Console.Error.WriteLine("No server found for key (empty ring).");
                    return 1;
                }

                Console.WriteLine(server);
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 1;
            }
        }

        private static bool TryParseArgs(string[] args, out string[] servers, out string key, out string error)
        {
            servers = null;
            key = null;
            error = null;

            if (args == null || args.Length == 0)
            {
                error = GetUsage();
                return false;
            }

            string serversArg = null;
            string keyArg = null;

            for (int i = 0; i < args.Length; i++)
            {
                if (string.Equals(args[i], ServersOption, StringComparison.OrdinalIgnoreCase))
                {
                    if (i + 1 >= args.Length)
                    {
                        error = $"Missing value for {ServersOption}. {GetUsage()}";
                        return false;
                    }
                    serversArg = args[++i];
                }
                else if (string.Equals(args[i], KeyOption, StringComparison.OrdinalIgnoreCase))
                {
                    if (i + 1 >= args.Length)
                    {
                        error = $"Missing value for {KeyOption}. {GetUsage()}";
                        return false;
                    }
                    keyArg = args[++i];
                }
            }

            if (string.IsNullOrWhiteSpace(serversArg))
            {
                error = $"Missing {ServersOption}. {GetUsage()}";
                return false;
            }

            servers = serversArg
                .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .Where(s => s.Length > 0)
                .ToArray();

            if (servers.Length == 0)
            {
                error = "At least one server address is required (format: host:port).";
                return false;
            }

            if (string.IsNullOrWhiteSpace(keyArg))
            {
                error = $"Missing {KeyOption}. {GetUsage()}";
                return false;
            }

            key = keyArg;
            return true;
        }

        private static string GetUsage() =>
            $"Usage: HashRingResolver --servers \"host1:11211,host2:11211\" --key \"mykey\"";
    }
}
