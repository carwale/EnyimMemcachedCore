using System;

namespace HashRingResolver
{
    internal static class Program
    {
        // TODO: Add servers here
        private static readonly string[] Servers = {};

        static int Main(string[] args)
        {
            string key = ParseKeyFromArgs(args);
            if (key == null)
            {
                Console.Error.WriteLine("Usage: HashRingResolver <key>   or   HashRingResolver --key <key>");
                return 1;
            }

            try
            {
                var resolver = new HashRingResolver(Servers);
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

        private static string ParseKeyFromArgs(string[] args)
        {
            if (args == null || args.Length == 0)
                return null;

            if (args.Length >= 2 && string.Equals(args[0], "--key", StringComparison.OrdinalIgnoreCase))
                return args[1];

            return args[0];
        }
    }
}
