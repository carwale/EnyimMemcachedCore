namespace Enyim.Caching.Memcached;

public static class CacheFlagHelper
{
    public static bool IsCritical(uint flags)
    {
        return ((CacheFlags)flags & CacheFlags.IsCritical) != 0;
    }

    public static bool IsSerialized(uint flags){
        return ((CacheFlags)flags & CacheFlags.IsSerialized) != 0;
    }

    public static bool IsCompressed(uint flags){
        return ((CacheFlags)flags & CacheFlags.IsCompressed) != 0;
    }

    public static bool IsCritical(CacheFlags flags){
        return (flags & CacheFlags.IsCritical) != 0;
    }
}