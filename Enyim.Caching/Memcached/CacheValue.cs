using System;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Wraps a cached value with metadata such as whether it's marked as critical.
    /// Uses a struct to minimize memory footprint.
    /// </summary>
    /// <typeparam name="T">The type of the cached value.</typeparam>
    public struct CacheValue<T>
    {
        private readonly T _value;
        private readonly bool _isCritical;

        /// <summary>
        /// Initializes a new instance of CacheValue.
        /// </summary>
        /// <param name="value">The cached value.</param>
        /// <param name="isCritical">Whether the value is marked as critical.</param>
        public CacheValue(T value, bool isCritical)
        {
            _value = value;
            _isCritical = isCritical;
        }

        /// <summary>
        /// Gets the cached value.
        /// </summary>
        public readonly T Value => _value;

        /// <summary>
        /// Gets a value indicating whether this cached item is marked as critical.
        /// </summary>
        public readonly bool IsCritical => _isCritical;

        /// <summary>
        /// Implicitly converts CacheValue to its underlying value.
        /// </summary>
        public static implicit operator T(CacheValue<T> cacheValue)
        {
            return cacheValue._value;
        }

        /// <summary>
        /// Implicitly converts a value to CacheValue (non-critical).
        /// </summary>
        public static implicit operator CacheValue<T>(T value)
        {
            return new CacheValue<T>(value, false);
        }

        /// <summary>
        /// Returns a string representation of the cache value.
        /// </summary>
        public override readonly string ToString()
        {
            return _value?.ToString() ?? string.Empty;
        }

        /// <summary>
        /// Determines whether the specified object is equal to this instance.
        /// </summary>
        public override readonly bool Equals(object obj)
        {
            if (obj is CacheValue<T> other)
            {
                return Equals(_value, other._value) && _isCritical == other._isCritical;
            }
            return false;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        public override int GetHashCode()
        {
            return HashCode.Combine(_value, _isCritical);
        }
    }
}

