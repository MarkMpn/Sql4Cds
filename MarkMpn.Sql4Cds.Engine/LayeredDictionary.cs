using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// A dictionary that combines multiple dictionaries into a single view
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    class LayeredDictionary<TKey,TValue> : IDictionary<TKey,TValue>
    {
        private readonly IDictionary<TKey, TValue>[] _inner;
        private readonly IDictionary<TKey, TValue> _fallback;

        public LayeredDictionary(params IDictionary<TKey, TValue>[] inner)
        {
            _inner = inner;
            _fallback = inner.Last();
        }

        public IDictionary<TKey, TValue>[] Inner => _inner;

        public TValue this[TKey key]
        {
            get
            {
                foreach (var dict in _inner)
                {
                    if (dict.TryGetValue(key, out var value))
                        return value;
                }

                throw new KeyNotFoundException();
            }
            set
            {
                foreach (var dict in _inner)
                {
                    if (dict == _fallback || dict.ContainsKey(key))
                    {
                        dict[key] = value;
                        return;
                    }
                }
            }
        }

        public ICollection<TKey> Keys => _inner.SelectMany(d => d.Keys).ToArray();

        public ICollection<TValue> Values => _inner.SelectMany(d => d.Values).ToArray();

        public int Count => _inner.Sum(d => d.Count);

        public bool IsReadOnly => _inner.Any(d => d.IsReadOnly);

        public void Add(TKey key, TValue value)
        {
            _fallback.Add(key, value);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            _fallback.Add(item);
        }

        public void Clear()
        {
            foreach (var dict in _inner)
                dict.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _inner.Any(d => d.Contains(item));
        }

        public bool ContainsKey(TKey key)
        {
            return _inner.Any(d => d.ContainsKey(key));
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            foreach (var dict in _inner)
            {
                dict.CopyTo(array, arrayIndex);
                arrayIndex += dict.Count;
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _inner.SelectMany(d => d).GetEnumerator();
        }

        public bool Remove(TKey key)
        {
            foreach (var dict in _inner)
            {
                if (dict.Remove(key))
                    return true;
            }

            return false;
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            foreach (var dict in _inner)
            {
                if (dict.Remove(item))
                    return true;
            }

            return false;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            foreach (var dict in _inner)
            {
                if (dict.TryGetValue(key, out value))
                    return true;
            }

            value = default;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    static class LayeredDictionaryExtensions
    {
        /// <summary>
        /// Removes a dictionary from a <see cref="LayeredDictionary{TKey, TValue}"/>, returning the new dictionary
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="dict"></param>
        /// <param name="remove"></param>
        /// <returns></returns>
        public static IDictionary<TKey, TValue> Unlayer<TKey,TValue>(this IDictionary<TKey, TValue> dict, IDictionary<TKey, TValue> remove)
        {
            if (!(dict is LayeredDictionary<TKey, TValue> layered))
            {
                if (dict == remove)
                    return null;

                return dict;
            }

            return new LayeredDictionary<TKey, TValue>(layered.Inner.Select(d => d.Unlayer(remove)).Where(d => d != null).ToArray());
        }
    }
}
