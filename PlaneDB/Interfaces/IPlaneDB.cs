using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <typeparam name="TKey">DB key type</typeparam>
  /// <typeparam name="TValue">DB value type</typeparam>
  /// <summary>
  ///   Kinda like LevelDB, but in C#!
  /// </summary>
  [PublicAPI]
  public interface IPlaneDB<TKey, TValue> : IPlaneBase, IDictionary<TKey, TValue>
    where TKey : notnull
  {
    /// <summary>
    ///   Iterate over the keys. Unlike the <see cref="IDictionary{TKey,TValue}.Keys">Keys</see> property, this wil not create
    ///   a materialized collection, and unlike <see cref="IEnumerable{T}.GetEnumerator" /> reading the actual values can be
    ///   skipped, too.
    /// </summary>
    IEnumerable<TKey> KeysIterator { get; }

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already
    ///   exist, or updates a key/value pair in the <see cref="IPlaneDB{TKey,TValue}" /> if the key
    ///   already exists.
    /// </summary>
    /// <param name="key">The key to be added or whose value should be updated</param>
    /// <param name="addValueFactory">The function used to generate a value for an absent key</param>
    /// <param name="updateValueFactory">
    ///   The function used to generate a new value for an existing key
    ///   based on the key's existing value
    /// </param>
    /// <returns>
    ///   The new value for the key.  This will be either be the result of addValueFactory (if the key was
    ///   absent) or the result of updateValueFactory (if the key was present).
    /// </returns>
    TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory,
      Func<TKey, TValue, TValue> updateValueFactory);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already
    ///   exist, or updates a key/value pair in the <see cref="IPlaneDB{TKey,TValue}" /> if the key
    ///   already exists.
    /// </summary>
    /// <param name="key">The key to be added or whose value should be updated</param>
    /// <param name="addValue">The value for an absent key</param>
    /// <param name="updateValueFactory">
    ///   The function used to generate a new value for an existing key
    ///   based on the key's existing value
    /// </param>
    /// <returns>
    ///   The new value for the key.  This will be either be addValue (if the key was
    ///   absent) or the result of updateValueFactory (if the key was present).
    /// </returns>
    TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already
    ///   exist, or updates a key/value pair in the <see cref="IPlaneDB{TKey,TValue}" /> if the key
    ///   already exists.
    /// </summary>
    /// <param name="key">The key to be added or whose value should be updated</param>
    /// <param name="addValueFactory">The function used to generate a value for an absent key</param>
    /// <param name="updateValueFactory">
    ///   The function used to generate a new value for an existing key
    ///   based on the key's existing value
    /// </param>
    /// <param name="factoryArgument">
    ///   An argument to pass into <paramref name="addValueFactory" /> and
    ///   <paramref name="updateValueFactory" />.
    /// </param>
    /// <returns>
    ///   The new value for the key.  This will be either be the result of addValueFactory (if the key was
    ///   absent) or the result of updateValueFactory (if the key was present).
    /// </returns>
    TValue AddOrUpdate<TArg>(TKey key, Func<TKey, TArg, TValue> addValueFactory,
      Func<TKey, TValue, TArg, TValue> updateValueFactory, TArg factoryArgument);

    /// <summary>
    ///   Copies the current data set to another dictionary.
    /// </summary>
    /// <param name="destination">Dictionary to which to copy</param>
    /// <remarks>The destination dictionary will be cleared!</remarks>
    void CopyTo(IDictionary<TKey, TValue> destination);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="valueFactory">The function used to generate a value for the key</param>
    /// <returns>
    ///   The value for the key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="value">The value for the key</param>
    /// <returns>
    ///   The value for the key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    TValue GetOrAdd(TKey key, TValue value);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="value">The value for the key</param>
    /// <param name="added">Indicates if value was added</param>
    /// <returns>
    ///   The value for the key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    TValue GetOrAdd(TKey key, TValue value, out bool added);

    /// <summary>
    ///   Adds a key/value pair to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="valueFactory">The function used to generate a value for the key</param>
    /// <param name="factoryArgument">
    ///   An argument to pass into <paramref name="valueFactory" />.
    /// </param>
    /// <returns>
    ///   The value for the key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument);

    /// <summary>
    ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise
    ///   returns the existing value.
    /// </summary>
    /// <param name="keysAndDefaults">
    ///   The keys of the elements to get, and a default value to add if the key does not exist
    ///   yet.
    /// </param>
    /// <returns>
    ///   The key-value pairs for each key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(IEnumerable<KeyValuePair<TKey, TValue>> keysAndDefaults);

    /// <summary>
    ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise
    ///   returns the existing value.
    /// </summary>
    /// <param name="keys">The keys of the elements to add.</param>
    /// <param name="value">The value for the keys</param>
    /// <returns>
    ///   The key-value pairs for each key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(IEnumerable<TKey> keys, TValue value);

    /// <summary>
    ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise
    ///   returns the existing value.
    /// </summary>
    /// <param name="keys">The keys of the elements to add.</param>
    /// <param name="valueFactory">
    ///   The function used to generate a value for the corresponding keys. This function may be
    ///   called more than once.
    /// </param>
    /// <returns>
    ///   The key-value pairs for each key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(IEnumerable<TKey> keys, Func<TKey, TValue> valueFactory);

    /// <summary>
    ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise
    ///   returns the existing value.
    /// </summary>
    /// <param name="keys">The keys of the elements to add.</param>
    /// <param name="valueFactory">
    ///   The function used to generate a value for the corresponding keys. This function may be
    ///   called more than once.
    /// </param>
    /// <param name="factoryArgument">
    ///   An argument to pass into <paramref name="valueFactory" />.
    /// </param>
    /// <returns>
    ///   The key-value pairs for each key. This will be either the existing value for the key if the
    ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
    ///   if the key was not in the dictionary.
    /// </returns>
    IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange<TArg>(IEnumerable<TKey> keys,
      Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument);

    /// <summary>
    ///   Raised when flushing memory tables
    /// </summary>
    event EventHandler<IPlaneDB<TKey, TValue>>? OnFlushMemoryTable;

    /// <summary>
    ///   Raised when merging on-disk tables
    /// </summary>
    event EventHandler<IPlaneDB<TKey, TValue>>? OnMergedTables;

    /// <summary>
    ///   Sets a value, overwriting any existing value.
    /// </summary>
    /// <param name="key">Key to set</param>
    /// <param name="value">Data for key</param>
    void Set(TKey key, TValue value);

    /// <summary>
    ///   Attempts to add the specified key and value to the <see cref="IPlaneDB{TKey,TValue}" />.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="value">
    ///   The value of the element to add. The value can be a null reference (Nothing
    ///   in Visual Basic) for reference types.
    /// </param>
    /// <returns>
    ///   true if the key/value pair was added to the <see cref="IPlaneDB{TKey,TValue}" />
    ///   successfully; otherwise, false.
    /// </returns>
    bool TryAdd(TKey key, TValue value);

    /// <summary>
    ///   Attempts to add the specified key and value to the <see cref="IPlaneDB{TKey,TValue}" />.
    /// </summary>
    /// <param name="key">The key of the element to add.</param>
    /// <param name="value">
    ///   The value of the element to add. The value can be a null reference (Nothing
    ///   in Visual Basic) for reference types.
    /// </param>
    /// <param name="existing">Existing value if any</param>
    /// <returns>
    ///   true if the key/value pair was added to the <see cref="IPlaneDB{TKey,TValue}" />
    ///   successfully; otherwise, false.
    /// </returns>
    bool TryAdd(TKey key, TValue value, out TValue existing);


    /// <summary>
    ///   Attempts to remove and return the the value with the specified key from the
    ///   <see cref="IPlaneDB{TKey, TValue}" />.
    /// </summary>
    /// <param name="key">The key of the element to remove and return.</param>
    /// <param name="value">
    ///   When this method returns, <paramref name="value" /> contains the object removed from the
    ///   <see cref="IPlaneDB{TKey,TValue}" /> or the default value of
    ///   <typeparamref
    ///     name="TValue" />
    ///   if the operation failed.
    /// </param>
    /// <returns>true if an object was removed successfully; otherwise, false.</returns>
    /// <exception cref="T:System.ArgumentNullException">
    ///   <paramref name="key" /> is a null reference
    ///   (Nothing in Visual Basic).
    /// </exception>
    bool TryRemove(TKey key, out TValue value);

    /// <summary>
    ///   Compares the existing value for the specified key with a specified value, and if they're equal,
    ///   updates the key with a third value.
    /// </summary>
    /// <param name="key">
    ///   The key whose value is compared with <paramref name="comparisonValue" /> and
    ///   possibly replaced.
    /// </param>
    /// <param name="newValue">
    ///   The value that replaces the value of the element with
    ///   <paramref
    ///     name="key" />
    ///   if the comparison results in equality.
    /// </param>
    /// <param name="comparisonValue">
    ///   The value that is compared to the value of the element with
    ///   <paramref name="key" />.
    /// </param>
    /// <returns>
    ///   true if the value with <paramref name="key" /> was equal to
    ///   <paramref
    ///     name="comparisonValue" />
    ///   and replaced with <paramref name="newValue" />; otherwise,
    ///   false.
    /// </returns>
    bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue);
  }
}