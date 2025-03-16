using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Base dictionary type for PlaneDBs.
/// </summary>
/// <typeparam name="TKey">Dict key type</typeparam>
/// <typeparam name="TValue">Dict value type</typeparam>
[PublicAPI]
public interface IPlaneDictionary<TKey, TValue> : IDictionary<TKey, TValue>,
  IAsyncEnumerable<KeyValuePair<TKey, TValue>>, IDisposable where TKey : notnull
{
  /// <summary>
  ///   Factory for <see cref="IPlaneDictionary{TKey,TValue}.TryUpdate(TKey,TValue,TValue)" />
  /// </summary>
  /// <param name="key">Key to update</param>
  /// <param name="existing">Existing value</param>
  /// <param name="newValue">New Value</param>
  /// <returns>Value should be updated</returns>
  delegate bool TryUpdateFactory(
    in TKey key,
    in TValue existing,
    [MaybeNullWhen(false)] out TValue newValue);

  /// <summary>
  ///   Factory for
  ///   <see
  ///     cref="IPlaneDictionary{TKey,TValue}.TryUpdate{TArg}(TKey,IPlaneDictionary{TKey,TValue}.TryUpdateFactoryWithArg{TArg}, TArg)" />
  /// </summary>
  /// <param name="existing">Existing value</param>
  /// <param name="arg">Argument for the factory function</param>
  /// <param name="newValue">New Value</param>
  /// <returns>Value should be updated</returns>
  delegate bool TryUpdateFactoryWithArg<in TArg>(
    in TValue existing,
    TArg arg,
    [MaybeNullWhen(false)] out TValue newValue);

  /// <summary>
  ///   Factory function delegate for updates
  /// </summary>
  /// <param name="existingValue">Existing value associated with the key</param>
  /// <returns>New value for key</returns>
  delegate TValue UpdateValueFactory(in TValue existingValue);

  /// <summary>
  ///   Factory function delegate for updates
  /// </summary>
  /// <param name="existingValue">Existing value associated with the key</param>
  /// <param name="arg">Factory argument</param>
  /// <returns>New value for key</returns>
  delegate TValue UpdateValueFactoryWithArg<in TArg>(in TValue existingValue, TArg arg);

  /// <summary>
  ///   Factory function delegate for values
  /// </summary>
  /// <returns>Value for the key</returns>
  delegate TValue ValueFactory();

  /// <summary>
  ///   Factory function delegate for values
  /// </summary>
  /// <param name="arg">Factory argument</param>
  /// <returns>Value for the key</returns>
  delegate TValue ValueFactoryWithArg<in TArg>(TArg arg);

  /// <summary>
  ///   Factory function delegate for values
  /// </summary>
  /// <param name="key">Current key</param>
  /// <returns>Value for the key</returns>
  delegate TValue ValueFactoryWithKey(in TKey key);

  /// <summary>
  ///   Factory function delegate for values
  /// </summary>
  /// <param name="key">Current key</param>
  /// <param name="arg">Factory argument</param>
  /// <returns>Value for the key</returns>
  delegate TValue ValueFactoryWithKeyAndArg<in TArg>(in TKey key, TArg arg);

  /// <summary>
  ///   Iterate over the keys. Unlike the <see cref="IDictionary{TKey,TValue}.Keys">Keys</see> property, this wil not create
  ///   a materialized collection, and unlike <see cref="IEnumerable{T}.GetEnumerator" /> reading the actual values can be
  ///   skipped, too.
  /// </summary>
  [CollectionAccess(CollectionAccessType.Read)]
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  TValue AddOrUpdate(
    TKey key,
    [InstantHandle] ValueFactory addValueFactory,
    [InstantHandle] UpdateValueFactory updateValueFactory);

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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  TValue AddOrUpdate(
    TKey key,
    TValue addValue,
    [InstantHandle] UpdateValueFactory updateValueFactory);

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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  TValue AddOrUpdate<TArg>(
    TKey key,
    [InstantHandle] ValueFactoryWithArg<TArg> addValueFactory,
    [InstantHandle] UpdateValueFactoryWithArg<TArg> updateValueFactory,
    TArg factoryArgument);

  /// <summary>
  ///   Copies the current data set to another dictionary.
  /// </summary>
  /// <param name="destination">Dictionary to which to copy</param>
  /// <remarks>The destination dictionary will be cleared!</remarks>
  [CollectionAccess(CollectionAccessType.Read)]
  void CopyTo(IDictionary<TKey, TValue> destination);

  /// <summary>
  ///   Iterate over the keys, asynchronously. Unlike the <see cref="IDictionary{TKey,TValue}.Keys">Keys</see> property, this
  ///   wil not create
  ///   a materialized collection, and unlike <see cref="IEnumerable{T}.GetEnumerator" /> reading the actual values can be
  ///   skipped, too.
  /// </summary>
  [CollectionAccess(CollectionAccessType.Read)]
  IAsyncEnumerable<TKey> GetKeysIteratorAsync(CancellationToken token);

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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  TValue GetOrAdd(TKey key, [InstantHandle] ValueFactory valueFactory);

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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  TValue GetOrAdd<TArg>(
    TKey key,
    [InstantHandle] ValueFactoryWithArg<TArg> valueFactory,
    TArg factoryArgument);

  /// <summary>
  ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise,
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<KeyValuePair<TKey, TValue>> keysAndDefaults);

  /// <summary>
  ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise,
  ///   returns the existing value.
  /// </summary>
  /// <param name="keys">The keys of the elements to add.</param>
  /// <param name="value">The value for the keys</param>
  /// <returns>
  ///   The key-value pairs for each key. This will be either the existing value for the key if the
  ///   key is already in the dictionary, or the new value for the key as returned by valueFactory
  ///   if the key was not in the dictionary.
  /// </returns>
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    TValue value);

  /// <summary>
  ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise,
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    [InstantHandle] ValueFactoryWithKey valueFactory);

  /// <summary>
  ///   Add a range of elements to the <see cref="IPlaneDB{TKey,TValue}" /> if the key does not already exist. Otherwise,
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange<TArg>(
    IEnumerable<TKey> keys,
    [InstantHandle] ValueFactoryWithKeyAndArg<TArg> valueFactory,
    TArg factoryArgument);

  /// <summary>
  ///   Sets a value, overwriting any existing value.
  /// </summary>
  /// <param name="key">Key to set</param>
  /// <param name="value">Data for key</param>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  void SetValue(TKey key, TValue value);

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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
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
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  bool TryAdd(TKey key, TValue value, [MaybeNullWhen(true)] out TValue existing);

  /// <summary>
  ///   Attempts to add the specified key and value pairs to the <see cref="IPlaneDB{TKey,TValue}" />.
  /// </summary>
  /// <param name="pairs">Pairs to add</param>
  /// <returns>Number of pairs added successfully, number of pairs skipped</returns>
  [CollectionAccess(CollectionAccessType.Read | CollectionAccessType.UpdatedContent)]
  (long, long) TryAdd(IEnumerable<KeyValuePair<TKey, TValue>> pairs);

  /// <summary>
  ///   Attempts to remove and return the value with the specified key from the
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
  /// <exception cref="ArgumentNullException">
  ///   <paramref name="key" /> is a null reference
  ///   (Nothing in Visual Basic).
  /// </exception>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  bool TryRemove(TKey key, [MaybeNullWhen(false)] out TValue value);

  /// <summary>
  ///   Attempts to remove the value with the specified keys from the
  ///   <see cref="IPlaneDB{TKey, TValue}" />.
  /// </summary>
  /// <param name="keys">Keys to remove</param>
  /// <returns>Number of keys actually removed</returns>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  long TryRemove(IEnumerable<TKey> keys);

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
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue);

  /// <summary>
  ///   Try to update a value, if the key is already present in the db.
  /// </summary>
  /// <param name="key">Key to update</param>
  /// <param name="updateFactory">Update factory function</param>
  /// <returns>Key was updated</returns>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  bool TryUpdate(TKey key, [InstantHandle] TryUpdateFactory updateFactory);

  /// <summary>
  ///   Try to update a value, if the key is already present in the db.
  /// </summary>
  /// <param name="key">Key to update</param>
  /// <param name="updateFactory">Update factory function</param>
  /// <param name="arg"></param>
  /// <typeparam name="TArg"></typeparam>
  /// <returns></returns>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  bool TryUpdate<TArg>(
    TKey key,
    [InstantHandle] TryUpdateFactoryWithArg<TArg> updateFactory,
    TArg arg);
}
