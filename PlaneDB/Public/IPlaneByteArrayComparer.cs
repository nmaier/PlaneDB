using System.Collections.Generic;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Compare byte sequences
/// </summary>
[PublicAPI]
public interface IPlaneByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>;
