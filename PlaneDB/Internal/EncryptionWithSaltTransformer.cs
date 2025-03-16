using System;
using System.Security.Cryptography;

using NMaier.BlockStream.Transformers;

namespace NMaier.PlaneDB;

internal sealed class EncryptionWithSaltTransformer(byte[] passphrase)
  : IPlaneSaltableBlockTransformer
{
  private byte[] currentSalt = [];
  private IBlockTransformer? currentTransformer;
  public bool MayChangeSize => throw new NotSupportedException();

  public ReadOnlySpan<byte> TransformBlock(ReadOnlySpan<byte> block)
  {
    throw new NotSupportedException();
  }

  public int UntransformBlock(ReadOnlySpan<byte> input, Span<byte> block)
  {
    throw new NotSupportedException();
  }

  public IBlockTransformer GetTransformerFor(ReadOnlySpan<byte> salt)
  {
    if (salt.SequenceEqual(currentSalt) && currentTransformer != null) {
      return currentTransformer;
    }

    lock (this) {
      if (salt.SequenceEqual(currentSalt) && currentTransformer != null) {
        return currentTransformer;
      }

      var gen = new Rfc2898DeriveBytes(
        passphrase,
        salt.ToArray(),
        200_000,
        HashAlgorithmName.SHA512);
      currentTransformer = new EncryptedCompressedTransformer(gen.GetBytes(64));
      currentSalt = salt.ToArray();

      return currentTransformer;
    }
  }
}
