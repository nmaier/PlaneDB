#if NET48
using System;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
  internal sealed class NotNullWhenAttribute : Attribute
  {
    [UsedImplicitly]
    internal bool ReturnValue { get; }

    internal NotNullWhenAttribute(bool returnValue)
    {
      ReturnValue = returnValue;
    }
  }
}
#endif