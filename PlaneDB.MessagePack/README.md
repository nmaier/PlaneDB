# ![Icon](https://github.com/nmaier/PlaneDB/raw/master/icon.png) PlaneDB.MessagePack

A PlaneDB MessagePack Serializer

![.NET CI Status](https://github.com/nmaier/PlaneDB/workflows/.NET%20CI/badge.svg)
![Nuget](https://img.shields.io/nuget/v/NMaier.PlaneDB)
![GitHub](https://img.shields.io/github/license/nmaier/PlaneDB)

## Features

- General purpose structured data serializer using neuecc's fantastic `MessagePack`.
- Best suited for serializing data structures to PlaneDB values in an efficient and fast way.
- Can be used with PlaneDB keys as well, however please note that there is no official guarantee that the output produced will be stable across versions of `MessagePack`. Therefore it is not recommended to use this serializer for keys.

## Example

```c#
using System;
using System.IO;

using MessagePack;

using NMaier.PlaneDB;

using var db = new TypedPlaneDB<string, DataObject>(
  new PlaneStringSerializer(),
  new PlaneMessagePackSerializer<DataObject>(),
  new DirectoryInfo("."), new PlaneDBOptions());
db.SetValue("test", new DataObject { Bar = 1, Baz = true, Foo = "test" });
if (db.TryGetValue("test", out var obj)) {
  Console.WriteLine($"OK: {obj}");
}
else {
  Console.Error.WriteLine("Not OK");
}


[MessagePackObject]
public class DataObject
{
  [Key(0)]
  public long Bar;

  [Key(1)]
  public bool Baz;

  [Key(2)]
  public string Foo = string.Empty;

  public override string ToString()
  {
    return $"{Foo} -> {Bar} -> {Baz}";
  }
}
```

## Status

Beta-grade software.
I have been dogfooding the code for a while in various internal applications of mine (none business-critical), with millions of records, along with running the unit test suite a lot.
