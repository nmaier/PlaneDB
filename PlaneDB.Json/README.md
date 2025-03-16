# ![Icon](https://github.com/nmaier/PlaneDB/raw/master/icon.png) PlaneDB.JSON

A PlaneDB JSON Serializer

![.NET CI Status](https://github.com/nmaier/PlaneDB/workflows/.NET%20CI/badge.svg)
![Nuget](https://img.shields.io/nuget/v/NMaier.PlaneDB)
![GitHub](https://img.shields.io/github/license/nmaier/PlaneDB)

## Features

- General purpose JSON serializer using `System.Text.JSON`.
- Best suited for serializing data structures to PlaneDB values without the need for additional third-party libraries.
- JSON Serialization is somewhat costly and "verbose", so you might be better suited by `PlaneDB.MessagePack`.
- Can be used with PlaneDB keys as well, however please note that there is no official guarantee that the output produced will be stable across versions of `System.Text.JSON`. Therefore it is not recommended to use this serializer for keys.

## Example

```c#
using System;
using System.IO;
using System.Text.Json.Serialization;

using NMaier.PlaneDB;

using var db = new TypedPlaneDB<string, DataObject>(
  new PlaneStringSerializer(),
  new PlaneJsonSerializer<DataObject>(),
  new DirectoryInfo("."),
  new PlaneDBOptions());
db.SetValue("test", new DataObject { Bar = 1, Baz = true, Foo = "test" });
if (db.TryGetValue("test", out var obj)) {
  Console.WriteLine($"OK: {obj}");
}
else {
  Console.Error.WriteLine("Not OK");
}


public class DataObject
{
  [JsonInclude]
  public long Bar;

  [JsonInclude]
  public bool Baz;

  [JsonInclude]
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
