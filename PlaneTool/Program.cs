using System;
using System.IO;

using NMaier.GetOptNet;
using NMaier.PlaneDB;

var opts = new Options();

try {
  opts.Parse(args);

  return 0;
}
catch (GetOptException ex) {
  Console.Error.WriteLine($"Error: {ex.Message}");
  Console.Error.WriteLine();
  opts.PrintUsage();

  return 2;
}
catch (FileNotFoundException) {
  Console.Error.WriteLine("Error: Cannot open DB, the database does not exist");

  return 1;
}
catch (PlaneDBBadMagicException) {
  Console.Error.WriteLine("Error: Cannot open DB, wrong options");
  Console.Error.WriteLine(
    "Make sure you are using the correct compressed or pass-phrase switches");
  Console.Error.WriteLine();
  opts.PrintUsage();

  return 2;
}
catch (PlaneDBAlreadyLockedException) {
  Console.Error.WriteLine(
    "Error: Cannot open DB, the database is locked by another process");

  return 1;
}
catch (Exception ex) {
  Console.Error.WriteLine($"Error: {ex}");

  return 1;
}
