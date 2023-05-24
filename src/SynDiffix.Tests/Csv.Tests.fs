module SynDiffix.CsvTests

open Xunit
open FsUnit.Xunit
open System.IO

[<Fact>]
let ``Test column inference`` () =
  let path = getTestFilePath "column-types.csv"
  let rows, columns = Csv.readWithTypes path Csv.rowNumberAid

  columns
  |> should
       equal
       [
         { Name = "int:i"; Type = IntegerType }
         { Name = "real:r"; Type = RealType }
         { Name = "bool:b"; Type = BooleanType }
         { Name = "string:s"; Type = StringType }
         { Name = "time:t"; Type = TimestampType }
       ]

  rows
  |> Seq.head
  |> Array.tail
  |> should
       equal
       [| //
         Integer 1
         Real 2.5
         Boolean true
         String "a"
         makeTimestamp (2023, 1, 25) (12, 27, 30)
       |]

[<Fact>]
let ``Implicit AID column`` () =
  let path = getTestFilePath "aid-column-implicit.csv"
  let rows, columns = Csv.readWithTypes path Csv.rowNumberAid

  columns
  |> should equal [ { Name = "c0:i"; Type = IntegerType }; { Name = "c1:i"; Type = IntegerType } ]

  rows
  |> Seq.toList
  |> should
       equal
       [
         [| List [ Integer 1L ]; Integer 10L; Integer 20L |]
         [| List [ Integer 2L ]; Integer 30L; Integer 40L |]
       ]


[<Fact>]
let ``Explicit integer AID column - reinterpret as string`` () =
  let path = getTestFilePath "aid-column-explicit-int.csv"
  let rows, columns = Csv.readWithTypes path [ "aid" ]

  columns
  |> should equal [ { Name = "c0:i"; Type = IntegerType }; { Name = "c1:i"; Type = IntegerType } ]

  rows
  |> Seq.toList
  |> should
       equal
       [
         [| List [ String "101" ]; Integer 10L; Integer 20L |]
         [| List [ String "102" ]; Integer 30L; Integer 40L |]
       ]

[<Fact>]
let ``Explicit string AID column`` () =
  let path = getTestFilePath "aid-column-explicit-string.csv"
  let rows, columns = Csv.readWithTypes path [ "aid" ]

  columns
  |> should equal [ { Name = "c0:i"; Type = IntegerType }; { Name = "c1:i"; Type = IntegerType } ]

  rows
  |> Seq.toList
  |> should
       equal
       [
         [| List [ String "10one" ]; Integer 10L; Integer 20L |]
         [| List [ String "10two" ]; Integer 30L; Integer 40L |]
       ]

[<Fact>]
let ``Multiple AID columns per row`` () =
  let path = getTestFilePath "aid-column-multiple.csv"
  let rows, columns = Csv.readWithTypes path [ "aid1"; "aid2" ]

  columns
  |> should equal [ { Name = "c0:i"; Type = IntegerType }; { Name = "c1:i"; Type = IntegerType } ]

  rows
  |> Seq.toList
  |> should
       equal
       [
         [| List [ String "101"; String "201" ]; Integer 10L; Integer 20L |]
         [| List [ String "102"; String "202" ]; Integer 30L; Integer 40L |]
       ]

[<Fact>]
let ``Output back to CSV`` () =
  let path = getTestFilePath "column-types.csv"
  let rows, columns = Csv.readWithTypes path Csv.rowNumberAid
  let output = rows |> Seq.map Array.tail |> Seq.toList |> Csv.toString columns

  output |> should equal (File.ReadAllText path)
