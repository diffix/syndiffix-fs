module SynDiffix.ProgramTests

open Xunit
open FsUnit.Xunit

open System

let private loadData csvPath =
  Csv.readWithTypes csvPath Csv.rowNumberAid |> mapFst Seq.toArray

let private getStdDev avg values =
  let variance =
    (values |> Array.fold (fun acc value -> acc + (value - avg) ** 2.0) 0.0)
    / (float values.Length)

  Math.Sqrt variance

let private getColumnStats index (rows: Row array) =
  let values =
    rows
    |> Array.map (fun row ->
      match row.[index] with
      | Integer i -> float i
      | Real r -> r
      | Timestamp t -> (t - TIMESTAMP_REFERENCE).TotalSeconds
      | _ -> failwith "Unsupported value type in test!"
    )

  let avg = Array.average values
  let sd = getStdDev avg values
  avg, sd


let listTestFiles =
  IO.Directory.GetFiles(dataDir, "test*.csv")
  |> Array.map (IO.Path.GetFileName >> Array.singleton)

[<Theory>]
[<MemberData(nameof (listTestFiles))>]
let ``System test`` file =
  let originalData, columns = loadData (getTestFilePath file)
  let syntheticData = processData columns originalData

  for i = 0 to columns.Length - 1 do
    match columns.[i].Type with
    | StringType ->
      let originalSet = originalData |> Array.map (fun row -> row.[i + 1]) |> Set.ofArray
      let syntheticSet = syntheticData |> Array.map (fun row -> row.[i]) |> Set.ofArray

      let common = (Set.intersect originalSet syntheticSet)
      let different = (Set.difference syntheticSet originalSet)
      // Some values are common, some are filtered and generalized to `prefix*123`.
      common |> Set.isEmpty |> should equal false
      different |> Set.isEmpty |> should equal false
    | _ ->
      let originalAvg, originalSD = originalData |> Array.map Array.tail |> getColumnStats i
      let syntheticAvg, syntheticSD = getColumnStats i syntheticData

      syntheticAvg |> should be (equalWithin originalAvg originalSD)
      syntheticSD |> should be (equalWithin originalSD (originalSD / 2.0))

[<Fact>]
let ``Null handling`` =
  let originalRows =
    [|
      [| Integer 0; Integer 1; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 5; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 2; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 7; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 21; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 4; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 21; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 28; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 19; Value.Boolean false; Integer -1 |]
      [| Integer 0; Integer 2; Value.Boolean false; Integer -1 |]
      [| Integer 1; Integer 1; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 13; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 25; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 30; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 6; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 2; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 15; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 24; Value.Boolean false; Integer -2 |]
      [| Integer 1; Integer 9; Value.Boolean false; Integer -2 |]
      [| Value.Null; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
      [| Integer 2; Value.Null; Value.Null; Value.Null |]
    |]

  let columns =
    [ IntegerType; IntegerType; BooleanType; IntegerType ]
    |> List.map (fun t -> { Name = ""; Type = t })

  let syntheticData =
    originalRows
    |> Array.mapi (fun i row -> row |> Array.insertAt 0 (List [ Integer i ]))
    |> processData columns

  syntheticData |> Array.map (Array.item 0) |> should not' (contain Value.Null)

  for i = 1 to 3 do
    syntheticData |> Array.map (Array.item i) |> should contain Value.Null

[<Fact>]
let ``Handling of string ranges`` () =
  let originalRows, columns = loadData (getTestFilePath "string-ranges.csv")

  let syntheticRows =
    processDataWithParams noiselessAnonContext defaultBucketizationParams columns originalRows uniqueAidCountStrategy

  let getStringPrefix =
    function
    | String string -> string.Substring(0, string.LastIndexOf(' '))
    | _ -> failwith "Unexpected value received!"

  let originalPrefixCount =
    originalRows
    |> Array.map (Array.item 1 >> getStringPrefix)
    |> Array.distinct
    |> Array.length

  let syntheticValuesCount = syntheticRows |> Array.map (Array.item 0) |> Array.distinct |> Array.length

  syntheticValuesCount |> should be (greaterThan originalPrefixCount)
