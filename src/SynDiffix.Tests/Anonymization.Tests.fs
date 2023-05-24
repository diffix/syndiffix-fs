module SynDiffix.AnonymizationTests

open Xunit
open FsUnit.Xunit

open SynDiffix
open Forest
open Counter

let private processSingleAid fileName =
  let csvPath = getTestFilePath fileName
  let rows, columns = Csv.readWithTypes csvPath [ "aid" ] |> mapFst Seq.toArray

  let countStrategy =
    {
      CreateEntityCounter = fun () -> GenericAidEntityCounter(1, 100)
      CreateRowCounter = fun () -> GenericAidRowCounter(1)
    }

  processDataWithParams noiselessAnonContext defaultBucketizationParams columns rows countStrategy

let private processMultipleAid fileName =
  let csvPath = getTestFilePath fileName

  let rows, columns = Csv.readWithTypes csvPath [ "aid1"; "aid2" ] |> mapFst Seq.toArray

  let countStrategy =
    {
      CreateEntityCounter = fun () -> GenericAidEntityCounter(2, 100)
      CreateRowCounter = fun () -> GenericAidRowCounter(2)
    }

  processDataWithParams noiselessAnonContext defaultBucketizationParams columns rows countStrategy

let private countOf value values =
  values |> Seq.where ((=) value) |> Seq.length

[<Fact>]
let ``Test flattening for single AID`` () =
  let anonRows = processSingleAid "flattening-single-aid.csv"
  let values = anonRows |> Array.map (Array.item 0)

  values
  |> Array.distinct
  |> Array.toList
  |> should matchList [ Integer 1L; Integer 2L ]

  let countOf value =
    values |> Array.where ((=) value) |> Array.length

  // Global contributions are [5=outlier] [3=top] 3 1 1 1 ...
  // A flattening of 2 occurs for the global count, while a combined flattening of 4 occurs for the local counts.
  // This means that the final counts adjusting step increases the local counts by 1, on average.

  // Left side has: 3 1 1 1
  countOf (Integer 1L) |> should lessThan 6
  // Right side has: 5 3 1 1 1 1 1
  countOf (Integer 2L) |> should lessThan 13

[<Fact>]
let ``Test flattening for multiple AIDs`` () =
  let anonRows = processMultipleAid "flattening-multi-aid.csv"
  let values = anonRows |> Array.map (Array.item 0)

  values
  |> Array.distinct
  |> Array.toList
  |> should matchList [ Integer 1L; Integer 2L ]

  // Global contributions are [5=outlier] [3=top] 1 1 1 ...
  // Left side has: 3 1 1 1
  values |> countOf (Integer 1L) |> should equal 4
  // Right side has: 5 1 1 1 1 1
  values |> countOf (Integer 2L) |> should equal 7

[<Fact>]
let ``Test low counts filter for single AID`` () =
  let anonRows = processSingleAid "lcf-single-aid.csv"
  let values = anonRows |> Array.map (Array.item 0)

  values |> should contain (String "show")
  values |> should not' (contain (String "suppress_1"))
  values |> should not' (contain (String "suppress_2"))

[<Fact>]
let ``Test low counts filter for multiple AIDs`` () =
  let anonRows = processMultipleAid "lcf-multiple-aid.csv"
  let values = anonRows |> Array.map (Array.item 0)

  values |> should contain (String "show")
  values |> should not' (contain (String "suppress_1"))
  values |> should not' (contain (String "suppress_2"))
  values |> should not' (contain (String "suppress_3"))
