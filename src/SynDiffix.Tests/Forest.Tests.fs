module SynDiffix.ForestTests

open Xunit
open FsUnit.Xunit
open FsUnit.CustomMatchers

open SynDiffix
open Range
open Forest
open Microdata
open Counter

let createConvertors count =
  Array.create count (IntegerConvertor() :> IDataConvertor)

[<Fact>]
let ``Root ranges are anonymized`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 0; Integer 1 |]
      [| List [ Integer 2 ]; Integer 0; Integer 5 |]
      [| List [ Integer 3 ]; Integer 0; Integer 2 |]
      [| List [ Integer 4 ]; Integer 0; Integer 7 |]
      [| List [ Integer 5 ]; Integer 0; Integer 21 |]
      [| List [ Integer 6 ]; Integer 0; Integer 4 |]
      [| List [ Integer 7 ]; Integer 0; Integer 21 |]
      [| List [ Integer 8 ]; Integer 0; Integer 28 |]
      [| List [ Integer 9 ]; Integer 0; Integer 19 |]
      [| List [ Integer 10 ]; Integer 0; Integer 2 |]
      [| List [ Integer 11 ]; Integer 1; Integer 1 |]
      [| List [ Integer 12 ]; Integer 1; Integer 13 |]
      [| List [ Integer 13 ]; Integer 1; Integer 25 |]
      [| List [ Integer 14 ]; Integer 1; Integer 30 |]
      [| List [ Integer 15 ]; Integer 1; Integer 6 |]
      [| List [ Integer 16 ]; Integer 1; Integer 2 |]
      [| List [ Integer 17 ]; Integer 1; Integer 15 |]
      [| List [ Integer 18 ]; Integer 1; Integer 24 |]
      [| List [ Integer 19 ]; Integer 1; Integer 9 |]
      [| List [ Integer 20 ]; Integer 0; Integer 100 |]
      [| List [ Integer 21 ]; Integer -5; Integer 0 |]
    |]

  let forest =
    Forest(
      rows,
      (createConvertors 2),
      defaultAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )

  (Tree.nodeData forest.TopTree).SnappedRanges
  |> should equal [| { Min = 0.0; Max = 2.0 }; { Min = 0.0; Max = 32.0 } |]

[<Fact>]
let ``Multiple rows per AID`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 1 |]
      [| List [ Integer 2 ]; Integer 1 |]
      [| List [ Integer 3 ]; Integer 1 |]
      [| List [ Integer 4 ]; Integer 1 |]
      [| List [ Integer 5 ]; Integer 1 |]
      [| List [ Integer 6 ]; Integer 1 |]
      [| List [ Integer 7 ]; Integer 1 |]
      [| List [ Integer 8 ]; Integer 1 |]
      [| List [ Integer 9 ]; Integer 1 |]
      [| List [ Integer 10 ]; Integer 0 |]
      [| List [ Integer 11 ]; Integer 0 |]
      [| List [ Integer 12 ]; Integer 0 |]
      [| List [ Integer 13 ]; Integer 0 |]
      [| List [ Integer 14 ]; Integer 0 |]
      [| List [ Integer 15 ]; Integer 0 |]
      [| List [ Integer 16 ]; Integer 0 |]
      [| List [ Integer 17 ]; Integer 0 |]
    |]

  let forest =
    Forest(
      rows,
      (createConvertors 1),
      defaultAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )

  // Sanity check, there's enough AIDs to branch at least once.
  forest.TopTree |> should be (ofCase (<@ Tree.Branch @>))

  let rowsAid =
    rows
    |> Array.map (fun row -> row |> Array.tail |> Array.insertAt 0 (List [ Integer 1L ]))

  let countStrategy =
    {
      CreateEntityCounter = fun () -> GenericAidEntityCounter(1, 100)
      CreateRowCounter = fun () -> GenericAidRowCounter(1)
    }

  let forestAid =
    Forest(
      rowsAid,
      (createConvertors 1),
      defaultAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      countStrategy
    )

  forestAid.TopTree |> should be (ofCase (<@ Tree.Leaf @>))

  match forestAid.TopTree with
  | Tree.Leaf l ->
    let entityCounter = l.Data.EntityCounter :?> GenericAidEntityCounter
    // There's a single AID contributing all rows.
    entityCounter.Counts.[0] |> should equal 1L
  | _ -> ()

[<Fact>]
let ``Outliers are not dropped from 1-dim trees - 1`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 1 |]
      [| List [ Integer 2 ]; Integer 5 |]
      [| List [ Integer 3 ]; Integer 2 |]
      [| List [ Integer 4 ]; Integer 7 |]
      [| List [ Integer 5 ]; Integer 21 |]
      [| List [ Integer 6 ]; Integer 4 |]
      [| List [ Integer 7 ]; Integer 21 |]
      [| List [ Integer 8 ]; Integer 28 |]
      [| List [ Integer 9 ]; Integer 19 |]
      [| List [ Integer 10 ]; Integer 2 |]
      [| List [ Integer 11 ]; Integer 1 |]
      [| List [ Integer 12 ]; Integer 13 |]
      [| List [ Integer 13 ]; Integer 25 |]
      [| List [ Integer 14 ]; Integer 30 |]
      [| List [ Integer 15 ]; Integer 6 |]
      [| List [ Integer 16 ]; Integer 2 |]
      [| List [ Integer 17 ]; Integer 15 |]
      [| List [ Integer 18 ]; Integer 24 |]
      [| List [ Integer 19 ]; Integer 9 |]
      [| List [ Integer 20 ]; Integer 100 |]
      [| List [ Integer 21 ]; Integer 0 |]
    |]

  let forest =
    Forest(
      rows,
      (createConvertors 1),
      noiselessAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )

  forest.TopTree |> Tree.noisyRowCount |> should equal 21L

[<Fact>]
let ``Outliers are not dropped from 1-dim trees - 2`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 1 |]
      [| List [ Integer 2 ]; Integer 1 |]
      [| List [ Integer 3 ]; Integer 1 |]
      [| List [ Integer 4 ]; Integer 1 |]
      [| List [ Integer 5 ]; Integer 1 |]
      [| List [ Integer 6 ]; Integer 1 |]
      [| List [ Integer 7 ]; Integer 1 |]
      [| List [ Integer 8 ]; Integer 1 |]
      [| List [ Integer 9 ]; Integer 100 |]
    |]

  let forest =
    Forest(
      rows,
      (createConvertors 1),
      noiselessAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )

  forest.TopTree |> Tree.noisyRowCount |> should equal 9L

  (Tree.nodeData forest.TopTree).ActualRanges
  |> Tree.isSingularityNode
  |> should equal true

// Context which never allows for splitting even at the root.
let private precisionLimitBucketizationParams =
  { defaultBucketizationParams with
      PrecisionLimitDepthThreshold = -1
      PrecisionLimitRowFraction = 1
  }

[<Fact>]
let ``Depth is limited for 1-dim trees`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 1 |]
      [| List [ Integer 2 ]; Integer 2 |]
      [| List [ Integer 3 ]; Integer 3 |]
      [| List [ Integer 4 ]; Integer 4 |]
      [| List [ Integer 5 ]; Integer 5 |]
      [| List [ Integer 6 ]; Integer 6 |]
      [| List [ Integer 7 ]; Integer 7 |]
      [| List [ Integer 8 ]; Integer 8 |]
      [| List [ Integer 9 ]; Integer 9 |]
    |]

  // Sanity check - default params don't limit depth.
  Forest(
    rows,
    (createConvertors 1),
    noiselessAnonContext,
    defaultBucketizationParams,
    testColumnNames,
    uniqueAidCountStrategy
  )
    .TopTree
  |> should be (ofCase (<@ Tree.Branch @>))

  Forest(
    rows,
    (createConvertors 1),
    noiselessAnonContext,
    precisionLimitBucketizationParams,
    testColumnNames,
    uniqueAidCountStrategy
  )
    .TopTree
  |> should be (ofCase (<@ Tree.Leaf @>))

[<Fact>]
let ``Depth is not limited for 2-dim trees`` () =
  let rows =
    [|
      [| List [ Integer 1 ]; Integer 1; Integer 1 |]
      [| List [ Integer 2 ]; Integer 2; Integer 2 |]
      [| List [ Integer 3 ]; Integer 3; Integer 3 |]
      [| List [ Integer 4 ]; Integer 4; Integer 4 |]
      [| List [ Integer 5 ]; Integer 5; Integer 5 |]
      [| List [ Integer 6 ]; Integer 6; Integer 6 |]
      [| List [ Integer 7 ]; Integer 7; Integer 7 |]
      [| List [ Integer 8 ]; Integer 8; Integer 8 |]
      [| List [ Integer 9 ]; Integer 9; Integer 9 |]
    |]

  Forest(
    rows,
    (createConvertors 2),
    noiselessAnonContext,
    precisionLimitBucketizationParams,
    testColumnNames,
    uniqueAidCountStrategy
  )
    .TopTree
  |> should be (ofCase (<@ Tree.Branch @>))


[<Fact>]
let ``Handling of NULL AIDs`` () =
  let rows =
    [|
      [| List [ Null ]; Integer 1 |]
      [| List [ Null ]; Integer 5 |]
      [| List [ String "" ]; Integer 2 |]
      [| List [ String "0" ]; Integer 2 |]
    |]

  let forest =
    Forest(
      rows,
      (createConvertors 1),
      noiselessAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )

  forest.TopTree
  |> Tree.nodeData
  |> Tree.dataCrossesLowThreshold 2
  |> should equal false
