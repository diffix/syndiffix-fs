module SynDiffix.ClusteringTests

open Xunit
open FsUnit.Xunit

open SynDiffix.Forest
open SynDiffix.Microdata
open SynDiffix.Clustering

module DependenceTests =
  let private snappedRange index node =
    (Tree.nodeData node).SnappedRanges[index]

  let private verifyRanges (scores: Dependence.Score list) =
    for score in scores do
      match score.NodeXY with
      | Some nodeXY ->
        let twoDimX = nodeXY |> snappedRange 0
        let twoDimY = nodeXY |> snappedRange 1
        let oneDimX = score.NodeX |> snappedRange 0
        let oneDimY = score.NodeY |> snappedRange 0

        // There are 3 options:
        //   - Both ranges match exactly.
        //   - X ranges match; Y is an inherited singularity superset.
        //   - Y ranges match; X is an inherited singularity superset.
        if twoDimX = oneDimX then oneDimY.Contains(twoDimY) |> should equal true
        elif twoDimY = oneDimY then oneDimX.Contains(twoDimX) |> should equal true
        else failwith "Score ranges don't match."
      | None -> ()

  let private verifyNodePairs (scores: Dependence.Score list) =
    let nodeId node = (Tree.nodeData node).Id

    // Each x,y pair should occur only once.
    scores
    |> List.groupBy (fun score -> nodeId score.NodeX, nodeId score.NodeY)
    |> List.iter (fun (_pair, group) -> group |> List.length |> should equal 1)

  [<Fact>]
  let ``Dependence measurement`` () =
    let path = getTestFilePath "dependency.csv"
    let rows, columns = Csv.readWithTypes path Csv.rowNumberAid
    let rows = rows |> Seq.toArray

    let columnNames, columnTypes =
      columns |> List.map (fun column -> column.Name, column.Type) |> List.unzip

    let dataConvertors = createDataConvertors columnTypes rows

    let forest =
      Forest(
        rows,
        dataConvertors,
        noiselessAnonContext,
        defaultBucketizationParams,
        testColumnNames,
        uniqueAidCountStrategy
      )

    let result = forest |> Dependence.measure 0 1
    result.Columns |> should equal (0, 1)
    verifyNodePairs result.Scores
    verifyRanges result.Scores

    // Data looks like this:
    // | * |   | * |   |
    // |   | * |   | * |
    // Dimension X has    1->2->4 nodes.
    // Dimension Y has    1->2    nodes.
    // Combination XY has 1->4    nodes.

    // Cartesian product of dimensions (X,Y) for each depth gives:
    // Depth 0: 1 x 1 = 1, has 1 matching XY node.
    // Depth 1: 2 x 2 = 4, has 4 matching XY nodes.
    // Depth 2: 4 x 2 = 8, dimension Y inherits singularity. No matching XY nodes.
    // Total evaluations: 13, 5 XY combinations.

    result.Scores |> List.length |> should equal 13

    result.Scores
    |> List.filter (fun s -> Option.isSome s.NodeXY)
    |> List.length
    |> should equal 5

    // Data fills only 50% of the boxes.
    result.DependenceYX |> should equal 0.5
    // X cannot be predicted from Y.
    result.DependenceXY |> should equal 0.0

module ClusteringTests =
  open Clustering

  let private dummyForest () =
    let columnTypes = [ IntegerType; IntegerType; IntegerType; IntegerType ]
    let rows = [| [| List [ Integer 0L ]; Integer 0L; Integer 0L; Integer 0L; Integer 0L |] |]

    Forest(
      rows,
      createDataConvertors columnTypes rows,
      noiselessAnonContext,
      defaultBucketizationParams,
      testColumnNames,
      uniqueAidCountStrategy
    )


  let private a, b, c, d = 0, 1, 2, 3

  // Fake base cluster results
  let combinationRows =
    Map.ofList
      [
        [ a; b ],
        [
          [| Integer 101; Integer 201 |]
          [| Integer 102; Integer 202 |]
          [| Integer 103; Integer 203 |]
          [| Integer 104; Integer 204 |]
          [| Integer 105; Integer 205 |]
        ]
        [ b; c ],
        [
          [| Integer 211; Integer 301 |]
          [| Integer 212; Integer 302 |]
          [| Integer 213; Integer 303 |]
          [| Integer 214; Integer 304 |]
          [| Integer 215; Integer 305 |]
          [| Integer 216; Integer 306 |]
        ]
        [ c; d ],
        [
          [| Integer 311; Integer 401 |]
          [| Integer 312; Integer 402 |]
          [| Integer 313; Integer 403 |]
          [| Integer 314; Integer 404 |]
          [| Integer 315; Integer 405 |]
          [| Integer 316; Integer 406 |]
          [| Integer 317; Integer 407 |]
          [| Integer 318; Integer 408 |]
        ]
      ]

  let private fakeTreeHarvest =
    fun _forest combination -> combinationRows.[Array.toList combination] :> Row seq

  [<Fact>]
  let ``Cluster stitching`` () =
    let forest = dummyForest ()

    // AB + (BC + CD)
    let resultRows, resultColumns =
      [
        CompositeCluster(
          [ b ],
          [ BaseCluster [ a; b ]; CompositeCluster([ c ], [ BaseCluster [ b; c ]; BaseCluster [ c; d ] ]) ]
        )
      ]
      |> buildTable fakeTreeHarvest forest

    resultColumns |> should equal [| 0; 1; 2; 3 |]

    // avg(5,avg(6,8)) = 6
    resultRows |> should haveLength 6

    resultRows
    |> should
         equal
         [| // Columns should (mostly) match on last digit.
           [| Integer 101L; Integer 201L; Integer 301L; Integer 401L |]
           [| Integer 102L; Integer 212L; Integer 312L; Integer 402L |]
           [| Integer 105L; Integer 215L; Integer 305L; Integer 405L |]
           [| Integer 103L; Integer 214L; Integer 314L; Integer 404L |]
           [| Integer 103L; Integer 203L; Integer 303L; Integer 403L |]
           [| Integer 104L; Integer 204L; Integer 316L; Integer 406L |]
         |]

  [<Fact>]
  let ``Cluster patching`` () =
    let forest = dummyForest ()
    // AB .. CD
    let resultRows, resultColumns =
      [ BaseCluster [ a; b ]; BaseCluster [ c; d ] ]
      |> buildTable fakeTreeHarvest forest

    resultColumns |> should equal [| 0; 1; 2; 3 |]

    // avg(5,8) = 6
    resultRows |> should haveLength 6

    resultRows
    |> should
         equal
         [| // AB and CD should match on last digit separately.
           [| Integer 104L; Integer 204L; Integer 312L; Integer 402L |]
           [| Integer 102L; Integer 202L; Integer 315L; Integer 405L |]
           [| Integer 101L; Integer 201L; Integer 311L; Integer 401L |]
           [| Integer 103L; Integer 203L; Integer 314L; Integer 404L |]
           [| Integer 104L; Integer 204L; Integer 313L; Integer 403L |]
           [| Integer 105L; Integer 205L; Integer 316L; Integer 406L |]
         |]
