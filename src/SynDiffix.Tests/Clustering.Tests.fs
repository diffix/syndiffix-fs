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

    let result = forest |> Dependence.measureDependence 0 1
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
    result.Dependence |> should equal 0.5
