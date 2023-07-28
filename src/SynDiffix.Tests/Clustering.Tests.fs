module SynDiffix.ClusteringTests

open Xunit
open FsUnit.Xunit

open SynDiffix.Forest
open SynDiffix.Microdata
open SynDiffix.Clustering

module DependenceTests =
  [<Fact>]
  let ``Dependence measurement`` () =
    let path = getTestFilePath "dependency.csv"
    let rows, columns = Csv.readWithTypes path Csv.rowNumberAid
    let rows = rows |> Seq.toArray

    let _, columnTypes = columns |> List.map (fun column -> column.Name, column.Type) |> List.unzip

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

    // Data fills only 50% of the boxes.
    result.Dependence |> should equal 0.5
