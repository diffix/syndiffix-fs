module SynDiffix.ClusteringTests

open Xunit
open FsUnit.Xunit

open SynDiffix.Forest
open SynDiffix.Microdata
open SynDiffix.Clustering

module DependenceTests =

  let measureDependence file =
    let path = getTestFilePath file
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
    result.Dependence

  [<Fact>]
  let ``Dependence measurements`` () =
    for file in [ "dependency_tau.csv"; "dependency_chi2.csv"; "dependency_anova.csv" ] do
      file |> measureDependence |> should be (greaterThan 0.8)
