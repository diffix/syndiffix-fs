[<AutoOpen>]
module SynDiffix.TestHelpers

open System

open SynDiffix
open Csv
open Forest
open Bucket
open Microdata
open Counter

// Many column names to ensure there's enough for all test cases to keep things tidy.
let testColumnNames = seq { for i in 1..100 -> $"col{i}" } |> List.ofSeq

let defaultAnonContext =
  {
    BucketSeed = 0UL
    AnonymizationParams =
      { AnonymizationParams.Default with
          Suppression = { LowThreshold = 2; LayerSD = 1.; LowMeanGap = 2. }
      }
  }

let defaultBucketizationParams =
  { BucketizationParams.Default with
      SingularityLowThreshold = 4
      RangeLowThreshold = 8
      ClusteringEnabled = false
  }

let noiselessAnonContext =
  { defaultAnonContext with
      AnonymizationParams =
        { defaultAnonContext.AnonymizationParams with
            LayerNoiseSD = 0.
            Suppression = { LowThreshold = 3; LayerSD = 0.; LowMeanGap = 0. }
            OutlierCount = { Lower = 1; Upper = 1 }
            TopCount = { Lower = 1; Upper = 1 }
        }
  }

let makeTimestamp (year, month, day) (hour, minute, second) =
  Timestamp(DateTime(year, month, day, hour, minute, second))

let dataDir = IO.Path.Join(__SOURCE_DIRECTORY__, "..", "..", "data")
let getTestFilePath file = IO.Path.Join(dataDir, file)

let uniqueAidCountStrategy =
  {
    CreateEntityCounter = fun (_) -> UniqueAidCounter()
    CreateRowCounter = fun () -> UniqueAidCounter()
  }

let processDataWithParams anonContext bucketizationParams columns rows countStrategy =
  let columnNames, columnTypes =
    columns |> List.map (fun column -> column.Name, column.Type) |> List.unzip

  let dataConvertors = createDataConvertors columnTypes rows
  let forest = Forest(rows, dataConvertors, anonContext, bucketizationParams, columnNames, countStrategy)

  forest.TopTree
  |> harvestBuckets
  |> generateMicrodata forest.DataConvertors forest.NullMappings
  |> Seq.map Array.tail // Drop the dummy AID instances field.
  |> Seq.toArray

let processData columns rows =
  processDataWithParams defaultAnonContext defaultBucketizationParams columns rows uniqueAidCountStrategy

module Csv =
  // Returns a list of types for columns which define the type in the header using `:`.
  let private getColumnTypes filePath =
    use csv = openCsv filePath

    csv.HeaderRecord
    |> Array.map (fun h -> h.Trim().Trim('\''))
    |> Array.choose (fun colName ->
      columnTypeFromName colName
      |> Option.map (fun colType -> { Name = colName; Type = colType })
    )
    |> Array.toList

  let readWithTypes (filePath: string) (aidColumns: string list) : CsvReadResult =
    read filePath (getColumnTypes filePath) aidColumns
