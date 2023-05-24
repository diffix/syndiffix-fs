module SynDiffix.Json

open Thoth.Json.Net

open SynDiffix.Combination
open SynDiffix.Range
open SynDiffix.Forest
open SynDiffix.Bucket

let rec private encodeValue =
  function
  | Null -> Encode.nil
  | Boolean bool -> Encode.bool bool
  | Integer int64 -> Encode.float (float int64)
  | Real float -> Encode.float float
  | String string -> Encode.string string
  | Timestamp ts -> Encode.datetime ts
  | List values -> values |> List.map encodeValue |> Encode.list

let private encodeValues values =
  values |> Array.map encodeValue |> Encode.array

let private encodeRange (range: Range) =
  [| range.Min; range.Max |] |> Array.map Encode.float |> Encode.array

let private encodeRanges (ranges: Ranges) =
  ranges |> Array.map encodeRange |> Encode.array

let private generateDecoder<'T> = Decode.Auto.generateDecoder<'T> CamelCase

let private extraRowsCoders =
  Extra.empty
  |> Extra.withUInt64
  |> Extra.withCustom encodeValue generateDecoder<Value>

let private rowsEncoder = Encode.Auto.generateEncoder<Tree.Row seq> (CamelCase, extraRowsCoders)

let private encodeNodeData (data: Tree.NodeData) =
  [
    "id", Encode.int data.Id
    "ranges", encodeRanges data.SnappedRanges
    "isStub", Encode.bool data.IsStub
  ]

let rec private encodeNode =
  function
  | Tree.Leaf leaf ->
    [
      "rows", rowsEncoder leaf.Rows
    //"subnodes", leaf.Data.Subnodes |> Array.map (Encode.option encodeNode) |> Encode.array
    ]
    |> List.append (encodeNodeData leaf.Data)
    |> Encode.object
  | Tree.Branch branch ->
    [
      "children",
      branch.Children
      |> Map.toList
      |> List.map (fun (index, node) -> sprintf "%d" index, encodeNode node)
      |> Encode.object
    //"subnodes", branch.Data.Subnodes |> Array.map (Encode.option encodeNode) |> Encode.array
    ]
    |> List.append (encodeNodeData branch.Data)
    |> Encode.object

let private encodeBucket bucket =
  Encode.object
    [
      "ranges", encodeRanges bucket.Ranges
      "count", Encode.int (int bucket.Count)
      "nodeId", Encode.int bucket.NodeId
    ]

let private encodeCombinations f values =
  values |> List.map (fun (c, v) -> toString c, f v) |> Encode.object

let private encodeAnonParams (anonParams: AnonymizationParams) =
  [
    "lcfLowThreshold", Encode.int anonParams.Suppression.LowThreshold
    "thresholdSD", anonParams.Suppression.LayerSD |> Encode.float
    "outlierCount", Encode.tuple2 Encode.int Encode.int (anonParams.OutlierCount.Lower, anonParams.OutlierCount.Upper)
    "topCount", Encode.tuple2 Encode.int Encode.int (anonParams.TopCount.Lower, anonParams.TopCount.Upper)
    "layerNoiseSD", anonParams.LayerNoiseSD |> Encode.float
  ]
  |> Encode.object

let private encodeBucketizationParams (bucketizationParams: BucketizationParams) =
  [
    "rangeLowThreshold", Encode.int bucketizationParams.RangeLowThreshold
    "singularityLowThreshold", Encode.int bucketizationParams.SingularityLowThreshold
    "depthThreshold", Encode.int bucketizationParams.PrecisionLimitDepthThreshold
    "rowFraction", Encode.int bucketizationParams.PrecisionLimitRowFraction
  ]
  |> Encode.object

let toString
  (columns: Columns)
  (nullMappings: float array)
  (anonParams: AnonymizationParams)
  (bucketizationParams: BucketizationParams)
  (forests: List<Combination * Tree>)
  (buckets: List<Combination * Buckets>)
  (synRows: Row seq)
  (originalRows: Row seq)
  (elapsedTime: System.TimeSpan)
  =
  let originalRows = originalRows |> Seq.map Array.tail

  [
    "colNames", columns |> List.map (fun column -> Encode.string column.Name) |> Encode.list
    "nullMappings", nullMappings |> Array.map Encode.float |> Encode.array
    "anonParams", encodeAnonParams anonParams
    "bucketizationParans", encodeBucketizationParams bucketizationParams
    "trees", forests |> encodeCombinations encodeNode
    "buckets", buckets |> encodeCombinations (Seq.map encodeBucket >> Encode.seq)
    "synRows", synRows |> Seq.map encodeValues |> Encode.seq
    "originalRows", originalRows |> Seq.map encodeValues |> Encode.seq
    "elapsedTime", elapsedTime.TotalSeconds |> Encode.float
  ]
  |> Encode.object
  |> Encode.toString 2
