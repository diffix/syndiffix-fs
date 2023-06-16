module SynDiffix.Microdata

open System

open SynDiffix
open SynDiffix.Combination
open SynDiffix.Range
open SynDiffix.Forest
open SynDiffix.Bucket

// This source of randomness isn't sticky, so can only be applied to already anonymized data.
let private nonStickyRng = Random(0)

let private generateFloat (range: Range) =
  range.Min + nonStickyRng.NextDouble() * range.Size()

type BooleanConvertor() =
  interface IDataConvertor with
    member this.ColumnType = BooleanType

    member this.ToFloat(value) =
      match value with
      | Boolean true -> 1.0
      | Boolean false -> 0.0
      | _ -> failwith "Unexpected value type!"

    member this.FromRange(range) =
      let value = generateFloat range >= 0.5
      struct (Boolean value, (if value then 1.0 else 0.0))

type RealConvertor() =
  interface IDataConvertor with
    member this.ColumnType = RealType

    member this.ToFloat(value) =
      match value with
      | Real value -> value // TODO: handle non-finite reals.
      | _ -> failwith "Unexpected value type!"

    member this.FromRange(range) =
      let value = generateFloat range
      struct (Real value, value)

type IntegerConvertor() =
  interface IDataConvertor with
    member this.ColumnType = IntegerType

    member this.ToFloat(value) =
      match value with
      | Integer value -> float value
      | _ -> failwith "Unexpected value type!"

    member this.FromRange(range) =
      let value = range |> generateFloat |> int64
      struct (Integer value, float value)

type TimestampConvertor() =
  let toUnspecified dateTime =
    DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified)

  interface IDataConvertor with
    member this.ColumnType = TimestampType

    member this.ToFloat(value) =
      match value with
      | Timestamp value -> (value - TIMESTAMP_REFERENCE).TotalSeconds
      | _ -> failwith "Unexpected value type!"

    member this.FromRange(range) =
      let value = generateFloat range

      struct (value
              |> TimeSpan.FromSeconds
              |> (+) TIMESTAMP_REFERENCE
              |> toUnspecified
              |> Timestamp,
              value)

type StringConvertor(values: Value seq) =
  // Extracts distinct String values from the data and produces a mapping from their float-representations
  // back to String, which will be used in the microdata generation step
  let valueMap =
    values
    |> Seq.filter ((<>) Null)
    |> Seq.map (fun value ->
      match value with
      | String value -> value
      | _ -> failwith "Unexpected value type!"
    )
    |> Set.ofSeq
    |> Set.toArray

  let stringsPrefix (string1: string) (string2: string) =
    let min = min string1.Length string2.Length
    let prefix = Text.StringBuilder(min)
    let mutable i = 0

    while i < min && string1[i] = string2[i] do
      string1[i] |> prefix.Append |> ignore
      i <- i + 1

    prefix.ToString()

  let mapRange range =
    let minValue = valueMap[int range.Min]
    let maxValue = valueMap[min (int range.Max) (valueMap.Length - 1)]
    let value = range |> generateFloat |> int64

    // The common prefix of the values assigned to the range is safe to show.
    struct (String(
              (stringsPrefix minValue maxValue)
              + "*"
              // In order to match the count of distinct values in the input,
              // we append a random integer value from inside the range.
              + value.ToString()
            ),
            float value)

  interface IDataConvertor with
    member this.ColumnType = StringType

    member this.ToFloat(value) =
      match value with
      | String value ->
        let index = Array.BinarySearch(valueMap, value, StringComparer.Ordinal)
        assert (index >= 0)
        float index
      | _ -> failwith "Unexpected value type!"

    member this.FromRange(range) =
      if range.IsSingularity() then
        struct (String valueMap[int range.Min], range.Min)
      else
        mapRange range

let private generateField (dataConvertor: IDataConvertor) (nullMapping: float) (range: Range) : MicrodataValue =
  if nullMapping = range.Min then
    assert range.IsSingularity()
    struct (Null, nullMapping)
  else
    dataConvertor.FromRange(range)

let private generateRow dataConvertors nullMappings ranges _index =
  Seq.map3 generateField dataConvertors nullMappings ranges |> Seq.toArray

let createDataConvertors (columnTypes: ExpressionType seq) (rows: Row seq) =
  columnTypes
  |> Seq.mapi (fun columnIndex columnType ->
    let fieldIndex = columnIndex + 1 // Skip AID instances field.

    match columnType with
    | BooleanType -> BooleanConvertor() :> IDataConvertor
    | IntegerType -> IntegerConvertor() :> IDataConvertor
    | RealType -> RealConvertor() :> IDataConvertor
    | TimestampType -> TimestampConvertor() :> IDataConvertor
    | StringType -> rows |> Seq.map (Array.item fieldIndex) |> StringConvertor :> IDataConvertor
    | _ -> failwith $"Invalid column type: `{columnType}`."
  )
  |> Seq.toArray

let generateMicrodata
  (dataConvertors: IDataConvertors)
  (nullMappings: float array)
  (buckets: Buckets)
  : MicrodataRow seq =
  buckets
  |> Seq.collect (fun bucket -> Seq.init (int bucket.Count) (generateRow dataConvertors nullMappings bucket.Ranges))

let materializeTree (forest: Forest) (combination: int seq) =
  let combination = combination |> Seq.sort |> Seq.toArray
  let tree = forest.GetTree(combination)
  let dataConverters = forest.DataConvertors |> getItemsCombination combination
  let nullMappings = forest.NullMappings |> getItemsCombination combination

  let rows =
    tree
    |> harvestBuckets
    |> generateMicrodata dataConverters nullMappings
    |> Seq.toArray

  rows, combination
