module SynDiffix.Csv

open CsvHelper

open System
open System.Globalization
open System.IO

type CsvReadResult = Row seq * Columns

let columnTypeFromName =
  function
  | EndsWith ":i" -> Some(IntegerType)
  | EndsWith ":r" -> Some(RealType)
  | EndsWith ":b" -> Some(BooleanType)
  | EndsWith ":t" -> Some(TimestampType)
  | EndsWith ":s" -> Some(StringType)
  | _ -> None

let rowNumberAid = []

let openCsv (filePath: string) =
  let reader = File.OpenText(filePath)

  let configuration = Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
  configuration.DetectDelimiter <- true

  let csv = new CsvReader(reader, configuration)
  csv.Read() |> ignore
  csv.ReadHeader() |> ignore

  csv

let read (filePath: string) (columns: Columns) (aidColumns: string list) : CsvReadResult =
  let csv = openCsv filePath

  let headerColumns = csv.HeaderRecord |> Array.map (fun h -> h.Trim().Trim('\''))

  let readAidValue =
    match aidColumns with
    | [] ->
      let mutable rowIndex = 0L

      fun () ->
        rowIndex <- rowIndex + 1L
        List [ Integer rowIndex ]
    | aidColumns ->
      let aidColumnIndexes =
        aidColumns
        |> List.map (fun aidColumn ->
          headerColumns
          |> Array.tryFindIndex ((=) aidColumn)
          |> Option.defaultWith (fun () -> failwith $"AID Column '{aidColumn}' was not found.")
        )

      fun () -> aidColumnIndexes |> List.map (csv.GetField >> parseField StringType) |> List

  let columnIndexes =
    columns
    |> List.map (fun column ->
      let index = Array.IndexOf(headerColumns, column.Name)

      if index < 0 then
        failwith $"Column '{column.Name}' was not found."

      index, column.Type
    )
    |> List.toArray

  seq {
    while csv.Read() do
      let row =
        columnIndexes
        |> Array.map (fun (index, columnType) -> csv.GetField(index).Trim() |> parseField columnType)
        |> Array.insertAt 0 (readAidValue ())

      yield row
  },
  columns

let private csvFormat value =
  match value with
  | Null -> ""
  | Boolean false -> "false"
  | Boolean true -> "true"
  | String string -> string
  | Integer i -> i.ToString()
  | Real r -> r.ToString()
  | Timestamp ts -> ts.ToString("yyyy-MM-dd HH:mm:ss")
  | List _ -> value |> Value.toString |> String.quote

let toString (columns: Columns) (rows: Value[] seq) =
  let header = columns |> List.map (fun column -> column.Name) |> String.join ","

  let rows = rows |> Seq.map (fun row -> row |> Array.map csvFormat |> String.join ",")

  (Seq.concat [ Seq.singleton header; rows ] |> String.join "\n") + "\n"

let writeTo (writer: IO.TextWriter) (columns: Columns) (rows: Value[] seq) =
  let header = columns |> List.map (fun column -> column.Name) |> String.join ","
  writer.WriteLine(header)

  rows
  |> Seq.map (fun row -> row |> Array.map csvFormat |> String.join ",")
  |> Seq.iter writer.WriteLine

  writer.Flush()
