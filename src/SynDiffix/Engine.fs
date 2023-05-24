module SynDiffix.Engine

open System
open System.IO
open System.Security.Cryptography

open SynDiffix.Clustering
open SynDiffix.Forest
open SynDiffix.Microdata
open SynDiffix.Counter

open SynDiffix.ArgumentParsing

type Result =
  {
    Columns: Column list
    Rows: Row array
    SynRows: Row array
    Forest: Forest
    ElapsedTime: TimeSpan
  }

let private createCountStrategy aidColumns maxThreshold =
  if aidColumns = Csv.rowNumberAid then
    {
      CreateEntityCounter = fun () -> UniqueAidCounter()
      CreateRowCounter = fun () -> UniqueAidCounter()
    }
  else
    {
      CreateEntityCounter = fun () -> GenericAidEntityCounter(aidColumns.Length, maxThreshold)
      CreateRowCounter = fun () -> GenericAidRowCounter(aidColumns.Length)
    }

let private printScores (scores: float[,]) =
  let dims = scores.GetLength(0)

  eprintf "   "

  for i in 0 .. dims - 1 do
    eprintf $"{i.ToString().PadLeft(6, ' ')}"

  eprintfn ""

  for i in 0 .. dims - 1 do
    eprintf $"{i.ToString().PadLeft(2, ' ')} "
    let mutable total = 0.0

    for j in 0 .. dims - 1 do
      if i = j then
        eprintf "  ----"
      else
        total <- total + scores.[i, j]
        eprintf "%s" (scores.[i, j].ToString("0.00").PadLeft(6, ' '))

    eprintfn "  = %s" (total.ToString("0.00"))

  eprintfn ""
  eprintf " = "

  for j in 0 .. dims - 1 do
    let mutable total = -1.0

    for i in 0 .. dims - 1 do
      total <- total + scores.[i, j]

    eprintf "%s" (total.ToString("0.00").PadLeft(6, ' '))

  eprintfn ""

let transform (arguments: ParsedArguments) =
  let countStrategy =
    createCountStrategy arguments.AidColumns arguments.BucketizationParams.RangeLowThreshold

  if arguments.Verbose then
    eprintfn "Computing salt..."

  let salt =
    use fileStream = File.Open(arguments.CsvPath, FileMode.Open, FileAccess.Read)
    (SHA256.Create()).ComputeHash fileStream

  let anonParams = { arguments.AnonymizationParams with Salt = salt }

  if arguments.Verbose then
    eprintfn "Reading rows..."

  let rows, columns =
    Csv.read arguments.CsvPath arguments.CsvColumns arguments.AidColumns
    |> mapFst Seq.toArray

  let stopWatch = Diagnostics.Stopwatch.StartNew()

  if arguments.Verbose then
    eprintfn "Building forest..."

  let columnNames, columnTypes =
    columns |> List.map (fun column -> column.Name, column.Type) |> List.unzip

  let dataConvertors = rows |> createDataConvertors columnTypes

  let anonContext = { BucketSeed = Hash.string arguments.CsvPath; AnonymizationParams = anonParams }

  let forest =
    Forest(rows, dataConvertors, anonContext, arguments.BucketizationParams, columnNames, countStrategy)

  let allColumns = [ 0 .. forest.Dimensions - 1 ]

  let patchList =
    if arguments.BucketizationParams.ClusteringEnabled then
      if arguments.BucketizationParams.ClusteringMaxClusterSize <= 1 then
        allColumns |> List.map (fun col -> Clustering.BaseCluster [ col ])
      else
        let forest' = if Sampling.shouldSample forest then Sampling.sampleForest forest else forest

        if arguments.Verbose then
          eprintfn "Measuring dependence..."

        let clusteringContext = Solver.clusteringContext arguments.MainColumn forest'

        if arguments.Verbose then
          printScores clusteringContext.DependencyMatrix

        if arguments.Verbose then
          eprintfn "Assigning clusters..."

        Solver.solve clusteringContext
    else
      if arguments.Verbose then
        eprintfn "Using all columns."

      [ Clustering.BaseCluster allColumns ]

  if arguments.Verbose then
    eprintfn $"Clusters: %A{patchList}."
    eprintfn $"Num patches: %A{patchList.Length}."
    eprintfn "Materializing clusters..."

  // TODO: Add AID column here after removing from materializeTree.
  let synRows, columnIds = Clustering.buildTable materializeTree forest patchList

  if arguments.Verbose then
    eprintfn "Microtable built."

  if Array.toList columnIds <> allColumns then
    failwith "Expected all columns to be present in final microtable."

  stopWatch.Stop()

  if arguments.Verbose then
    eprintfn $"Time elapsed: {stopWatch.Elapsed.TotalMilliseconds} ms."
    eprintfn $"Memory used: {GC.GetTotalMemory(true) / (1024L * 1024L)} MB."

  {
    Columns = columns
    Rows = rows
    SynRows = synRows
    Forest = forest
    ElapsedTime = stopWatch.Elapsed
  }
