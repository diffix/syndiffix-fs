module SynDiffix.Program

open SynDiffix.ArgumentParsing
open SynDiffix.Engine

open SynDiffix.Forest
open SynDiffix.Bucket

let rec private collectRoots (root: Tree) =
  let lowerRoots =
    (Tree.nodeData root).Subnodes
    |> Seq.choose (
      function
      | ValueSome x -> Some x
      | ValueNone -> None
    )
    |> Seq.collect collectRoots
    |> Seq.distinctBy (fun root -> (Tree.nodeData root).Context.Combination)
    |> Seq.toList

  root :: lowerRoots

[<EntryPoint>]
let main argv =
  let arguments = parseArguments argv
  let result = transform arguments

  let debugTrees =
    if arguments.Verbose then
      // Gets all built trees, including lower dimensions.
      result.Forest.GetAvailableTrees()
      |> List.collect collectRoots
      |> List.map (fun root -> (Tree.nodeData root).Context.Combination, root)
      |> List.distinctBy fst
    else
      []

  let debugBuckets = debugTrees |> List.map (mapSnd harvestBuckets)

  Json.toString
    result.Columns
    result.Forest.NullMappings
    result.Forest.AnonymizationContext.AnonymizationParams
    result.Forest.BucketizationParams
    debugTrees
    debugBuckets
    result.SynRows
    result.Rows
    result.ElapsedTime
  |> printfn "%s"

  0
