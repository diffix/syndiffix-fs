module SynDiffix.Program

open SynDiffix.ArgumentParsing
open SynDiffix.Engine

[<EntryPoint>]
let main argv =
  let result = argv |> parseArguments |> transform
  Csv.toString result.Columns result.SynRows |> printfn "%s"
  0
