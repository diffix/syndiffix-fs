module SynDiffix.Program

open SynDiffix.ArgumentParsing
open SynDiffix.Engine

[<EntryPoint>]
let main argv =
  try
    let result = argv |> parseArguments |> transform
    Csv.toString result.Columns result.SynRows |> printfn "%s"
    0
  with ex ->
    eprintfn "ERROR: %s" ex.Message
#if DEBUG
    eprintfn "StackTrace:\n%s" ex.StackTrace
#endif
    1
