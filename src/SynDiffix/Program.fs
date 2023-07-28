module SynDiffix.Program

open SynDiffix.ArgumentParsing
open SynDiffix.Engine

[<EntryPoint>]
let main argv =
  try
    let parsedArgs = argv |> parseArguments
    let result = parsedArgs |> transform 1
    Csv.writeTo parsedArgs.OutputWriter result.Columns result.SynRows
    0
  with ex ->
    eprintfn "ERROR: %s" ex.Message
#if DEBUG
    eprintfn "StackTrace:\n%s" ex.StackTrace
#endif
    1
