module SynDiffix.AnonymizerTests

open Xunit
open FsUnit.Xunit

open SynDiffix.Anonymizer

[<Fact>]
let ``Test noisy row limit for depth limiting`` () =
  let salt = [| 1uy |]
  let seed = 0UL
  noisyRowLimit salt seed 100 1 |> should (equalWithin 5) 100
  noisyRowLimit salt seed 200 2 |> should (equalWithin 5) 100
  noisyRowLimit salt seed 100 10000 |> should equal 0
  noisyRowLimit salt seed 10000 10000 |> should equal 1
  noisyRowLimit salt seed 1000000 10000 |> should (equalWithin 5) 100
