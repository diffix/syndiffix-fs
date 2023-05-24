module SynDiffix.MicrodataTests

open Xunit
open FsUnit.Xunit

open SynDiffix
open Range
open Microdata

let unwrapString =
  function
  | String value -> value
  | _ -> failwith "Expecting a `String` value."

[<Fact>]
let ``String mapping`` () =
  let convertor =
    [ "a"; "aaa"; "abc"; "bac"; "z"; "Abc"; "AAA"; "aaa"; "bac"; "a"; "a1"; "123"; "cab"; "bbb" ]
    |> List.map (fun string -> [| List [ Null ]; String string |])
    |> createDataConvertors [ StringType ]
    |> Array.head

  String "123" |> convertor.ToFloat |> should equal 0.0
  String "AAA" |> convertor.ToFloat |> should equal 1.0
  String "z" |> convertor.ToFloat |> should equal 10.0

  (1.0, 1.0)
  ||> createRange
  |> convertor.FromRange
  |> unwrapString
  |> should equal "AAA"

  (3.0, 5.0)
  ||> createRange
  |> convertor.FromRange
  |> unwrapString
  |> should startWith "a*"
