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
  |> vfst
  |> unwrapString
  |> should equal "AAA"

  (3.0, 5.0)
  ||> createRange
  |> convertor.FromRange
  |> vfst
  |> unwrapString
  |> should startWith "a*"

[<Fact>]
let ``Boolean mapping`` () =
  let convertor =
    [ true; false ]
    |> List.map (fun b -> [| List [ Null ]; Boolean b |])
    |> createDataConvertors [ BooleanType ]
    |> Array.head

  Boolean true |> convertor.ToFloat |> should equal 1.0
  Boolean false |> convertor.ToFloat |> should equal 0.0

  (0.0, 0.5)
  ||> createRange
  |> convertor.FromRange
  |> vfst
  |> should equal (Boolean false)

  (0.5, 1.0)
  ||> createRange
  |> convertor.FromRange
  |> vfst
  |> should equal (Boolean true)
