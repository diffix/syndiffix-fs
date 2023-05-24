module SynDiffix.RangeTests

open System
open Xunit
open FsUnit.Xunit

open SynDiffix
open Range
open Microdata

let integerConvertor = IntegerConvertor() :> IDataConvertor
let realConvertor = RealConvertor() :> IDataConvertor
let timestampConvertor = TimestampConvertor() :> IDataConvertor

let valueToFloat value =
  match value with
  | Integer _ -> integerConvertor.ToFloat(value)
  | Real _ -> realConvertor.ToFloat(value)
  | Timestamp _ -> timestampConvertor.ToFloat(value)
  | _ -> failwith "Invalid value!"

let createSnappedRange min max =
  (valueToFloat min, valueToFloat max) ||> createRange |> snapRange

[<Fact>]
let ``Creates snapped ranges for ints`` () =
  (createSnappedRange (Integer 1) (Integer 2))
  |> should equal { Min = 1.0; Max = 2.0 }

  (createSnappedRange (Integer 3) (Integer 7))
  |> should equal { Min = 0.0; Max = 8.0 }

  (createSnappedRange (Integer 11) (Integer 21))
  |> should equal { Min = 8.0; Max = 24.0 }

  (createSnappedRange (Integer 11) (Integer 14))
  |> should equal { Min = 10.0; Max = 14.0 }

  (createSnappedRange (Integer -1) (Integer 2))
  |> should equal { Min = -2.0; Max = 2.0 }

  (createSnappedRange (Integer -3) (Integer -2))
  |> should equal { Min = -3.0; Max = -2.0 }

  (createSnappedRange (Integer -7) (Integer 0))
  |> should equal { Min = -8.0; Max = 0.0 }

  (createSnappedRange (Integer 0) (Integer 5))
  |> should equal { Min = 0.0; Max = 8.0 }

  (createSnappedRange (Integer -5) (Integer -2))
  |> should equal { Min = -6.0; Max = -2.0 }

  (createSnappedRange (Integer -5) (Integer 7))
  |> should equal { Min = -8.0; Max = 8.0 }

  (createSnappedRange (Integer -6) (Integer 2))
  |> should equal { Min = -8.0; Max = 8.0 }

  (createSnappedRange (Integer 21) (Integer 23))
  |> should equal { Min = 21.0; Max = 23.0 }

  (createSnappedRange (Integer 0) (Integer 0))
  |> should equal { Min = 0.0; Max = 1.0 }


[<Fact>]
let ``Creates snapped ranges for floats`` () =
  (createSnappedRange (Real 0.200000) (Real 0.400000))
  |> should equal { Min = 0.0; Max = 0.5 }

  (createSnappedRange (Real 0.010000) (Real 0.100000))
  |> should equal { Min = 0.0; Max = 0.125 }

  (createSnappedRange (Real -1.400000) (Real -0.300000))
  |> should equal { Min = -2.0; Max = -0.0 }

  (createSnappedRange (Real 0.333000) (Real 0.780000))
  |> should equal { Min = 0.0; Max = 1.0 }

  (createSnappedRange (Real 0.002000) (Real 0.010000))
  |> should equal { Min = 0.0; Max = 0.015625 }

  (createSnappedRange (Real 0.660000) (Real 0.900000))
  |> should equal { Min = 0.5; Max = 1.0 }

  (createSnappedRange (Real 10.001) (Real 10.002))
  |> should equal { Min = 10.0009765625; Max = 10.0029296875 }

  (createSnappedRange (Real 158.88434124351295) (Real 158.94684124353768))
  |> should equal { Min = 158.875; Max = 159.0 }

  (createSnappedRange (Real 0.0) (Real 1e-17))
  |> should equal { Min = 0.0; Max = Math.Pow(2.0, -56) }

  (createSnappedRange (Real 0) (Real(Math.Pow(2.0, -1073))))
  |> should equal { Min = 0.0; Max = Math.Pow(2.0, -1073) }

  (createSnappedRange (Real 0) (Real(Math.Pow(2.0, -1073) + Math.Pow(2.0, -1074))))
  |> should equal { Min = 0.0; Max = Math.Pow(2.0, -1072) }

[<Fact>]
let ``Creates snapped ranges for timestamps`` () =
  (createSnappedRange (makeTimestamp (2004, 1, 3) (13, 25, 47)) (makeTimestamp (2005, 3, 5) (21, 13, 7)))
  |> should equal { Min = 6408896512.0; Max = 6476005376.0 }
