module SynDiffix.CommonTests

open Xunit
open FsUnit.Xunit

let parseTimestamp = parseField TimestampType

let private toUtc value =
  match value with
  | Timestamp ts -> Timestamp(ts.ToUniversalTime())
  | other -> other

[<Fact>]
let ``Parse timestamp from ISO 8601`` () =
  parseTimestamp "2023-01-24T14:25:47.000Z"
  |> toUtc
  |> should equal (makeTimestamp (2023, 1, 24) (14, 25, 47))

  parseTimestamp "2023-01-24T14:25:47-05:00"
  |> toUtc
  |> should equal (makeTimestamp (2023, 1, 24) (19, 25, 47))

  parseTimestamp "2023-01-24T14:25:47+01:00"
  |> toUtc
  |> should equal (makeTimestamp (2023, 1, 24) (13, 25, 47))

[<Fact>]
let ``Parse timestamp from loose string`` () =
  parseTimestamp "2023-01-24"
  |> should equal (makeTimestamp (2023, 1, 24) (0, 0, 0))

  parseTimestamp "2023-01-24 14:25"
  |> should equal (makeTimestamp (2023, 1, 24) (14, 25, 0))

  parseTimestamp "2023/01/24 14:25:47"
  |> should equal (makeTimestamp (2023, 1, 24) (14, 25, 47))
