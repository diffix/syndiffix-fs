module SynDiffix.Range

open System

[<Struct>]
type Range =
  {
    // For simplicity, we use float boundaries for Boolean, Integer and Real ranges.
    // Other types of ranges are not yet supported.
    // Notice: this won't work for integer values bigger than 2^53 because of floating point limitations.
    Min: float
    Max: float
  }
  member this.Middle() =
    if this.Min = this.Max then this.Min else (this.Min + this.Max) / 2.0

  // Returns 0 if value is in range's lower half, 1 if value is in range's upper half.
  member this.HalfIndex(value: float) =
    if this.Min = this.Max || value < this.Middle() then 0 else 1

  member this.Half(index: int) =
    match index with
    | 0 -> { Min = this.Min; Max = this.Middle() }
    | 1 -> { Min = this.Middle(); Max = this.Max }
    | _ -> failwith "Invalid index for range half!"

  member this.Contains(range: Range) =
    range.Min >= this.Min && range.Max <= this.Max

  member this.ContainsValue(value: float) =
    value = this.Min || (value > this.Min && value < this.Max)

  member this.Overlaps(range: Range) =
    range.Min < this.Max && range.Max > this.Min

  member this.Size() = this.Max - this.Min

  member this.IsSingularity() = this.Min = this.Max

type Ranges = Range array

let private nextPowerOf2 x =
  assert (x > 0.0)
  Math.Pow(2.0, Math.Ceiling(Math.Log2(x)))

let private floorBy value amount = (value / amount) |> floor |> (*) amount

let rec private snapFloatRange (min: float) (max: float) =
  let snappedSize =
    match max - min with
    // Only happens when there is a single value in the column. In practice, the size of the snapped range won't matter,
    // because only a singularity will be present, but the forest handling code doesn't expect 0-size ranges.
    | 0.0 -> 1.0
    | size -> nextPowerOf2 size

  let alignedMin = floorBy min (snappedSize / 2.0)

  if alignedMin + snappedSize < max then
    snapFloatRange alignedMin max // This snapped range doesn't fit, so we need to increase it.
  else
    alignedMin, alignedMin + snappedSize

let createRange (min: float) (max: float) = { Min = min; Max = max }

let snapRange (range: Range) =
  let min, max = snapFloatRange range.Min range.Max
  { Min = min; Max = max }

let expandRange (value: float) (range: Range) =
  { Min = min value range.Min; Max = max value range.Max }
