module SynDiffix.Combination

// An ordered set of 0-based indices describing a combination of generic items.
type Combination = int array

let rec private generateCombinationsHelper (accumulator: Combination) index current max =
  let accumulator = Array.copy accumulator // Avoids subtle bugs when mapping combinations through a lazy sequence.

  seq {
    if index = accumulator.Length then
      yield accumulator
    elif current > max then
      ()
    else
      accumulator.[index] <- current
      yield! generateCombinationsHelper accumulator (index + 1) (current + 1) max
      yield! generateCombinationsHelper accumulator index (current + 1) max
  }

// Returns a sequence with all combinations of `n` integers taken by `k` amounts.
let generateCombinations k n =
  if k = 0 then
    Seq.empty
  else
    generateCombinationsHelper (Array.zeroCreate k) 0 0 (n - 1)

// Returns the array items specified by the given combination.
let getItemsCombination<'T> (indices: Combination) (items: 'T array) = indices |> Array.map (Array.get items)

let isSubsetCombination lowerCombination upperCombination =
  Array.forall (fun index -> Array.exists ((=) index) upperCombination) lowerCombination

let toString (combination: Combination) = $"({String.joinWithComma combination})"
