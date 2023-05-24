[<AutoOpen>]
module SynDiffix.Utils

open System

let mapFst f (a, b) = (f a), b
let mapSnd f (a, b) = a, (f b)

let (|EndsWith|_|) (substring: string) (input: string) =
  if input.EndsWith(substring) then Some() else None

// Aliases for common mutable collections
type MutableList<'T> = Collections.Generic.List<'T>
type Dictionary<'K, 'V> = Collections.Generic.Dictionary<'K, 'V>
type KeyValuePair<'K, 'V> = Collections.Generic.KeyValuePair<'K, 'V>
type HashSet<'T> = Collections.Generic.HashSet<'T>
type Stack<'T> = Collections.Generic.Stack<'T>

// 3-tuple deconstruction.
let inline fst3 (a, _, _) = a
let inline snd3 (_, b, _) = b
let inline thd3 (_, _, c) = c

module String =
  let join (sep: string) (values: seq<'T>) = String.Join<'T>(sep, values)

  let joinWithComma (values: seq<'T>) = join ", " values

  let quote (string: string) =
    "\"" + string.Replace("\"", "\"\"") + "\""

  let quoteSingle (string: string) = "'" + string.Replace("'", "''") + "'"

  let equalsI s1 s2 =
    String.Equals(s1, s2, StringComparison.InvariantCultureIgnoreCase)

  let toLower (s: string) = s.ToLower()

module Set =
  let addSeq items set =
    items |> Seq.fold (fun acc item -> Set.add item acc) set

module Result =
  let value (result: Result<'T, string>) : 'T =
    match result with
    | Ok result -> result
    | Error err -> failwith err

module Dictionary =
  let getOrDefault key defaultValue (dict: Dictionary<'K, 'V>) =
    match dict.TryGetValue(key) with
    | true, value -> value
    | false, _ -> defaultValue

  let getOrInit key initFn (dict: Dictionary<'K, 'V>) =
    match dict.TryGetValue(key) with
    | true, value -> value
    | false, _ ->
      let value = initFn ()
      dict.[key] <- value
      value

  let inline increment key (dict: Dictionary<'K, ^V>) =
    dict.[key] <-
      match dict.TryGetValue(key) with
      | true, count -> count + LanguagePrimitives.GenericOne
      | false, _ -> LanguagePrimitives.GenericOne

type Hash = uint64

module Hash =
  let bytes (data: byte[]) : Hash =
    // Implementation of FNV-1a hash algorithm: http://www.isthe.com/chongo/tech/comp/fnv/index.html
    let fnvPrime = 1099511628211UL
    let offsetBasis = 14695981039346656037UL

    let mutable hash = offsetBasis

    for octet in data do
      hash <- hash ^^^ uint64 octet
      hash <- hash * fnvPrime

    if hash = 0UL then 1UL else hash

  let string (data: string) =
    data |> Text.Encoding.UTF8.GetBytes |> bytes

  let strings start data =
    data |> Seq.distinct |> Seq.map string |> Seq.fold (^^^) start

// introduced for compatibility of math-related functions with C (PostgreSQL)
module Math =
  let roundAwayFromZero (x: float) : float =
    System.Math.Round(x, MidpointRounding.AwayFromZero)
