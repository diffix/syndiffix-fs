module SynDiffix.Counter

open SynDiffix.Anonymizer

open BloomFilter

// If AID values are guaranteed to be unique, we don't need to track distinct values when checking
// whether entities are low-count or apply flattening when counting rows, which greatly reduces memory usage.
// In the generic case, tracking entities requires less memory than tracking row contributions,
// so the two counters are separated into different types.

type IEntityCounter =
  abstract Add: AidHash array -> unit
  abstract IsLowCount: byte array -> LowCountParams -> bool

type IRowCounter =
  abstract Add: AidHash array -> unit
  abstract Count: AnonymizationContext -> int64

let private aidValueIsNotNull hash = hash <> 0UL

type GenericAidEntityCounter(dimensions, estimatedCount: int) =
  let aidSets =
    Array.init
      dimensions
      (fun _ ->
        // In order to reduce memory usage, we use a bloom filter to count unique AID values.
        // The bloom filter will adjust its size automatically to match the target error rate for an estimated count.
        FilterBuilder.Build(uint estimatedCount, 0.02)
      )

  let counts = Array.create dimensions 0L
  let seeds = Array.create dimensions 0UL

  member this.Counts = counts // Needed for testing.

  interface IEntityCounter with
    member this.Add aidHashes =
      aidHashes
      |> Array.iteri (fun index aidHash ->
        if aidValueIsNotNull aidHash then
          if aidSets.[index].Add(aidHash) then
            counts.[index] <- counts.[index] + 1L
            seeds.[index] <- seeds.[index] ^^^ aidHash
      )

    member this.IsLowCount salt lowCountParams =
      (counts, seeds) ||> Seq.zip |> isLowCount salt lowCountParams

type GenericAidRowCounter(dimensions) =
  let contributions =
    Array.init dimensions (fun _ -> { AidContributions = Dictionary<AidHash, float>(); UnaccountedFor = 0.0 })

  interface IRowCounter with
    member this.Add aidValues =
      aidValues
      |> Array.iteri (fun index aidValue ->
        let contribution = contributions.[index]

        if aidValueIsNotNull aidValue then
          let updatedContribution =
            match contribution.AidContributions.TryGetValue(aidValue) with
            | true, aidContribution -> aidContribution + 1.0
            | false, _ -> 1.0

          contribution.AidContributions.[aidValue] <- updatedContribution
        else
          // Missing AID value, add to the unaccounted rows count, so we can flatten them separately.
          contribution.UnaccountedFor <- contribution.UnaccountedFor + 1.0
      )

    member this.Count anonContext =
      match countMultipleContributions anonContext contributions with
      | Some result -> result.AnonymizedCount
      | None -> 0

type UniqueAidCounter() =
  let mutable count = 0L
  let mutable seed = 0UL

  let add (aidHashes: AidHash array) =
    assert (aidHashes.Length = 1)

    if aidValueIsNotNull aidHashes.[0] then
      count <- count + 1L
      seed <- seed ^^^ aidHashes.[0]

  interface IEntityCounter with
    member this.Add aidHashes = add aidHashes

    member this.IsLowCount salt lowCountParams =
      isLowCount salt lowCountParams [ count, seed ]

  interface IRowCounter with
    member this.Add aidHashes = add aidHashes

    member this.Count anonContext =
      countSingleContributions anonContext count seed
