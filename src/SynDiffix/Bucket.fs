module rec SynDiffix.Bucket

open SynDiffix.Range
open SynDiffix.Combination
open SynDiffix.Forest

[<Struct>]
type Bucket = { Ranges: Ranges; Count: int64; NodeId: int }

type Buckets = Bucket array

let private adjustCounts (buckets: Buckets) (currentCount: int64) (targetCount: int64) =
  let ratio = (float targetCount) / (float currentCount)

  let buckets, _ =
    buckets
    |> Array.mapFold
      (fun accError bucket ->
        let adjustedCount = bucket.Count |> float |> ((*) ratio)
        let accError = accError + adjustedCount - floor (adjustedCount)

        // Add the previously accumulated errors to the current count, if larger than 1 unit.
        let adjustedCount, accError =
          if accError > 1.0 then
            adjustedCount + 1.0, accError - 1.0
          else
            adjustedCount, accError

        { bucket with Count = int64 adjustedCount }, accError
      )
      0.0

  buckets |> Array.filter (fun bucket -> bucket.Count > 0)

let private getSubBuckets (harvestedNodes: Dictionary<int, Buckets>) (node: Tree.Node) =
  let nodeData = Tree.nodeData node
  let dimensions = nodeData.SnappedRanges.Length
  let count = Tree.noisyRowCount node

  (nodeData.Subnodes, generateCombinations (dimensions - 1) dimensions)
  ||> Seq.map2 (fun subnode combination ->
    let actualSubnodeRanges = getItemsCombination combination nodeData.ActualRanges

    if actualSubnodeRanges |> Tree.isSingularityNode then
      [| { Ranges = actualSubnodeRanges; Count = count; NodeId = nodeData.Id } |]
    else
      subnode
      |> ValueOption.map (doHarvestBuckets harvestedNodes)
      |> ValueOption.defaultValue [||]
  )

let private getSmallestRanges (subBuckets: Buckets array) =
  let dimensions = subBuckets.Length

  let (cumulativeRanges: Range option array array) =
    Array.init dimensions (fun _ -> Array.create dimensions None)

  generateCombinations (dimensions - 1) dimensions
  |> Seq.iteri (fun combinationIndex combination ->
    subBuckets.[combinationIndex]
    |> Array.iter (fun bucket ->
      bucket.Ranges
      |> Array.iteri (fun rangeIndex bucketRange ->
        let dimensionIndex = combination.[rangeIndex]

        let cumulativeRange =
          match cumulativeRanges.[dimensionIndex].[combinationIndex] with
          | None -> bucketRange
          | Some cumulativeRange ->
            {
              Min = min cumulativeRange.Min bucketRange.Min
              Max = max cumulativeRange.Max bucketRange.Max
            }

        cumulativeRanges.[dimensionIndex].[combinationIndex] <- Some cumulativeRange
      )
    )
  )

  cumulativeRanges
  |> Array.map (
    Seq.filter Option.isSome
    >> Seq.map Option.get
    >> Seq.minBy (fun range -> range.Size())
  )

let private getPerDimensionRanges (smallestRanges: Ranges) (subBuckets: Buckets array) =
  let dimensions = subBuckets.Length
  let (perDimensionRanges: Range array array) = Array.create dimensions [||]

  (generateCombinations (dimensions - 1) dimensions, subBuckets)
  ||> Seq.iter2 (fun subNodeCombination subNodeBuckets ->
    subNodeCombination
    |> Array.iteri (fun rangeIndex dimensionIndex ->
      perDimensionRanges.[dimensionIndex] <-
        subNodeBuckets
        |> Array.filter (fun bucket -> smallestRanges.[dimensionIndex].Contains bucket.Ranges.[rangeIndex])
        |> Array.collect (fun bucket -> Array.replicate (int bucket.Count) bucket.Ranges.[rangeIndex])
        |> Array.append perDimensionRanges.[dimensionIndex]
    )
  )

  perDimensionRanges

let private compactSmallestRanges (node: Tree.Node) (smallestRanges: Ranges) =
  (smallestRanges, Tree.getHalvedRanges node)
  ||> Array.map2 (fun smallestRange compactedNodeRange ->
    if compactedNodeRange.Overlaps smallestRange then
      // Drop the sections of the smallest range which are outside the compacted node range.
      {
        Min = max smallestRange.Min compactedNodeRange.Min
        Max = min smallestRange.Max compactedNodeRange.Max
      }
    else
      // If the ranges have no overlap, expand the smallest range to include both.
      {
        Min = min smallestRange.Min compactedNodeRange.Min
        Max = max smallestRange.Max compactedNodeRange.Max
      }
  )

let private getSubBucketsRanges (smallestRanges: Ranges) (subBuckets: Buckets array) =
  assert (smallestRanges.Length = subBuckets.Length)
  let dimensions = smallestRanges.Length

  generateCombinations (dimensions - 1) dimensions
  |> Seq.mapi (fun combinationIndex combination ->
    let smallestRanges = getItemsCombination combination smallestRanges

    subBuckets.[combinationIndex]
    |> Array.filter (fun bucket ->
      smallestRanges
      |> Array.forall2 (fun bucketRange smallestRange -> smallestRange.Contains bucketRange) bucket.Ranges
    )
    |> Array.collect (fun bucket -> Array.replicate (int bucket.Count) bucket.Ranges)
  )
  |> Seq.toArray

// This source of randomness isn't sticky, so can only be applied to already anonymized data.
let private nonStickyRng = System.Random(0)

let private matchSubRanges (count: int) (perDimensionRanges: Range array array) (subNodesRanges: Ranges array array) =
  assert (perDimensionRanges.Length = subNodesRanges.Length)
  let dimensions = perDimensionRanges.Length

  seq { 0 .. count - 1 }
  |> Seq.map (fun matchCounter ->
    // We cycle through all the dimensions and subnodes equally when selecting the sub-ranges to be matched.
    let subNodeIndex = matchCounter % dimensions
    let dimensionIndex = dimensions - subNodeIndex - 1

    let subNodeRanges = subNodesRanges.[subNodeIndex]
    let dimensionRanges = perDimensionRanges.[dimensionIndex]

    // Create a random match between the selected sub-ranges in order to avoid biases in the output.
    let subNodeRange = subNodeRanges.[nonStickyRng.Next(subNodeRanges.Length)]
    let dimensionRange = dimensionRanges.[nonStickyRng.Next(dimensionRanges.Length)]

    subNodeRange |> Array.insertAt dimensionIndex dimensionRange
  )

let private refineBuckets (harvestedNodes: Dictionary<int, Buckets>) (node: Tree.Node) (count: int64) =
  let nodeData = Tree.nodeData node

  let bucket =
    {
      Ranges = Tree.createBucketRanges nodeData
      Count = count
      NodeId = nodeData.Id
    }

  let subBuckets = node |> getSubBuckets harvestedNodes |> Seq.toArray

  if subBuckets |> Array.exists Array.isEmpty then
    [| bucket |]
  else
    let smallestRanges = subBuckets |> getSmallestRanges |> compactSmallestRanges node
    let perDimensionRanges = subBuckets |> getPerDimensionRanges smallestRanges
    let subNodesRanges = subBuckets |> getSubBucketsRanges smallestRanges

    if
      perDimensionRanges |> Array.exists Array.isEmpty
      || subNodesRanges |> Seq.exists Array.isEmpty
    then
      // Can't match if there are no buckets for some subnodes or no subranges for some dimensions.
      [| bucket |]
    else
      matchSubRanges (int count) perDimensionRanges subNodesRanges
      |> Seq.map (fun ranges -> { Ranges = ranges; Count = 1; NodeId = nodeData.Id })
      |> Seq.toArray

let rec private doHarvestBuckets (harvestedNodes: Dictionary<int, Buckets>) (node: Tree.Node) : Buckets =
  let nodeId = (Tree.nodeData node).Id

  match harvestedNodes.TryGetValue(nodeId) with
  | false, _ ->
    let harvestedBuckets =
      match node with
      | Tree.Leaf leaf ->
        let lowThreshold = leaf.Data.Context.AnonymizationContext.AnonymizationParams.Suppression.LowThreshold

        if Tree.dataCrossesLowThreshold lowThreshold leaf.Data then
          let count = Tree.noisyRowCount node
          let bucketRanges = Tree.createBucketRanges leaf.Data

          if Tree.isSingularityNode bucketRanges || bucketRanges.Length = 1 then
            [| { Ranges = bucketRanges; Count = count; NodeId = leaf.Data.Id } |]
          else
            refineBuckets harvestedNodes node count
        else
          [||]

      | Tree.Branch branch ->
        let childrenBuckets =
          branch.Children.Values
          |> Seq.collect (doHarvestBuckets harvestedNodes)
          |> Seq.toArray

        let childrenCount = childrenBuckets |> Array.sumBy (fun bucket -> bucket.Count)
        let totalCount = Tree.noisyRowCount node

        if 2L * childrenCount < totalCount then
          let bucketRanges = Tree.createBucketRanges branch.Data

          if bucketRanges.Length = 1 then
            [| { Ranges = bucketRanges; Count = totalCount; NodeId = branch.Data.Id } |]
          else
            refineBuckets harvestedNodes node (totalCount - childrenCount)
            |> Array.append childrenBuckets
        else
          adjustCounts childrenBuckets childrenCount totalCount

    harvestedNodes.Add(nodeId, harvestedBuckets)

    harvestedBuckets
  | true, harvestedBuckets -> harvestedBuckets

let harvestBuckets (node: Tree.Node) : Buckets =
  // Maps node.NodeData.Id to its already harvested buckets, so it's done only once.
  let harvestedNodes: Dictionary<int, Buckets> = new Dictionary<int, Buckets>()
  doHarvestBuckets harvestedNodes node
