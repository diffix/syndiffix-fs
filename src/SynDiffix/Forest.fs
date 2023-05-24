module SynDiffix.Forest

open System

open SynDiffix.Combination
open SynDiffix.Range
open SynDiffix.Counter

type CountStrategy =
  {
    CreateEntityCounter: unit -> IEntityCounter
    CreateRowCounter: unit -> IRowCounter
  }

module rec Tree =
  type Subnodes = Node voption array

  type Context =
    {
      Combination: Combination
      AnonymizationContext: AnonymizationContext
      BucketizationParams: BucketizationParams
      RowLimit: int
      mutable BuildTime: TimeSpan
      CountStrategy: CountStrategy
    }

  type NodeData =
    {
      Id: int
      SnappedRanges: Ranges
      ActualRanges: Ranges
      Subnodes: Subnodes
      IsStub: bool
      EntityCounter: IEntityCounter
      Context: Context
      mutable IsStubSubnode: bool voption
      mutable NoisyCount: int64
    }

  type Row = { Values: float array; Aids: AidHash array }

  [<Struct>]
  type Leaf = { Data: NodeData; Rows: MutableList<Row> }

  [<Struct>]
  type Branch = { Data: NodeData; Children: Map<int, Node> }

  type Node =
    | Leaf of Leaf
    | Branch of Branch

  let nodeData =
    function
    | Leaf leaf -> leaf.Data
    | Branch branch -> branch.Data

  let private getNextNodeId =
    let mutable nodeId = 0

    fun () ->
      nodeId <- nodeId + 1
      nodeId

  let createBucketRanges (nodeData: NodeData) =
    (nodeData.SnappedRanges, nodeData.ActualRanges)
    ||> Array.map2 (fun snapped actual -> if actual.IsSingularity() then actual else snapped)

  let rec private collectRows =
    function
    | Leaf leaf -> leaf.Rows :> Row seq
    | Branch branch -> branch.Children.Values |> Seq.collect collectRows

  let noisyRowCount (node: Node) =
    let nodeData = nodeData node

    if nodeData.NoisyCount = 0 then
      let anonContext = nodeData.Context.AnonymizationContext

      // Uses range midpoints as the "bucket labels" for seeding.
      let bucketSeed =
        nodeData
        |> createBucketRanges
        |> Array.map (fun range -> Real(range.Middle()))
        |> Array.toList
        |> Value.addToSeed anonContext.BucketSeed

      let anonContext = { anonContext with BucketSeed = bucketSeed }
      let minCount = anonContext.AnonymizationParams.Suppression.LowThreshold
      let rowCounter = nodeData.Context.CountStrategy.CreateRowCounter()

      node |> collectRows |> Seq.iter (fun row -> rowCounter.Add row.Aids)
      nodeData.NoisyCount <- anonContext |> rowCounter.Count |> max minCount

    nodeData.NoisyCount

  let dataCrossesLowThreshold lowThreshold (nodeData: NodeData) =
    let anonParams = nodeData.Context.AnonymizationContext.AnonymizationParams
    let lowCountParams = { anonParams.Suppression with LowThreshold = lowThreshold }

    nodeData.EntityCounter.IsLowCount anonParams.Salt lowCountParams |> not

  let isSingularityNode (ranges: Ranges) =
    ranges |> Array.forall (fun range -> range.IsSingularity())

  let private isStubSubnode =
    function
    | ValueSome node ->
      match nodeData node with
      | { IsStubSubnode = ValueSome value } -> value
      | { IsStub = true } -> true
      | nodeData ->
        let bucketizationParams = nodeData.Context.BucketizationParams

        let stubLowTreshold =
          if isSingularityNode nodeData.ActualRanges then
            bucketizationParams.SingularityLowThreshold
          else
            bucketizationParams.RangeLowThreshold

        let isStubSubnode = nodeData |> dataCrossesLowThreshold stubLowTreshold |> not
        nodeData.IsStubSubnode <- ValueSome isStubSubnode
        isStubSubnode
    | ValueNone -> true

  let private allSubnodesAreStubs (subnodes: Subnodes) =
    // 0-dim subnodes of 1-dim nodes are not considered stubs.
    subnodes.Length > 0 && subnodes |> Array.forall isStubSubnode

  let private hashAid (aidValue: Value) =
    match aidValue with
    | Null
    | String "" -> 0UL
    | Integer i -> i |> System.BitConverter.GetBytes |> Hash.bytes
    | String s -> Hash.string s
    | _ -> failwith "Unsupported AID type."

  let private updateAids (row: Row) (nodeData: NodeData) =
    nodeData.EntityCounter.Add row.Aids

    // We need to recompute cached values each time new contributions arrive.
    nodeData.IsStubSubnode <- ValueNone
    nodeData.NoisyCount <- 0L

  let private updateActualRanges (row: Row) (nodeData: NodeData) =
    nodeData.ActualRanges
    |> Array.iteri (fun index actualRange ->
      let value = row.Values.[nodeData.Context.Combination.[index]]
      nodeData.ActualRanges.[index] <- expandRange value actualRange
    )

  // Removes the low-count half, if one exists, from the specified range.
  let private getHalvedRange (leaf: Leaf) (dimension: int) (range: Range) =
    let valueIndex = leaf.Data.Context.Combination[dimension]
    // IEntityCounter is mutable, so we need to create a new object for each slot.
    let perHalfEntityCounters =
      Array.init 2 (fun _ -> leaf.Data.Context.CountStrategy.CreateEntityCounter())

    leaf.Rows
    |> Seq.iter (fun row ->
      let halfIndex = range.HalfIndex(row.Values[valueIndex])
      perHalfEntityCounters.[halfIndex].Add row.Aids
    )

    let anonParams = leaf.Data.Context.AnonymizationContext.AnonymizationParams

    match
      perHalfEntityCounters.[0].IsLowCount anonParams.Salt anonParams.Suppression,
      perHalfEntityCounters.[1].IsLowCount anonParams.Salt anonParams.Suppression
    with
    | false, true -> range.Half(0)
    | true, false -> range.Half(1)
    | _ -> range

  let getHalvedRanges (node: Node) =
    match node with
    | Leaf leaf ->
      if leaf.Data.IsStub then
        leaf.Data.SnappedRanges |> Array.mapi (getHalvedRange leaf)
      else
        leaf.Data.SnappedRanges
    | Branch branch -> branch.Data.SnappedRanges

  let createLeaf context snappedRanges subnodes initialRow =
    let values = getItemsCombination context.Combination initialRow.Values

    let nodeData =
      {
        Id = getNextNodeId ()
        SnappedRanges = snappedRanges
        Subnodes = subnodes
        IsStub = allSubnodesAreStubs (subnodes)
        ActualRanges = (values, values) ||> Array.map2 createRange
        EntityCounter = context.CountStrategy.CreateEntityCounter()
        Context = context
        IsStubSubnode = ValueNone
        NoisyCount = 0
      }

    updateAids initialRow nodeData
    updateActualRanges initialRow nodeData

    Leaf { Data = nodeData; Rows = MutableList<Row>([ initialRow ]) }

  // Each dimension corresponds to a bit in index, where 0 means the lower range half, and 1 means the upper range half.
  let private findChildIndex (values: float array) (nodeData: NodeData) =
    (0, nodeData.Context.Combination, nodeData.SnappedRanges)
    |||> Array.fold2 (fun index valueIndex range -> (index <<< 1) ||| range.HalfIndex(values.[valueIndex]))

  let private removeDimensionFromIndex position index =
    let lowerMask = (1 <<< position) - 1
    let upperMask = ~~~((1 <<< (position + 1)) - 1)
    // Remove missing position bit from index.
    ((index &&& upperMask) >>> 1) ||| (index &&& lowerMask)

  let private createChildLeaf (childIndex: int) (parent: Branch) (initialRow: Row) =
    // Create child's ranges by halfing parent's ranges, using the corresponding bit in the index to select the correct half.
    let snappedRanges, _ =
      Array.mapFoldBack
        (fun (range: Range) (index: int) ->
          let halfRange = range.Half(index &&& 1)
          halfRange, index >>> 1
        )
        parent.Data.SnappedRanges
        childIndex

    // Set child's subnodes to the matching-range children of the parent's subnodes.
    let subnodes, _ =
      parent.Data.Subnodes
      |> Array.mapFold
        (fun position subnode ->
          let subnode =
            match subnode with
            | ValueSome(Branch subnodeBranch) ->
              match
                childIndex
                |> removeDimensionFromIndex position
                |> subnodeBranch.Children.TryFind
              with
              | Some x -> ValueSome x
              | None -> ValueNone
            | _ -> ValueNone

          subnode, position + 1
        )
        0

    createLeaf parent.Data.Context snappedRanges subnodes initialRow

  let private leafShouldBeSplit depth (leaf: Leaf) =
    let lowThreshold = leaf.Data.Context.AnonymizationContext.AnonymizationParams.Suppression.LowThreshold
    let depthThreshold = leaf.Data.Context.BucketizationParams.PrecisionLimitDepthThreshold

    // `RowLimit` is 0 for nodes above 1dim, no need to check dimensions.
    (depth <= depthThreshold || leaf.Rows.Count >= leaf.Data.Context.RowLimit)
    && (not leaf.Data.IsStub)
    && leaf.Data.ActualRanges |> isSingularityNode |> not
    && dataCrossesLowThreshold lowThreshold leaf.Data

  let rec addRow depth (node: Node) (row: Row) =
    match node with
    | Leaf leaf ->
      updateAids row leaf.Data
      updateActualRanges row leaf.Data

      if leafShouldBeSplit depth leaf then
        // Convert current leaf node into a new branch node and insert previously gathered rows down the tree.
        let branch =
          Branch
            {
              Data =
                { leaf.Data with
                    EntityCounter = leaf.Data.Context.CountStrategy.CreateEntityCounter()
                }
              Children = Map.empty
            }

        leaf.Rows.Add(row)
        Seq.fold (addRow depth) branch leaf.Rows
      else
        leaf.Rows.Add(row)
        Leaf leaf
    | Branch branch ->
      let childIndex = findChildIndex row.Values branch.Data

      let newChild =
        match Map.tryFind childIndex branch.Children with
        | Some child -> addRow (depth + 1) child row
        | None -> createChildLeaf childIndex branch row

      updateAids row branch.Data
      updateActualRanges row branch.Data

      Branch { branch with Children = Map.add childIndex newChild branch.Children }

  // Outliers need special handling. They must:
  //   - be added to existing leaves;
  //   - not change the actual ranges of the nodes.
  let rec private add1DimOutlierRow (node: Node) (row: Row) =
    match node with
    | Leaf leaf ->
      updateAids row leaf.Data
      leaf.Rows.Add(row)
      Leaf leaf
    | Branch branch ->
      let childIndex =
        if branch.Children.Count = 1 then
          branch.Children.Keys |> Seq.head
        else
          assert (branch.Children.Count = 2)
          findChildIndex row.Values branch.Data

      let newChild = add1DimOutlierRow branch.Children.[childIndex] row
      updateAids row branch.Data
      Branch { branch with Children = Map.add childIndex newChild branch.Children }

  let private getLowCountRowsInChild childIndex (branch: Branch) =
    match branch.Children.TryFind childIndex with
    | Some(Leaf leaf) ->
      let lowThreshold = leaf.Data.Context.AnonymizationContext.AnonymizationParams.Suppression.LowThreshold
      if dataCrossesLowThreshold lowThreshold leaf.Data then None else Some leaf.Rows
    | Some _ -> None
    | None -> Some(MutableList<Row>())

  let rec pushDown1DimRoot root =
    match root with
    | Leaf _ -> root
    | Branch branch ->
      match getLowCountRowsInChild 0 branch, getLowCountRowsInChild 1 branch with
      | None, Some rows -> rows |> Seq.fold add1DimOutlierRow (pushDown1DimRoot branch.Children.[0])
      | Some rows, None -> rows |> Seq.fold add1DimOutlierRow (pushDown1DimRoot branch.Children.[1])
      | _ -> root

  // Casts a `Value` to a `float` in order to match it against a `Range`.
  let castValueToFloat (dataConvertor: IDataConvertor) value =
    if value = Null then None else value |> dataConvertor.ToFloat |> Some

  let mapRow (dataConvertors: IDataConvertors) (nullMappings: float array) (row: Value array) =
    let values =
      row
      |> Array.tail
      |> Array.map2 castValueToFloat dataConvertors
      |> Array.map2 Option.defaultValue nullMappings

    let aids =
      match Array.head row with
      | List aids -> aids
      | _ -> failwith "Expecting a list of AIDs as first value in row."

    { Values = values; Aids = aids |> Seq.map hashAid |> Seq.toArray }


type Tree = Tree.Node

let private getSubnodes
  (lowerLevel: int)
  (dimensions: int)
  (upperCombination: Combination)
  (getTree: Combination -> Tree)
  =
  generateCombinations lowerLevel dimensions
  |> Seq.filter (fun lowerCombination -> isSubsetCombination lowerCombination upperCombination)
  |> Seq.map (ValueSome << getTree)
  |> Seq.toArray

let private getActualRanges (dataConvertors: IDataConvertors) (rows: Row array) =
  rows
  |> Array.fold
    (fun boundaries row ->
      row
      |> Array.tail
      |> Array.map2 Tree.castValueToFloat dataConvertors
      |> Array.map2
        (fun boundary currentValue ->
          currentValue
          |> Option.map (fun currentValue ->
            boundary
            |> Option.map (fun (minValue, maxValue) -> min minValue currentValue, max maxValue currentValue)
            |> Option.orElse (Some(currentValue, currentValue))
          )
          |> Option.defaultValue boundary
        )
        boundaries
    )
    (Array.create dataConvertors.Length None)
  |> Array.map (fun boundary -> boundary |> Option.defaultValue (0.0, 0.0) ||> createRange)

let mapNulls range =
  if range.Max > 0 then 2.0 * range.Max
  elif range.Min < 0 then 2.0 * range.Min
  else 1.0

type Forest
  (
    rows: Row array,
    dataConvertors: IDataConvertors,
    anonContext: AnonymizationContext,
    bucketizationParams: BucketizationParams,
    columnNames: string list,
    countStrategy: CountStrategy
  ) =
  let unsafeRandom = Random(0)

  let dimensions = Seq.length dataConvertors
  let columnNames = columnNames

  let nullMappings, snappedRanges =
    rows
    |> getActualRanges dataConvertors
    |> Array.map (fun range ->
      let nullMapping = mapNulls range
      nullMapping, range |> expandRange nullMapping |> snapRange
    )
    |> Array.unzip

  let mappedRows = rows |> Array.map (Tree.mapRow dataConvertors nullMappings)

  let treeCache = Dictionary<Combination, Tree>(LanguagePrimitives.FastGenericEqualityComparer)

  let rec getTree (combination: Combination) =
    treeCache |> Dictionary.getOrInit combination (fun () -> buildTree combination)

  and buildTree (combination: Combination) =
    let level = combination.Length

    // Build lower levels first because 1-Dim trees might mutate `snappedRanges`.
    let subnodes = getSubnodes (level - 1) dimensions combination getTree

    let stopwatch = Diagnostics.Stopwatch.StartNew()

    let combinationColumns = getItemsCombination combination (Array.ofList columnNames)

    let anonContext =
      { anonContext with
          BucketSeed = Hash.strings anonContext.BucketSeed combinationColumns
      }

    let rowLimit =
      if level = 1 then
        Anonymizer.noisyRowLimit
          anonContext.AnonymizationParams.Salt
          anonContext.BucketSeed
          rows.Length
          bucketizationParams.PrecisionLimitRowFraction
      else
        // Required to only limit depth for 1dim trees.
        0

    let treeContext: Tree.Context =
      {
        Combination = combination
        AnonymizationContext = anonContext
        BucketizationParams = bucketizationParams
        RowLimit = rowLimit
        BuildTime = TimeSpan.Zero
        CountStrategy = countStrategy
      }

    // Warning: Do not access `snappedRanges` before 1-Dim trees are built.
    let rootRanges = getItemsCombination combination snappedRanges
    let root = mappedRows |> Array.head |> Tree.createLeaf treeContext rootRanges subnodes

    let tree = mappedRows |> Seq.tail |> Seq.fold (Tree.addRow 0) root

    treeContext.BuildTime <- stopwatch.Elapsed
    tree

  do
    for i = 0 to dimensions - 1 do
      // We need to flatten uni-dimensional trees, by pushing the root down as long as one of its halves
      // fails LCF, and update the corresponding dimension's range, in order to anonymize its boundaries.
      let flattenedTree = [| i |] |> buildTree |> Tree.pushDown1DimRoot
      snappedRanges.[i] <- (Tree.nodeData flattenedTree).SnappedRanges.[0]
      treeCache.Add([| i |], flattenedTree) // Cache the flattened version of the tree.

  // ----------------------------------------------------------------
  // Public interface
  // ----------------------------------------------------------------

  member this.AnonymizationContext = anonContext
  member this.BucketizationParams = bucketizationParams
  member this.Rows = rows
  member this.Dimensions = dimensions
  member this.NullMappings = nullMappings
  member this.DataConvertors = dataConvertors
  member this.Random = unsafeRandom
  member this.ColumnNames = columnNames

  member this.CountStrategy = countStrategy

  member this.TopTree = getTree [| 0 .. dimensions - 1 |]

  member this.GetTree(combination: Combination) =
    let tree = getTree combination

    // To reduce memory usage, drop multi-dimensional trees from the cache between invocations.
    treeCache.Keys
    |> Seq.filter (Array.length >> ((<>) 1))
    |> Seq.iter (treeCache.Remove >> ignore)

    tree

  member this.GetAvailableTrees() =
    treeCache |> Seq.map (fun pair -> pair.Value) |> Seq.toList
