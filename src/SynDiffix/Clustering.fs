module SynDiffix.Clustering

open System
open FsToolkit.ErrorHandling

open SynDiffix.Combination
open SynDiffix.Forest

module Sampling =
  let shouldSample (forest: Forest) =
    let dimensions = forest.Dimensions
    let numRows = forest.Rows.Length
    let numSamples = forest.BucketizationParams.ClusteringTableSampleSize

    if numSamples >= numRows then
      false
    else
      let sampled2dimWork = dimensions * dimensions * numSamples
      let full2dimWork = (numRows * dimensions * 3) / 2

      let totalWorkWithSample = sampled2dimWork + full2dimWork
      let totalWorkWithoutSample = dimensions * dimensions * numRows

      totalWorkWithoutSample > totalWorkWithSample * 2

  let sampleForest (forest: Forest) =
    let samplingAnonContext =
      { forest.AnonymizationContext with
          AnonymizationParams =
            { forest.AnonymizationContext.AnonymizationParams with
                Suppression = { LowThreshold = 2; LowMeanGap = 1; LayerSD = 0.5 }
                LayerNoiseSD = 0.
            }
      }

    // New RNG prevents `forest.Random` from being affected by sample size.
    let random = Random(forest.Random.Next())

    let origRows = forest.Rows
    let numSamples = forest.BucketizationParams.ClusteringTableSampleSize

    Forest(
      Array.init numSamples (fun _ -> origRows.[random.Next(origRows.Length)]),
      forest.DataConvertors,
      samplingAnonContext,
      forest.BucketizationParams,
      forest.ColumnNames,
      forest.CountStrategy
    )


module Dependence =
  // Aliases to help tell what nodes functions expect.
  type private OneDimNode = Tree.Node
  type private TwoDimNode = Tree.Node
  type private AnyDimNode = Tree.Node

  type Score =
    {
      Score: float
      Count: float
      NodeXY: TwoDimNode option
      NodeX: OneDimNode
      NodeY: OneDimNode
    }

  type Result =
    {
      Columns: int * int
      DependenceXY: float
      DependenceYX: float
      Scores: Score list
      MeasureTime: TimeSpan
    }

  let private count (node: AnyDimNode) = node |> Tree.noisyRowCount |> float

  let private getChild index (node: AnyDimNode) =
    match node with
    | Tree.Branch branch -> branch.Children |> Map.tryFind index
    | Tree.Leaf _ -> None

  let private findChild predicate (node: AnyDimNode) =
    match node with
    | Tree.Branch branch ->
      branch.Children
      |> Seq.filter predicate
      |> Seq.toList
      |> function
        | [ child ] -> Some child.Value
        | [] -> None
        | _ -> failwith "Expected to match a single child node."
    | Tree.Leaf _ -> None

  let inline private getBit index (value: int) = (value >>> index) &&& 1

  let private isSingularity (node: OneDimNode) =
    (Tree.nodeData node).ActualRanges.[0].IsSingularity()

  let private singularityValue (node: OneDimNode) =
    (Tree.nodeData node).ActualRanges.[0].Min

  let private snappedRange index (node: OneDimNode) =
    (Tree.nodeData node).SnappedRanges.[index]

  let measure (colX: int) (colY: int) (forest: Forest) : Result =
    // Ensure colX < colY.
    if colX >= colY then
      failwith "Invalid input."

    let numRows = float forest.Rows.Length
    let rangeThresh = forest.BucketizationParams.RangeLowThreshold

    let mustPassRangeThresh (x: float) =
      if x < rangeThresh then None else Some x

    // Walk state
    let scores = MutableList<Score>()
    let mutable dataLossXY = 0.0
    let mutable dataLossYX = 0.0
    let mutable totalActualCount = 0.0

    let rec walk (nodeXY: TwoDimNode option) (nodeX: OneDimNode) (nodeY: OneDimNode) =
      option {
        // Stop walk if either node has `count < range_thresh`.
        let! countX = nodeX |> count |> mustPassRangeThresh
        let! countY = nodeY |> count |> mustPassRangeThresh

        // Compute and store score.
        let actual2dimCount = nodeXY |> Option.map count |> Option.defaultValue 0.
        let expected2dimCount = (countX * countY) / numRows
        totalActualCount <- totalActualCount + actual2dimCount

        let score =
          (abs (expected2dimCount - actual2dimCount))
          / (max expected2dimCount actual2dimCount)

        scores.Add(
          {
            Score = score
            Count = expected2dimCount
            NodeXY = nodeXY
            NodeX = nodeX
            NodeY = nodeY
          }
        )

        // Walk children.
        // Dim 0 (X) is at bit 1.
        // Dim 1 (Y) is at bit 0.
        if isSingularity nodeX && isSingularity nodeY then
          // Stop walk if both 1-dims are singularities.
          ()
        elif isSingularity nodeX then
          dataLossXY <- dataLossXY + actual2dimCount
          let xSingularity = singularityValue nodeX

          for idChildY in 0..1 do
            nodeY
            |> getChild idChildY
            |> Option.iter (fun childY ->
              // Find child that matches on dim Y. It can be on either side of dim X.
              let childXY =
                nodeXY
                |> Option.bind (
                  findChild (fun pair ->
                    (pair.Key |> getBit 0) = idChildY
                    && (snappedRange 0 pair.Value).ContainsValue(xSingularity)
                  )
                )

              walk childXY nodeX childY
            )
        elif isSingularity nodeY then
          dataLossYX <- dataLossYX + actual2dimCount
          let ySingularity = singularityValue nodeY

          for idChildX in 0..1 do
            nodeX
            |> getChild idChildX
            |> Option.iter (fun childX ->
              // Find child that matches on dim X. It can be on either side of dim Y.
              let childXY =
                nodeXY
                |> Option.bind (
                  findChild (fun pair ->
                    (pair.Key |> getBit 1) = idChildX
                    && (snappedRange 1 pair.Value).ContainsValue(ySingularity)
                  )
                )

              walk childXY childX nodeY
            )
        else
          for idChildXY in 0..3 do
            let idChildX = idChildXY |> getBit 1
            let idChildY = idChildXY |> getBit 0

            option {
              let! childX = nodeX |> getChild idChildX
              let! childY = nodeY |> getChild idChildY
              let childXY = nodeXY |> Option.bind (getChild idChildXY)
              walk childXY childX childY
            }
            |> ignore
      }
      |> ignore

    let rootXY = forest.GetTree([| colX; colY |])
    let rootX = forest.GetTree([| colX |])
    let rootY = forest.GetTree([| colY |])

    let stopwatch = Diagnostics.Stopwatch.StartNew()

    walk (Some rootXY) rootX rootY

    let totalWeightedScore, totalCount =
      if scores.Count > 1 then
        scores
        |> Seq.tail // Skip root measurement.
        |> Seq.fold
          (fun (accScore, accCount) score -> (accScore + score.Score * score.Count, accCount + score.Count))
          (0., 0.)
      else
        0., 0.

    let averageScore = if totalCount > 0. then totalWeightedScore / totalCount else 0.

    let dependenceXY, dependenceYX =
      if totalActualCount > 0.0 then
        totalActualCount <- totalActualCount - (count rootXY) // Skip root measurement.

        let scaledScore loss =
          if loss = 0.0 then averageScore
          elif loss >= totalActualCount then 0.0
          else averageScore * (1.0 - (loss / totalActualCount))

        scaledScore dataLossYX, scaledScore dataLossXY
      else
        averageScore, averageScore

    {
      Columns = colX, colY
      DependenceXY = dependenceXY
      DependenceYX = dependenceYX
      Scores = Seq.toList scores
      MeasureTime = stopwatch.Elapsed
    }

  let measureAll (forest: Forest) : float[,] =
    let numColumns = forest.Dimensions
    let dependencyMatrix = Array2D.create<float> numColumns numColumns 1.0

    generateCombinations 2 numColumns
    |> Seq.iter (fun comb ->
      let x, y = comb[0], comb[1]
      let score = (forest |> measure x y)
      dependencyMatrix.[x, y] <- score.DependenceXY
      dependencyMatrix.[y, x] <- score.DependenceYX
    )

    dependencyMatrix

module Clustering =
  type ColumnId = int // Global index of a column.
  type private ColumnIndex = int // Local index of a column in a micro-table.

  type Cluster =
    | BaseCluster of columns: ColumnId list
    | CompositeCluster of stitchColumns: ColumnId list * clusters: Cluster list

  type PatchList = Cluster list

  type TreeMaterializer = Forest -> Combination -> Row seq

  let private shuffleRows (random: Random) (rows: Row array) : Row array =
    let mutable i = rows.Length - 1

    while i > 0 do
      let j = random.Next(i + 1)
      let temp = rows.[i]
      rows.[i] <- rows.[j]
      rows.[j] <- temp
      i <- i - 1

    rows

  let private sortRowsBy (columns: int array) (rows: Row array) : Row array =
    let comparer = comparer Ascending NullsLast
    let columns = Array.toList columns

    let rec compare remainingColumns (rowA: Row) (rowB: Row) =
      match remainingColumns with
      | [] -> 0
      | column :: tail ->
        let valueA = rowA.[column]
        let valueB = rowB.[column]

        match comparer valueA valueB with
        | 0 -> compare tail rowA rowB
        | order -> order

    rows |> Array.sortWith (compare columns)

  let private averageLength microTables =
    microTables |> List.averageBy (fst >> Array.length >> float) |> round |> int

  let private alignLength (random: Random) length (microTable: Row array) =
    if length = microTable.Length then
      microTable
    elif length < microTable.Length then
      Array.truncate length microTable
    else
      let randomSubset =
        Array.init (length - microTable.Length) (fun _ -> microTable[random.Next(microTable.Length)])

      Array.concat [ microTable; randomSubset ]

  let private findIndexes subset superset =
    subset |> Array.map (fun c -> Array.findIndex ((=) c) superset)

  let private findIndexesExcept subset superset =
    superset
    |> Array.mapi (fun i c -> if subset |> Array.contains c then None else Some i)
    |> Array.choose id

  let private stitchMicroTables
    (random: Random)
    (stitchColumns: ColumnId array)
    (left: Row array, leftColumns: ColumnId array)
    (right: Row array, rightColumns: ColumnId array)
    =
    assert (left.Length = right.Length)

    let leftStitchIndexes = leftColumns |> findIndexes stitchColumns
    let leftDataIndexes = leftColumns |> findIndexesExcept stitchColumns

    let rightStitchIndexes = rightColumns |> findIndexes stitchColumns
    let rightDataIndexes = rightColumns |> findIndexesExcept stitchColumns

    (left |> shuffleRows random |> sortRowsBy leftStitchIndexes,
     right |> shuffleRows random |> sortRowsBy rightStitchIndexes)
    ||> Array.mapi2 (fun i leftRow rightRow ->
      let sharedData =
        if i % 2 = 0 then
          leftRow |> getItemsCombination leftStitchIndexes
        else
          rightRow |> getItemsCombination rightStitchIndexes

      let leftData = leftRow |> getItemsCombination leftDataIndexes
      let rightData = rightRow |> getItemsCombination rightDataIndexes

      Array.concat [ sharedData; leftData; rightData ]
    ),
    // leftColumns and rightColumns include stitchColumns, so we need to deduplicate.
    Array.concat [ stitchColumns; leftColumns; rightColumns ] |> Array.distinct

  let private patchMicroTables (random: Random) (microTables: List<Row array * ColumnId array>) =
    let allColumns =
      microTables
      |> Seq.indexed
      |> Seq.collect (fun (tableIndex, (_rows, columns)) ->
        columns
        |> Array.mapi (fun columnIndex columnId -> (tableIndex, columnIndex, columnId))
      )
      |> Seq.sortBy thd3
      |> Seq.toArray

    let avgLength = averageLength microTables

    let rows =
      microTables
      |> List.map (fst >> alignLength random avgLength >> shuffleRows random)
      |> List.toArray

    Array.init
      avgLength
      (fun rowIndex ->
        allColumns
        |> Array.map (fun (tableIndex, columnIndex, _columnId) -> rows.[tableIndex].[rowIndex].[columnIndex])
      ),
    allColumns |> Array.map thd3

  let rec private materializeCluster (materializeTree: TreeMaterializer) (forest: Forest) (cluster: Cluster) =
    match cluster with
    | BaseCluster columns ->
      let combination = List.toArray columns
      Array.sortInPlace combination
      materializeTree forest combination |> Seq.toArray, combination

    | CompositeCluster(stitchColumns, clusters) ->
      let stitchColumns = List.toArray stitchColumns
      let microTables = clusters |> List.map (materializeCluster materializeTree forest)
      let avgLength = averageLength microTables

      microTables
      |> List.map (fun (microTable, columns) -> microTable |> alignLength forest.Random avgLength, columns)
      |> List.reduce (fun acc next -> stitchMicroTables forest.Random stitchColumns acc next)

  let buildTable (materializeTree: TreeMaterializer) (forest: Forest) (patchList: PatchList) =
    patchList
    |> List.map (materializeCluster materializeTree forest)
    |> patchMicroTables forest.Random

module Solver =
  open Clustering

  type private JoinTree = { JoinColumn: ColumnId; Children: JoinTree list }

  type ClusteringContext =
    {
      DependencyMatrix: float[,]
      MainColumn: ColumnId option
      AnonymizationParams: AnonymizationParams
      BucketizationParams: BucketizationParams
      Random: Random
    }
    member this.NumColumns = this.DependencyMatrix.GetLength(0)

  type private MutableCluster = MutableList<ColumnId>
  type private MutablePatch = MutableList<MutableCluster>

  let private buildClusters (context: ClusteringContext) (permutation: int array) : PatchList =
    let numColumns = context.NumColumns
    let mainColumn = context.MainColumn
    let dependencyMatrix = context.DependencyMatrix

    let bestPair x y =
      max dependencyMatrix.[x, y] dependencyMatrix.[y, x]

    let averagePair x y =
      (dependencyMatrix.[x, y] + dependencyMatrix.[y, x]) / 2.0

    let maxClusterSize = context.BucketizationParams.ClusteringMaxClusterSize
    let mergeThreshold = context.BucketizationParams.ClusteringMergeThreshold
    let patchThreshold = context.BucketizationParams.ClusteringPatchThreshold

    let patches = MutableList<MutablePatch>()
    let roots = MutableList<ColumnId>() // Starting column of each patch.
    let patchesByColumn = Array.create numColumns -1
    let clustersByStitchColumn = Array.init numColumns (fun _ -> MutableList<MutableCluster>())

    let findBestAvailableCluster newCol =
      let mutable bestPatchIndex = -1
      let mutable bestScore = 0.0
      let mutable bestCluster = null

      for patchIndex = 0 to patches.Count - 1 do
        for cluster in patches.[patchIndex] do
          if
            cluster.Count < maxClusterSize
            && cluster |> Seq.forall (fun col -> bestPair col newCol >= mergeThreshold)
            && (cluster.Count = 1 || bestPair cluster.[0] cluster.[1] >= mergeThreshold)
          then
            let mutable avg = 0.0

            for col in cluster do
              avg <- avg + averagePair col newCol

            avg <- avg / float cluster.Count

            // Find where the column can be derived best.
            if avg > bestScore then
              bestPatchIndex <- patchIndex
              bestScore <- avg
              bestCluster <- cluster

      if bestPatchIndex >= 0 then Some(bestPatchIndex, bestCluster) else None

    let findBestJoinColumn i =
      let colA = permutation.[i]

      if mainColumn.IsSome then
        if mainColumn.Value = colA then None else mainColumn
      else
        let mutable bestScore = -infinity
        let mutable bestColB = -1

        for j = 0 to i - 1 do
          let colB = permutation.[j]
          let scoreAB = dependencyMatrix.[colA, colB]

          if scoreAB > bestScore then
            bestScore <- scoreAB
            bestColB <- colB

        if bestScore >= patchThreshold then Some bestColB else None

    for i = 0 to numColumns - 1 do
      let col = permutation.[i]

      match findBestAvailableCluster col with
      | Some(patchIndex, cluster) ->
        // Found some available cluster, merge there.
        cluster.Add(col)
        patchesByColumn.[col] <- patchIndex
      | None ->
        match findBestJoinColumn i with
        | Some bestCol ->
          // Add a new cluster by stitching with best column.
          let patchIndex = patchesByColumn.[bestCol]
          let patch = patches.[patchIndex]
          let cluster = MutableCluster()
          cluster.Add(bestCol)
          cluster.Add(col)
          patch.Add(cluster)
          patchesByColumn.[col] <- patchIndex
          clustersByStitchColumn.[bestCol].Add(cluster)
        | None ->
          // No best column is found, start a new patch.
          roots.Add(col)
          let patch = MutablePatch()
          let cluster = MutableCluster()
          cluster.Add(col)
          patches.Add(patch)
          patch.Add(cluster)
          patchesByColumn.[col] <- patches.Count - 1
          clustersByStitchColumn.[col].Add(cluster)

    let rec buildCluster rootCol : Cluster option =
      let childClusters =
        if clustersByStitchColumn.[rootCol].Count > 1 then
          // There might be a patch without merges.
          // Since we have multiple clusters, we drop the single column one.
          // TODO: Reconsider when trying the idea of "patch-stitches".
          clustersByStitchColumn.[rootCol]
          |> Seq.filter (fun cluster -> cluster.Count > 1)
          |> Seq.toList
        else
          clustersByStitchColumn.[rootCol] |> Seq.toList

      childClusters
      |> List.map (fun childCluster ->
        childCluster
        |> Seq.filter (fun col -> col <> rootCol)
        |> Seq.fold
          (fun currentCluster col ->
            match buildCluster col with
            | Some(CompositeCluster([ col' ], otherClusters)) when col = col' ->
              CompositeCluster([ col ], currentCluster :: otherClusters)
            | Some otherCluster -> CompositeCluster([ col ], [ currentCluster; otherCluster ])
            | None -> currentCluster
          )
          (BaseCluster(Seq.toList childCluster))
      )
      |> function
        | [] -> None
        | [ cluster ] -> Some cluster
        | clusters -> Some(CompositeCluster([ rootCol ], clusters))

    roots |> Seq.choose buildCluster |> Seq.toList

  let private clusteringQuality (context: ClusteringContext) (patches: PatchList) =
    let dependencyMatrix = context.DependencyMatrix
    let scoreMatrix = Array2D.copy dependencyMatrix
    let numColumns = context.NumColumns

    let markVisited a b =
      scoreMatrix.[a, b] <- 0.0
      scoreMatrix.[b, a] <- 0.0

    let rec walk (cluster: Cluster) =
      match cluster with
      | BaseCluster baseCluster ->
        let arr = List.toArray baseCluster

        for i = 0 to arr.Length - 1 do
          for j = 0 to i - 1 do
            markVisited arr.[i] arr.[j]
      | CompositeCluster(_, clusters) -> clusters |> List.iter walk

    patches |> List.iter walk

    let mutable sum = 0.0

    // Score is total unsatisfied dependence between columns.
    for i = 0 to numColumns - 1 do
      for j = i + 1 to numColumns - 1 do
        sum <- sum + (max scoreMatrix.[i, j] scoreMatrix.[j, i])

    // Take the average score per column.
    sum / float numColumns

  let clusteringContext mainColumn (forest: Forest) =
    {
      DependencyMatrix = Dependence.measureAll forest
      MainColumn = mainColumn
      AnonymizationParams = forest.AnonymizationContext.AnonymizationParams
      BucketizationParams = forest.BucketizationParams
      Random = forest.Random
    }

  let private moveToFront arr elem =
    match Array.partition (fun x -> x = elem) arr with
    | [| elem |], rest -> Array.append [| elem |] rest
    | _, _ -> arr

  let private doSolve (context: ClusteringContext) =
    let numCols = context.NumColumns
    let random = context.Random
    let minMutationIndex = if context.MainColumn.IsSome then 1 else 0

    let mutate (solution: int array) =
      let copy = Array.copy solution
      let i = random.Next(minMutationIndex, numCols)
      let mutable j = random.Next(minMutationIndex, numCols)

      while i = j do
        j <- random.Next(minMutationIndex, numCols)

      copy.[i] <- solution.[j]
      copy.[j] <- solution.[i]
      copy

    let evaluate (solution: int array) =
      let patches = buildClusters context solution
      clusteringQuality context patches

    // Constants
    let initialSolution =
      if context.MainColumn.IsSome then
        moveToFront [| 0 .. numCols - 1 |] context.MainColumn.Value
      else
        [| 0 .. numCols - 1 |]

    let initialTemperature = 5.0
    let minTemperature = 3.5E-3
    let alpha = 1.5E-3

    let nextTemperature currentTemp =
      currentTemp / (1.0 + alpha * currentTemp)

    // Solver state
    let mutable currentSolution = initialSolution
    let mutable currentEnergy = evaluate initialSolution
    let mutable bestSolution = initialSolution
    let mutable bestEnergy = currentEnergy
    let mutable temperature = initialTemperature

    // Simulated annealing loop
    while bestEnergy > 0 && temperature > minTemperature do
      let newSolution = mutate currentSolution
      let newEnergy = evaluate newSolution
      let energyDelta = newEnergy - currentEnergy

      if energyDelta <= 0.0 || Math.Exp(-energyDelta / temperature) > random.NextDouble() then
        currentSolution <- newSolution
        currentEnergy <- newEnergy

      if currentEnergy < bestEnergy then
        bestSolution <- currentSolution
        bestEnergy <- currentEnergy

      temperature <- nextTemperature temperature

    buildClusters context bestSolution

  let solve (context: ClusteringContext) =
    assert (context.BucketizationParams.ClusteringMaxClusterSize > 1)

    let numCols = context.NumColumns
    let searchCols = numCols - (if context.MainColumn.IsSome then 1 else 0)

    if searchCols >= 2 then
      // TODO: Do an exact search up to a number of columns.
      doSolve context
    else
      // Build a base cluster that includes everything.
      [ BaseCluster [ 0 .. numCols - 1 ] ]
