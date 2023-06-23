# Reference implementation of SynDiffix

## Goals

This is the reference implementation of SynDiffix that should serve as a guide for a fully-featured
integration of the mechanism into other projects.

## Usage

From inside the `src\SynDiffix` folder execute:

```
dotnet run file.csv --columns column0:r column1:i ...
```

`<file.csv>` - Specifies the input CSV file.

`--columns [<string>...]` - The selection of columns to synthesize and their types in the format `name:t`, where `t` is one of the following:
`b`-boolean, `i`-integer, `r`-real, `t`-timestamp, `s`-string.

### Optional arguments

`--aidcolumns [<string>...]` - List of entity ID columns used for anonymization. If not specified,
assumes each row represents a different entity.

`--verbose` - Display extra output for debugging purposes.

#### Anonymization parameters

`--lcf-low-threshold, -lcf <int>` - Low threshold for the low-count filter.

`--range-low-threshold, -range <int>` - Low threshold for a range bucket.

`--singularity-low-threshold, -sing <int>` - Low threshold for a singularity bucket.

`--threshold-sd, -thresh_sd <double>` - Threshold SD for LCF/range/singularity decisions.

`--outlier-count <int> <int>` - Outlier count interval.

`--top-count <int> <int>` - Top values count interval.

`--layer-noise-sd, -noise_sd <double>` - Count SD for each noise layer.

#### Clustering parameters

`--no-clustering` - Disables column clustering.

`--clustering-maincolumn` - Column to be prioritized in clusters.

`--clustering-samplesize <int>` - Table sample size when measuring dependence.

`--clustering-maxsize <int>` - Maximum cluster size.

`--clustering-thresh-merge <double>` - Dependence threshold for combining columns in a cluster.

`--clustering-thresh-patch <double>` - Dependence threshold for making a patch.

#### Precision parameters

`--precision-limit-row-fraction <int>` - Tree nodes are allowed to split if `node_num_rows >= table_num_rows/row_fraction`.

`--precision-limit-depth-threshold <int>` - Tree depth threshold below which the `row-fraction` check is not applied.

## Step-by-step description of the algorithm

All of the following steps, whenever a `count` is mentioned, refer to the `anonymized (noisy) count`
(the `true count` is never used directly).

### Casting to real

Before the forest building is conducted, preliminary casting of all values to reals is done:

- **null** - see [Forest building](#forest-building)
- **boolean** - false becomes 0.0, true becomes 1.0
- **integer** - natural cast to real
- **real** - kept intact (TODO: document behavior for non-finite reals, once it's implemented)
- **string** - a sorted array with all of the distinct values is first created, then each value gets assigned
  its position in the array
- **timestamp** - becomes seconds since 1800-01-01 00:00:00 UTC

### Forest building

There are two types of nodes: branches and leaves. Both kinds store an array of ranges, an array of subnodes, and a stub flag.
A branch has `2^tree_dimensions` children, some of which can be missing. A leaf stores the list of rows matching its ranges.

- For each row:
  - For each column:
    - Compute the range of the values space.
- For each column:
  - Map nulls to a value outside twice the actual min or max boundary,
    to prevent leaking low-count nulls into the bottom or top values range.
  - Snap the range size to the next power-of-two.
  - Align the range boundaries to the snapped size.
- For k in 1..length(columns):
  - For each combination of k columns:
    - Build the tree root for the column combination.
    - For each row:
      - Find target leaf node. If one doesn't exist:
        - Create a new leaf node by halfing the ranges of the parent.
        - Fill the subnodes list from the lower dimensional trees.
          - The subnodes are the children of parent's subnodes with the same corresponding ranges as the current node.
        - Determine if the node is a stub.
          - A node is a stub if all of its subnodes either:
            - are missing;
            - fail the range / singularity threshold;
            - are also stubs themselves.
      - Add the row to its matching leaf node.
      - If target node is not a singularity, passes LCF, is not a stub, and (below depth threshold or over row fraction):
        - Convert the target leaf node to a branch node.
        - Add the previously stored rows to the newly created node.
    - If k = 1:
      - Push the root down as long as one of its children fails LCF.
      - Add the rows from the previously dropped low-count nodes to the remaining tree.
      - Update the snapped range for the corresponding dimension.

### Bucket harvesting

A bucket has an array of ranges (one for each dimension), and a noisy row count.
When creating a bucket from a node, use the node's singularity ranges at its singularity dimensions
and its snapped ranges elsewhere.

- Do a bottom-up traversal of the forest's root. At each node:
  - If the node is a leaf:
    - If the node fails LCF:
      - Return nothing.
    - If the node is a singularity:
      - Return a bucket from node's single value.
    - If in 1-dimensional node:
      - Return a bucket from node's ranges.
    - Else:
      - Return the refined buckets from node's ranges and subnodes.
  - Else:
    - Gather all buckets from child nodes.
    - If there are fewer rows in the children's buckets than half of the current node's rows:
      - If in 1-dimensional node:
        - Return a bucket from node's ranges.
      - Else:
        - Generate `current.count - children.count` refined buckets from node's ranges and subnodes.
        - Append the refined buckets to the previously harvested buckets.
        - Return the resulting buckets.
    - Else:
      - Adjust the counts of the buckets so that their sum matches the count of the current node.
      - Return the resulting buckets.

#### Bucket refinement

- Gather the sub-dimensional buckets. For each possible subnode:
  - If the subnode's combination corresponds to a singularity:
    - Return a bucket with singularity ranges for it.
  - If the subnode exists:
    - Harvest and return the buckets for it.
  - Else:
    - Return an empty list.
- If insufficient buckets for a subnode:
  - Return the input bucket.
- Compute the possible cumulative ranges for each subnode and dimension.
- Compute the smallest ranges for all dimensions.
- Compact smallest ranges by excluding any low-count half-ranges.
- Gather the possible per-dimension ranges which are inside the smallest ranges.
- Drop the sub-dimensional buckets which are outside the smallest ranges.
- If insufficient ranges:
  - Return the input bucket.
- Else:
  - Generate new buckets by matching the remaining possible ranges of a column with
    their corresponding sub-dimensional buckets using a deterministic random shuffle.

### Microdata generation

- For each harvested bucket:
  - For 1..bucket.count:
    - For each range in the bucket:
      - Generate a random float value inside the range.
      - Cast the generated value back to the corresponding column type.
        - In the case of **string** columns:
          - If the range is a singularity:
            - Return the exact value.
          - Else:
            - Return the common prefix of the range, plus `*`, plus a random integer from inside the range.
    - Return a synthetic row from the generated values.
