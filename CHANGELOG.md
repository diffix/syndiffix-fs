## Changelog

### Version 1.1.0

- Added `syndiffix.py` python wrapper for ML feature selection.
- Lowered default thresholds for range and singularity nodes to 15 and 5.
- Improved clustering algorithm for main column.
- Added `--output` (`-o`) CLI argument to directly save the CSV file to disk.
- Added `--clustering-mainfeatures <features>` CLI argument to specify main column's ML features.
- Added `--clusters <clusters>` CLI argument which allows defining clusters manually.

### Version 1.0.2

- Fixed a bug in the computation of low-count/range/singularity thresholds' mean.

### Version 1.0.1

- Fixed conversion bug when generating boolean microdata.
