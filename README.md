# TSFEFS
Time Series Feature Engineering File System for<br>
 - GreekCaptures
 - GreekEssentiates

## More about TSFEFS
Using Cloud database is expensive, in terms of the storage cost of the data, as well as the massive operations.<br>
TSFEFS enables all the data to be stored as files, provided with easy time-series accesses for read and write.<br>

On top of TSFEFS, <br>
SafeTSFEFS is a wrapper class of TSFEFS that provides easy access of TSFEFS without having the programmer managing commands.<br>
It is easy because every single execution of SafeTSFEFS is followed by all the operations that will make the execution "safer", <br>
which makes it sub-optimal, slow, but safe.<br>

### Features
 - Import (export) from (to) dataframe, src (dst) file, src (dst) folder.
 - Supported format: csv, TSFEFS (i.e., TSFEFS in TSFEFS).
 - Caching: opening a .tsfefs file don't need everything in memory.
 - Optimize: reorganize the file sizes more evenly.
 - Delayed action: not everytime updating and saving are neccessary, although using SafeTSFEFS will do them all.


## Getting Started

### Dependencies
Python 3.10.5<br>
pandas==2.0.1<br>
numpy==1.24.3<br>

### Installing
Tentatively will make it an installable package.

### Imaging
Tentatively will make it a Docker image.

### Executing program
Look at the examples in ./test TSFEFS/ for
 - faster execution
 - more programmer judgement
Look at the examples in ./test SafeTSFEFS/ for
 - no brainer
 - slow execution

## Authors
Contributors names and contact info
Henry Leung henry.leung@greekisgood.com


## Version History
* 0.1
    * Initial Release

## License

This project is licensed under the https://github.com/GreekIsGood/TSFEFS/blob/main/LICENSE - see the LICENSE.md file for details



## Acknowledgments
All from scratch.
