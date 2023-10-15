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
1. Look at the examples in ./1. test TSFEFS/ for <br>
 - faster execution <br>
 - more programmer judgement <br>
<br>
2. Look at the examples in ./2. test SafeTSFEFS/ for <br>
 - no brainer <br>
 - slow execution <br>
<br>
3. For the admin, TSFEFS was tested with real data (Gold historical price).<br>
Such data is not in github, users are suggested to test their own real data <br>
by reusing the procedures in ./3. test RealData/ <br>


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
