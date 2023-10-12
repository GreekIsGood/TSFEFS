# TSFEFS
Time Series Feature Engineering File System.

## More about TSFEFS
Using Cloud database is expensive, in terms of the storage cost of the data, as well as the massive operations.
TSFEFS enables all the data to be stored as files, provided with easy time-series accesses for read and write.
On top of TSFEFS, SafeTSFEFS is a wrapper class of TSFEFS that provides easy access of TSFEFS without having the programmer managing commands. 
It is because every single execution of SafeTSFEFS includes all the subsequent potential "safe" operations, which is sub-optimal and slow, but safe.

### Features
 - Import (export) from (to) dataframe, src (dst) file, src (dst) folder.
 - Supported format: csv, TSFEFS (i.e., TSFEFS in TSFEFS).
 - Caching: opening a .tsfefs file don't need everything in memory.
 - Optimize: reorganize the file sizes more evenly.
 - Delayed action: not everytime updating and saving are neccessary, although using SafeTSFEFS will do them all.


## Getting Started

### Dependencies
Python 3.10.5
pandas==2.0.1
numpy==1.24.3

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

This project is licensed under the [NAME HERE] License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
