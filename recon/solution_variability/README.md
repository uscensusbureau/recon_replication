# Solution Variability

## Main file help text
The tool is executed by running `python3 -m solvar`.

```
usage: solvar [-h] [-d] [-t] [-c CONFIGFILE] -i INPUT -o OUTPUT [--demo] [--age]

Solution Variability

required arguments:
  -i INPUT, --input INPUT
                        Input path in filesystem. All .lp and .lp.gz in all
                        subfolders inside this directory will be considered.
  -o OUTPUT, --output OUTPUT
                        Output path in filesystem. Files will be written here
                        mimicking input directory hierarchy.

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Enable debugging mode.
  -t, --test            Run in testing mode.
  -c CONFIGFILE, --configFile CONFIGFILE
                        Overwrites the default config file: ./config.ini
  --demo                Whether to run demo intrinsic variability analysis.
  --age                 Whether to run age intrinsic variability analysis.

```

## Explanation of flow
- Set the filesystem and other options in `config.ini` (or the file specified by `-c`)
- The `.lp` and `.lp.gz` files from the input directory (specified by `-i`) in the filesystem will be processed. Each file will be downloaded (and unzipped, if required) to the worker node that will process it.
- Then, intrinsic variability (IV) will be computed on each `.lp` file in parallel using Yarn/Spark. Basic IV will always be computed, additionally demo and/or age IV may be computed depending, respectively, on whether `--demo` and `--age` are set.
- The output stats will then be compiled into `ivs.csv` and uploaded to the output directory (specified by `-o`) in the filesystem.
- The output of each run is saved in `${tractId}.log` and uploaded to the output directory in the filesystem while preserving input hierarchy. Those logs are also populated in `workDir`.

## Example run
1. Set the filesystem to `s3`, `sparkMaster` to `yarn`, then `s3Bucket` to `galois-das-dev`.
2. Say the inputs are stored in the bucket in a directory called `input/`. Run:
```bash
python3 -m solvar -i input -o output
```
3. When it finishes, go to the S3 bucket and look for a folder called `output`. That will contain the output files!

Example S3 bucket hierarchy before running:
```
input/
|__ model_11111111111.lp
|__ folder1/
    |__ model_45645645645.lp.gz
    |__ model_99999999999.lp
```

After running:
```
output/
|__ ivs.csv
|__ 11111111111.log
|__ folder1/
    |__ 45645645645.log
    |__ 99999999999.log
```

Note that the log format now has the `${tractId}`. So to inspect a certain model while it's running, you can:
```bash
yarn logs -applicationId <APPID> | grep <tractId>
```
to see all log messages related to that tractId.

## Census LP files
[July 21, 2021]: At the moment, the [`galois-das-dev` S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/galois-das-dev?region=us-west-2&tab=objects) contains Census provided-in-July input files in `input/` and dummy input files (copies of Census provided-in-June) in `input_dummy/`.

## Block Level Models

Block-level models can be extracted from the tract-level ones using the `block_level_rewriter.py` utility. Run as
```bash
python block_level_rewriter.py -t text -i input_models -o output_models [--no-pct]
```
Next, run solvar with the `output_models` as inputs to get the solution variability of the blocks. Note that the rewriter
used is named `text`. A `gurobi` option exists but is not recommended currently (slow).

## Testing solution variability results
The directory `testLPs/` contains scripts to generate simple LP files whose 'ground truth' solutions are manually computed beforehand and defined in the variable `solutions_testLPs` in `solvar/config/solutions_testLPs.py`. These LPs are uploaded to the AWS path `input_test/` and used to test `solvar`. Run `solvar` on them using:
```
python3 -m solvar -t -i input_test -o <some_folder>
```
for both configurations of `useImpreciseAgeBuckets` in `config.ini`, i.e. `Yes` and `No`. This will test if the solution variability computed via the code matches the ground truth results.

For an explanation of the ground truth results and more details, refer to `testLPs/README.md`, in particular, the `Testing solvar` section.

## Type Checks

To run the type checker, run:
```bash
mypy solvar
```

In case you are missing stubs or `mypy` itself, create a python virtual environment, and install all requirements:
```
~$ cd # Let's create the env in home directory
~$ python3 -m venv das-env
~$ . ~/das-env/bin/activate
(das-env) ~$ cd ${STATS_2010}/recon/solution_variability
(das-env) ~$ python3 -m pip install -r requirements.txt
(das-env) ~$ mypy solvar
```
