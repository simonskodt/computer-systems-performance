# Computer Systems Performance: Partitiong Algorithms

The task in this assignment is to reproduce two of the four partitioning algorithms described in the paper titled "Data Partitioning on Chip Multiprocessors." The purpose of the paper is to outline some partitioning strategies that are used in databases to parallelize computation in chip multiprocessors (CMPs).

## Partitioning Techniques

The paper focuses solely on hash-based partitioning, not taking range or tag-aware strategies into account. Hash-based partitioning distributes data evenly across different shards using the hash of the shard key. The pros of this technique are the uniform distribution of the data giving a more consistent performance, while the cons are that data locality is lost, and range-based queries will by design be less efficient, as we have to inspect all partitions.

The four hash-based techniques that are mentioned in the paper are shown in the table below:

| Technique | How it Works |
|-----------|--------------|
| Independent | Each thread processes its own partition independently without coordination |
| Concurrent | Threads work simultaneously on partitions with synchronization mechanisms |
| Count-then-move | First counts elements, then moves them to final positions in two phases |
| Parallel Buffers | Uses separate buffer spaces for each thread to avoid conflicts |

> [!NOTE]
> We choose to implement the independent and the concurrent output.

## How to Run

To compile the project: `make`

To compile and run the project: `scripts/run.sh` with additional flags (or use the `./partition` file).

Usage: `./partition <algorithm> <n_tuples> <n_hash_bits> [n_threads]`
- `algorithm`:   'ind' for independent or 'con' for concurrent
- `n_hash_bits`: number of hash bits to use
- `n_threads`:   number of threads to use
- `n_tuples`:    number of tuples to partition (optional, default 2^(24))

> [!TIP]
> The `n_tuples` argument is optional. If not provided, it defaults to 2^(24).

Examples:
```sh
# From project root
scripts/run.sh ind 8 4
scripts/run.sh con 8 4

# From scripts folder
cd scripts
./run.sh ind 8 4
cd ..
```

### Setting Affinity

To compile and run the project with the affinity flag: scripts/run.sh -a or scripts/run.sh --affinity

Usage: `./partition [-a|--affinity] <algorithm> <n_hash_bits> <n_threads>  [n_tuples]`

`-a` or `--affinity`: optional flag to enable thread affinity

Examples:
```sh
scripts/run.sh -a ind 8 4         # method 1
scripts/run.sh --affinity con 8 4 # method 2
```

### Script: `run-server.sh`

To run the server script: `.scripts/run-server.sh`. It generates a `/out` folder with many different runs for both the independent and concurrent output.

Example:
```sh
# From the project root
scripts/run-server.sh
```

> [!IMPORTANT]
> In the `run-server.sh` script, outcomment the perf command to obtain extra information on the running algorithm. 

## Course Context

We examine how threads and shared resources impact computer system performance. Since partitioning techniques affect performance directly, we will test and compare two different approaches through targeted experiments.