# Computer Systems Performance: Partitioning Algorithms

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

## Course Context

This paper fits into the course of Computer Systems Performance, given that threads and shared resources were found to have a direct effect on overall performance. The choice of partitioning technique should therefore ideally have high performance, and the goal of this assignment is to design and run experiments that compare performance for two of these techniques.
