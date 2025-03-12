import matplotlib.pyplot as plt
import os
import pprint
import sys

HASH_BIT_LEVELS = range(1, 19)
THREAD_LEVELS = [2**n for n in range(6)]
ALGORITHM_NAMES = ['ind', 'con']


# Constructs the file name given number of hashbits, threads and algorithm name
def get_file_name(dir, algorithm_name, hash_bit_level, thread_level):
    return f'{dir}/{algorithm_name}_{hash_bit_level}hash_{thread_level}thread.txt'

# Opens a results file and returns the average throughput
def find_average(file):
    if os.path.isfile(file) == False: # If file does not exist, return 0
        return 0
    with open(file, 'r') as f:
        lines = f.readlines()
        if len(lines) > 0:
            sum = 0
            n = 0
            for line in lines:
                if 'Throughput' in line: # Find the lines with throughput
                    n += 1
                    sum += int(line.split(' ')[1])
            return sum / n # Return the average throughput
        return 0


# Parses the output files and returns a dictionary with the results from each algorithm
def parse_output_files(directory):
    results = {}
    for algorithm_name in ALGORITHM_NAMES:
        results[algorithm_name] = {}
        for thread_level in THREAD_LEVELS:
            results[algorithm_name][thread_level] = []
            for hash_bit_level in HASH_BIT_LEVELS:
                file_name = get_file_name(directory, algorithm_name, hash_bit_level, thread_level)
                results[algorithm_name][thread_level].append(find_average(file_name))
    return results

# Takes the results dictionary and graphs thorughput as a function of # of hash bits for each thread level of both algorithms
def make_graphs(results, dir):
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    # Set title of the whole plot to be name of directory
    title = dir.split('/')[-1]
    fig.suptitle(f'{title} Results')

    markers = ['o', 'x', 's', 'D', '^', 'v']
    # Plot for Independent Output Results
    for i, thread_level in enumerate(THREAD_LEVELS):
        axs[0].plot(HASH_BIT_LEVELS, results['ind'][thread_level], label=f'{thread_level} Threads', marker=markers[i])
    axs[0].set_xlabel('Number of Hash Bits')
    axs[0].set_ylabel('Million tuples/s')
    axs[0].set_title('Independent Output Results')
    axs[0].legend()
    axs[0].set_xticks(range(2, 19, 2))

    # Plot for Concurrent Output Results
    for i, thread_level in enumerate(THREAD_LEVELS):
        axs[1].plot(HASH_BIT_LEVELS, results['con'][thread_level], label=f'{thread_level} Threads', marker=markers[i])
    axs[1].set_xlabel('Number of Hash Bits')
    axs[1].set_ylabel('Million tuples/s')
    axs[1].set_title('Concurrent Output Results')
    axs[1].legend()
    axs[1].set_xticks(range(2, 19, 2))

    fig.tight_layout()
    fig.savefig(f'./results/{title}_graphs.png')

def main():
    # Check that the correct number of arguments are passed
    if len(sys.argv) != 2:
        print('Usage: python make_graphs.py <directory>')
        sys.exit()
    
    dir = sys.argv[1]
    # Check that the directory exists
    if os.path.isdir(dir) == False:
        print(f'{dir} is not a directory')
        sys.exit()
    
    # Check that file format in directory is correct using a regex on the first file
    if not os.path.isfile(f'{dir}/ind_1hash_2thread.txt'):
        print(f'Files in {dir} are not in the correct format: <algorithm>_<hash_bits>hash_<threads>thread.txt')
        sys.exit()
    
    # Parse the output files
    results = parse_output_files(dir)
    pprint.pp(results)

    # Make the graphs
    make_graphs(results, dir)

if __name__ == '__main__':
    main()