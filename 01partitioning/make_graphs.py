import matplotlib.pyplot as plt

HASH_BIT_LEVELS = [1, 10, 18]
INDEPENDENT_OUTPUT_RESULTS_1 = [305, 122, 123]
INDEPENDENT_OUTPUT_RESULTS_8 = [729, 342, 128]
CONCURRENT_OUTPUT_RESULTS_1 = [174, 91, 45]
CONCURRENT_OUTPUT_RESULTS_8 = [16, 53, 136]

def make_graphs():
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))

    # Plot for Independent Output Results
    axs[0].plot(HASH_BIT_LEVELS, INDEPENDENT_OUTPUT_RESULTS_1, label='1 Thread', marker='o')
    axs[0].plot(HASH_BIT_LEVELS, INDEPENDENT_OUTPUT_RESULTS_8, label='8 Threads', marker='x')
    axs[0].set_xlabel('Number of Hash Bits')
    axs[0].set_ylabel('Million tuples/s')
    axs[0].set_title('Independent Output Results')
    axs[0].legend()
    axs[0].set_xticks(range(2, 19, 2))

    # Plot for Concurrent Output Results
    axs[1].plot(HASH_BIT_LEVELS, CONCURRENT_OUTPUT_RESULTS_1, label='1 Thread', marker='o')
    axs[1].plot(HASH_BIT_LEVELS, CONCURRENT_OUTPUT_RESULTS_8, label='8 Threads', marker='x')
    axs[1].set_xlabel('Number of Hash Bits')
    axs[1].set_ylabel('Million tuples/s')
    axs[1].set_title('Concurrent Output Results')
    axs[1].legend()
    axs[1].set_xticks(range(2, 19, 2))

    fig.tight_layout()
    fig.savefig('./graphs.png')
    return

def main():
    make_graphs()

if __name__ == '__main__':
    main()