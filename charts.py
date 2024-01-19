import matplotlib.pyplot as plt
import numpy as np


if __name__ == "__main__":
    # empty
    # simple_seq = [1, 1]
    # complex_seq = [1, 1]
    # complex_para = [1, 1]

    # small dataset
    simple_seq = [9, 11, 9, 9, 10, 10, 9, 10, 12, 11]
    complex_seq = [25, 32, 25, 27, 33, 25, 30, 26, 28, 31]
    complex_para = [23, 23, 23, 24, 26, 22, 26, 23, 23, 25]

    # large dataset
    # simple_seq = [164, 166, 179, 220, 213, 192, 173, 208, 199, 171]
    # complex_seq = [227, 218, 205, 241, 215, 233, 235, 236, 224, 230]
    # complex_para = [223, 220, 243, 218, 226, 236, 217, 221, 230, 229]

    N = 10
    x = [i for i in range(N)]
    simple_seq_avg = [np.average(simple_seq) for _ in range(N)]
    complex_seq_avg = [np.average(complex_seq) for _ in range(N)]
    complex_para_avg = [np.average(complex_para) for _ in range(N)]

    plt.plot(x, simple_seq, 'c', label="DAG 1", linestyle="-")
    plt.plot(x, complex_seq, 'r', label="DAG 2", linestyle="-")
    plt.plot(x, complex_para, 'b', label="DAG 3", linestyle="-")

    plt.plot(x, simple_seq_avg, 'c', label="DAG 1 AVG", linestyle="--")
    plt.plot(x, complex_seq_avg, 'r', label="DAG 2 AVG", linestyle="--")
    plt.plot(x, complex_para_avg, 'b', label="DAG 3 AVG", linestyle="--")

    plt.title("Comparison of DAGs Execution Time")
    plt.xlabel("Iteration")
    plt.ylabel("Execution Time [sec]")

    plt.legend()
    plt.show()


if __name__ == "__main__":
    # D1 - small
    # time_periods = ['T0', 'T1']
    # q_start = [0, 5]
    # q_end = [2, 7]
    # end = [3, 8]
    # bar_width = 0.1

    # D2 - small
    # time_periods = ['T0', 'T1', 'T2', 'T3', 'T4', 'T5']
    # q_start = [0, 5, 9, 13, 17, 21]
    # q_end = [2, 6, 11, 15, 19, 23]
    # end = [3, 7, 11.5, 15.5, 19.5, 23.5]
    # bar_width = 0.5

    # D3 - small
    # time_periods = ['T0', 'T1', 'T2', 'T3', 'T4', 'T5']
    # q_start = [0, 5, 10, 10, 10, 20]
    # q_end = [2, 7, 12, 15, 17, 22]
    # end = [3, 8, 12.5, 15.5, 18, 22.5]
    # bar_width = 0.5

    # D1 - large
    # time_periods = ['T0', 'T1']
    # q_start = [0, 57]
    # q_end = [4, 60]
    # end = [52, 100]
    # bar_width = 0.1

    # D2 - large
    time_periods = ['T0', 'T1', 'T2', 'T3', 'T4', 'T5']
    q_start = [0, 56, 101, 140, 182, 223]
    q_end = [2, 58, 103, 143, 184, 225]
    end = [51, 98, 136, 178, 219, 225.5]
    bar_width = 0.5

    # D3 - large
    # time_periods = ['T0', 'T1', 'T2', 'T3', 'T4', 'T5']
    # q_start = [0, 57, 101, 101, 101, 219]
    # q_end = [2, 59, 103, 139, 175, 221]
    # end = [52, 97, 136, 171, 213, 221.5]
    # bar_width = 0.5

    fig, ax = plt.subplots()


    bars3 = ax.barh(time_periods, end, label='Run Duration', color='green', height=bar_width)
    bars2 = ax.barh(time_periods, q_end, label='Time in Queue', color='grey', height=bar_width)
    bars1 = ax.barh(time_periods, q_start, color='white', height=bar_width)

    ax.set_xlabel('Time [sec]')
    ax.set_ylabel('Task')
    ax.set_title('Time in Queue and Run Duration of DAG Tasks')

    ax.legend()
    plt.show()
