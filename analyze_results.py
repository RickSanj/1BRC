import sys
import subprocess
import numpy as np


def get_args():
    if len(sys.argv) != 5:
        print("Use: python3 analyze_results.py <input file> <output file> <number of threads> <number_of_runs>")
        sys.exit(1)
    return sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]


def run_program(input, output, threads, num):
    times = []
    for _ in range(int(num)):
        result = subprocess.run(
            ["cmake-build-release/1BRC", input, output, threads], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        res = result.stdout.decode("utf-8").strip()

        time = float(res.split(" ")[0])
        times.append(time)
    return times

def analyze_results(times):
    print(f"Min : {min(times):.4f}")
    print(f"Mean: {np.mean(times):.4f}")
    print(f"Std : {np.std(times, ddof=1):.4f}")
    print("\n")


if __name__ == "__main__":
    input, output, threads, num = get_args()

    p = run_program(input, output, threads, num)
    analyze_results(p)
