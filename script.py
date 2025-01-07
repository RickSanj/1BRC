import time
import sys
from collections import defaultdict

class StationData:
    def __init__(self):
        self.count = 0
        self.sum = 0.0
        self.min = float('inf')
        self.max = float('-inf')

    def update(self, temp:float):
        self.count += 1
        self.sum += temp
        if temp < self.min:
            self.min = temp
        if temp > self.max:
            self.max = temp

    def mean(self):
        return self.sum / self.count if self.count != 0 else 0.0

def process_file(file_path, output_file_path):
    station_data = defaultdict(lambda: StationData())

    with open(file_path, 'r', encoding='utf-8') as file:
        for row in file:
            station, temp = row.strip().split(';')
            temp = float(temp)
            station_data[station].update(temp)

    with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
        for station, data in sorted(station_data.items()):
            file.write(f"{station}={data.min:.1f}/{data.mean():.1f}/{data.max:.1f}\n")


if len(sys.argv) != 3:
    print("Usage: python script.py <input_file> <output_file>")
    sys.exit(1)

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]

start_time = time.time()
process_file(input_file_path, output_file_path)
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time:.2f} seconds")