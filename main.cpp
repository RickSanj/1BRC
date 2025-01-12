// This is a personal academic project. Dear PVS-Studio, please check it.
// PVS-Studio Static Code Analyzer for C, C++, C#, and Java: http://www.viva64.com

#include "main.h"

void process_chunk(const std::vector<std::string> &lines,
                   std::mutex &res_queue_mutex, std::queue<std::unordered_map<std::string, StationData>> &res_queue,
                   std::condition_variable &res_cv) {
    std::unordered_map<std::string, StationData> local_map;

    for (const auto &line: lines) {
        std::istringstream ss(line);
        std::string station, temp_str;
        if (std::getline(ss, station, ';') && std::getline(ss, temp_str)) {
            local_map[station].update(std::stod(temp_str));
        }
    }
    {
        std::lock_guard<std::mutex> lock(res_queue_mutex);
        res_queue.push(local_map);
    }
    res_cv.notify_all();
}


void merge_stations(
        std::unordered_map<std::string, StationData> &station_map,
        std::mutex &res_queue_mutex,
        std::queue<std::unordered_map<std::string, StationData>> &res_queue,
        std::condition_variable &cv,
        std::atomic<bool> &done) {
    while (true) {
        std::unordered_map<std::string, StationData> local_map;
        {
            std::unique_lock<std::mutex> lock(res_queue_mutex);
            cv.wait(lock, [&]() { return !res_queue.empty() || done; });

            if (res_queue.empty() && done) break;

            local_map = std::move(res_queue.front());
            res_queue.pop();
        }

        for (const auto &[station, data]: local_map) {
            station_map[station].count += data.count;
            station_map[station].sum += data.sum;
            station_map[station].min = std::min(station_map[station].min, data.min);
            station_map[station].max = std::max(station_map[station].max, data.max);
        }
    }
}


void worker_thread(
        std::queue<std::vector<std::string>> &task_queue,
        std::mutex &queue_mutex,
        std::condition_variable &cv,
        std::mutex &res_queue_mutex,
        std::queue<std::unordered_map<std::string, StationData>> &res_queue,
        std::condition_variable &res_cv) {
    while (true) {
        std::vector<std::string> chunk;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock, [&]() { return !task_queue.empty(); });

            chunk = std::move(task_queue.front());
            task_queue.pop();
        }
        if (chunk.empty()) {
            break;
        }
        process_chunk(chunk, res_queue_mutex, res_queue, res_cv);
    }
}


void print_output(std::unordered_map<std::string, StationData> &station_map, const std::string &output_file) {
    std::vector<std::pair<std::string, StationData>> sorted_data(station_map.begin(), station_map.end());
    std::sort(sorted_data.begin(), sorted_data.end(), [](const auto &a, const auto &b) {
        return a.first < b.first;
    });

    std::ofstream output(output_file);
    if (!output.is_open()) {
        std::cerr << "Error: Could not open output file." << std::endl;
        return;
    }

    output << std::fixed << std::setprecision(1);
    for (const auto &[station, data]: sorted_data) {
        output << station << "=" << data.min << "/" << data.mean() << "/" << data.max << "\n";
    }
    output.close();
}


void read_file_in_chunks(const std::string &input_file, const int &thread_count,
                         std::queue<std::vector<std::string>> &task_queue, size_t &chunk_size, std::mutex &queue_mutex,
                         std::condition_variable &cv) {
    std::ifstream input(input_file, std::ios::binary);

    if (!input.is_open()) {
        std::cerr << "Failed to open the file." << std::endl;
        return;
    }

    const size_t buffer_size = 50 * 1024 * 1024;
    std::vector<char> buffer(buffer_size);
    std::string temp_data;
    std::vector<std::string> chunk(chunk_size);

    while (true) {
        input.read(buffer.data(), buffer_size);
        size_t bytes_read = input.gcount();
        if (bytes_read == 0) {
            break;
        }

        temp_data.append(buffer.data(), bytes_read);

        size_t start_pos = 0;
        while (true) {
            size_t end_pos = temp_data.find('\n', start_pos);

            if (end_pos == std::string::npos) {
                break;
            }

            chunk.emplace_back(temp_data.substr(start_pos, end_pos - start_pos));

            start_pos = end_pos + 1;
        }

        if (start_pos < temp_data.size()) {
            temp_data = temp_data.substr(start_pos);
        } else {
            temp_data.clear();
        }

        if (!chunk.empty()) {
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                task_queue.push(chunk);
            }
            cv.notify_one();
            chunk.clear();
        }
    }

    if (!temp_data.empty()) {
        chunk.push_back(std::move(temp_data));
    }
    if (!chunk.empty()) {
        std::lock_guard<std::mutex> lock(queue_mutex);
        task_queue.push(std::move(chunk));
    }
    cv.notify_one();


    for (int i = 0; i < thread_count; ++i) {
        {
            //emplace empty chunk, it is used as a poison pill
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.emplace();
        }
        cv.notify_all();
    }

    input.close();
}


void process_file(const std::string &input_file, const std::string &output_file, const int &thread_count) {
    std::unordered_map<std::string, StationData> station_map;
    size_t chunk_size = 10000;

    std::vector<std::thread> workers;
    std::queue<std::vector<std::string>> task_queue;
    std::queue<std::unordered_map<std::string, StationData>> res_queue;
    std::mutex queue_mutex;
    std::mutex res_queue_mutex;
    std::mutex map_mutex;
    std::condition_variable cv, res_cv;
    std::atomic<bool> done(false);

    for (int i = 0; i < thread_count; ++i) {
        workers.emplace_back(worker_thread,
                             std::ref(task_queue),
                             std::ref(queue_mutex),
                             std::ref(cv),
                             std::ref(res_queue_mutex),
                             std::ref(res_queue),
                             std::ref(res_cv));
    }
    std::thread merge_thread(merge_stations,
                             std::ref(station_map),
                             std::ref(res_queue_mutex),
                             std::ref(res_queue),
                             std::ref(res_cv),
                             std::ref(done));

    read_file_in_chunks(input_file, thread_count, task_queue, chunk_size, queue_mutex, cv);

    for (auto &worker: workers) {
        worker.join();
    }

    done = true;
    res_cv.notify_all();

    merge_thread.join();

    print_output(station_map, output_file);
}


int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: <input.txt> <output.txt> num_of_threads!\n";
        return 1;
    }

    std::string input = argv[1];
    std::string output = argv[2];
    int num_of_threads = std::stoi(argv[3]);

    auto start_time = get_current_time_fenced();
    process_file(input, output, num_of_threads);
    auto end_time = get_current_time_fenced();

    std::cout << to_s(end_time - start_time) << " milliseconds" << std::endl;
    return 0;
}
