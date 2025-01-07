#ifndef INC_1BRC_MAIN_H
#define INC_1BRC_MAIN_H

#include "options_parser.h"
#include <iostream>
#include <chrono>
#include <mutex>
#include <thread>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>


namespace fs = std::filesystem;

inline std::chrono::high_resolution_clock::time_point
get_current_time_fenced() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto res_time = std::chrono::high_resolution_clock::now();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return res_time;
}

template<class D>
inline long long to_s(const D &d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

struct StationData {
    int count = 0;
    double sum = 0.0;
    double min = std::numeric_limits<double>::infinity();
    double max = -std::numeric_limits<double>::infinity();

    void update(double temp) {
        ++count;
        sum += temp;
        if (temp < min) min = temp;
        if (temp > max) max = temp;
    }

    double mean() const {
        return count ? sum / count : 0.0;
    }
};

#endif //INC_1BRC_MAIN_H
