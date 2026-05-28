#include "MapReduceJob.h"
#include <atomic>
#include <thread>
#include <algorithm>
#include <barrier>


void MapReduceJob::update_state(MapReduceStage stage, uint32_t total) {
    uint64_t new_state = (static_cast<uint64_t>(stage) << 62) | (static_cast<uint64_t>(total) << 31);
    _state.store(new_state);
}


void MapReduceJob::worker(int tid) {

    // stage 1 - map
    MapContext mapContext(intermediateVecs[tid]);
    while (true) {
        uint32_t index = mapCounter.fetch_add(1);
        if (index >= inputVec.size()) {
            break;
        }
        auto pair = inputVec[index];

        client.map(pair.first, pair.second, mapContext);

        _state.fetch_add(1);
    }

    // stage 2 - sort our intermediate vector
    std::sort(intermediateVecs[tid].begin(), intermediateVecs[tid].end(),
        [](const IntermediatePair& a, const IntermediatePair& b) {
            return *(a.first) < *(b.first);
        }
    );


    syncBarrier->arrive_and_wait(); // wait to everyone


    // stage 3 - thread 0 show
    if (tid == 0) {

        // update stage and proccesed
        uint32_t totalShuffleTasks = 0;
        for (const auto& vec : intermediateVecs) {
            totalShuffleTasks += vec.size();
        }
        update_state(SHUFFLE_STAGE, totalShuffleTasks);

        // shuffle
        while (true) {
            K2* maxKey = nullptr;

            for (auto& vec : intermediateVecs) {
                if (!vec.empty()) {
                    if (maxKey == nullptr || *maxKey < *(vec.back().first)) {
                        maxKey = vec.back().first.get();
                    }
                }
            }

            if (maxKey == nullptr) {
                break;
            }

            std::vector<IntermediatePair> currentKeyGroup;

            for (auto& vec : intermediateVecs) {
                while (!vec.empty()) {
                    K2* currentKey = vec.back().first.get();

                    if (!(*currentKey < *maxKey) && !(*maxKey < *currentKey)) {
                        currentKeyGroup.push_back(vec.back());
                        vec.pop_back();
                    } else {
                        break;
                    }
                }
            }

            shuffledQueue.push_back(currentKeyGroup);
            _state.fetch_add(currentKeyGroup.size());
        }
    }

    syncBarrier->arrive_and_wait(); // wait to thread 0


    // stage 4 - reduce
    if (tid == 0) { // update stage and proccesed
        uint32_t totalReduceTasks = 0;
        for (const auto& group : shuffledQueue) {
            totalReduceTasks += group.size();
        }
        update_state(REDUCE_STAGE, totalReduceTasks);
    }
    syncBarrier->arrive_and_wait(); // wait to thread 0
    ReduceContext reduceContext(globalOutputVec, outputMutex);
    while (true) {
        uint32_t index = reduceCounter.fetch_add(1);
        if (index >= shuffledQueue.size()) {
            break;
        }
        auto& currentGroup = shuffledQueue[index];

        client.reduce(currentGroup, reduceContext);

        _state.fetch_add(currentGroup.size());
    }


}

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel) :
    inputVec(inputVec), _state(0), intermediateVecs(multiThreadLevel), mapCounter(0), client(client), multiThreadLevel(multiThreadLevel), reduceCounter(0)
{
    // define barrier
    syncBarrier = new std::barrier<>(multiThreadLevel);

    // define stage
    update_state(MAP_STAGE, inputVec.size());

    // create threads
    for (int i = 0; i < multiThreadLevel; i++) {
        threads.emplace_back(&MapReduceJob::worker, this, i);
    }

}

MapReduceState MapReduceJob::getState(void) const
{
    uint64_t current_state = _state.load();
    MapReduceStage new_stage = static_cast<MapReduceStage>(current_state >> 62);

    uint32_t processed = current_state & ((1ULL << 31) - 1);
    uint32_t total = (current_state >> 31) & ((1ULL << 31) - 1);

    double per = 0.0;
    if (total > 0) {
        per = (static_cast<double>(processed) / static_cast<double>(total)) * 100.0;
    } else {
        per = 100.0;
    }

    return {new_stage, per};
}

void MapReduceJob::wait(void)
{
    std::lock_guard<std::mutex> lock(_waitMutex); // RAII usage for safety

    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}



OutputVec MapReduceJob::getOutput(void)
{
    wait();
    std::sort(globalOutputVec.begin(), globalOutputVec.end(),
        [](const OutputPair& a, const OutputPair& b) {
            return *(a.first) < *(b.first);
        }
    );
    return globalOutputVec;
}

bool MapReduceJob::isDone(void) const
{
    MapReduceState new_state = getState();
    return (new_state.stage == REDUCE_STAGE && new_state.percentage >= 100);

}

MapReduceJob::~MapReduceJob()
{
    wait();
    delete syncBarrier;
}
