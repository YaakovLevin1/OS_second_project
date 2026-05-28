#include "MapReduceJob.h"
#include <atomic>
#include <thread>
#include <algorithm>
#include <barrier>

/*
===============================================
Implement:
===============================================
*/
void MapReduceJob::update_stage(void) {
    _state.fetch_add(1ULL << 62);
}

void MapReduceJob::update_total(void) {
    _state.fetch_add(1ULL << 31);
}

void MapReduceJob::set_proccesed(int value) {
    uint64_t current_state = _state.load();
    uint64_t next_state;

    do {
        uint64_t cleared_state = current_state & ~((1ULL << 31) - 1);

        next_state = cleared_state | (static_cast<uint64_t>(value) & ((1ULL << 31) - 1));

    } while (!_state.compare_exchange_weak(current_state, next_state));
}

void MapReduceJob::update_proccesed(void) {
    _state.fetch_add(1);
}




void MapReduceJob::worker(int tid) {
    // stage 1 - map
    // TODO - update atomic stage

    MapContext mapContext(intermediateVecs[tid]);
    while (true) {
        uint32_t index = mapCounter.fetch_add(1);
        if (index > inputVec.size() - 1) {
            break;
        }
        auto pair = inputVec[index];

        client.map(pair.first, pair.second, mapContext);

        // TODO - update percentage
    }

    // stage 2 - sort our intermediate vector
    std::sort(intermediateVecs[tid].begin(), intermediateVecs[tid].end(),
        [](const IntermediatePair& a, const IntermediatePair& b) {
            return *(a.first) < *(b.first);
        }
    );


    syncBarrier->arrive_and_wait(); // wait to everyone

    // stage 3 - thread 0 show
    // TODO - update atomic stage
    if (tid == 0) {

        uint32_t totalShuffleTasks = 0;
        for (const auto& vec : intermediateVecs) {
            totalShuffleTasks += vec.size();
        }

        while (true) {

        }
        // TODO - update atomic percentage

    }
    syncBarrier->arrive_and_wait(); // wait to thread 0

    // stage 4 - reduce
    // TODO - update atomic stage


}

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel) :
    inputVec(inputVec), _state(0), intermediateVecs(multiThreadLevel), mapCounter(0), client(client), multiThreadLevel(multiThreadLevel)
{
    // define barrier
    syncBarrier = new std::barrier<>(multiThreadLevel);

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
    }

    return {new_stage, per};

}

void MapReduceJob::wait(void)
{
    _waitMutex.lock();
    if (isDone())
        return;


    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    _waitMutex.unlock();
}



OutputVec MapReduceJob::getOutput(void)
{
    wait();

}

bool MapReduceJob::isDone(void) const
{
    MapReduceState new_state = getState();
    return (new_state.stage == REDUCE_STAGE && new_state.percentage >= 100);

}

MapReduceJob::~MapReduceJob()
{
    wait();
}
