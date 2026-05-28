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
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
