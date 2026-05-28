#include "MapReduceJob.h"

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
{
    // TODO: implement this constructor
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
