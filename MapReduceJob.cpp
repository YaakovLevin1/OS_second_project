#include "MapReduceJob.h"

#include <thread>

/*
===============================================
Implement:
===============================================
*/

void MapReduceJob::worker(int tid) {

}

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
{

    // create threads
    for (int i = 0; i < multiThreadLevel; i++) {
        threads.emplace_back(&MapReduceJob::worker, this, i);
    }

}

MapReduceState MapReduceJob::getState(void) const
{
    // TODO: implement this function
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
