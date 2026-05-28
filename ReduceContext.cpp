#include "ReduceContext.h"

// implement here your constructor and destructor

ReduceContext::ReduceContext(OutputVec& outputVec, std::mutex& mutex) : globalOutputVec(outputVec), outputMutex(mutex) {}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    outputMutex.lock();

    OutputPair data(key,value);
    globalOutputVec.push_back(data);

    outputMutex.unlock();
}