#include "ReduceContext.h"

// implement here your constructor and destructor

ReduceContext::ReduceContext(OutputVec& outputVec, std::mutex& mutex) : globalOutputVec(outputVec), outputMutex(mutex) {}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    std::lock_guard<std::mutex> lock(outputMutex); // RAII mutex
    globalOutputVec.emplace_back(std::move(key), std::move(value));
}