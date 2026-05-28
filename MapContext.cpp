#include "MapContext.h"

// implement here your constructor and destructor
MapContext::MapContext(IntermediateVec& threadVector) : _threadVector(threadVector){
}
void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{

    _threadVector.emplace_back(std::move(key), std::move(value));
}
