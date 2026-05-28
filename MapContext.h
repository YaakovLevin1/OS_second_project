#ifndef MAP_CONTEXT_H
#define MAP_CONTEXT_H

#include "MapReduceKeys.h"
#include <vector>
#include <utility>


class MapContext
{
public:
    /*
        Constructor that initialites the intermediate vector for each thread
    */
    explicit MapContext(IntermediateVec& threadVector);
    /*
    You must keep and implement this function:
    */
    void addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value);

    // Blocking the ability to copy instances of mapContext
    MapContext(const MapContext&) = delete;
    MapContext& operator=(const MapContext&) = delete;
private:
    IntermediateVec& _threadVector;
};

#endif // MAP_CONTEXT_H