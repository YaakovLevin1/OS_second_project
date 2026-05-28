#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include <atomic>
#include <thread>
#include "MapReduceClient.h"
#include <barrier>
// you can add other includes here

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
	// get from user
	const InputVec& inputVec;
	const MapReduceClient &client;
	int multiThreadLevel;

	// working vectors
	std::vector<std::thread> threads;
	std::vector<IntermediateVec> intermediateVecs;
	OutputVec globalOutputVec;

	// the cool vars (atomic, mutex, barrier)
	std::mutex outputMutex;
	std::mutex _waitMutex;
	std::atomic<uint64_t> _state;
	std::atomic<uint32_t> mapCounter;
	std::barrier<>* syncBarrier;

	// function for the threads
	void worker(int tid);

	// function for state
	void update_stage(void);
	void update_total();
	void set_proccesed(int value);
	void update_proccesed();

};
	
#endif // MAP_REDUCE_JOB_H
