#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/MinMaxHeap.hpp"
#include "server/messages.h"
#include "server/master.h"
#include "tools/cycle_timer.h"
#include "server/pullrequest.h"

#include <climits>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <iostream>

#define WORKER_THREADS           36
#define INITIAL_NUM_WORKERS      2
#define ADD_WORKER_THRESHOLD     34 //((int)(0.95 * WORKER_THREADS))
#define PROJECT_IDEA_THRESHOLD   2
#define KILL_WORKER_LOWER_BOUND  6
#define KILL_WORKER_THRESHOLD    18 //((int)(0.5 * WORKER_THREADS))
#define KILL_WORKER_AFTER        60


using namespace std;
using namespace minmax;

struct Worker_stat {

  Worker_handle handle;
  unordered_map<int, Client_handle> tag_client_map;

};

class WorkerQueueCompare {
private:
  unordered_map<int, Worker_stat>& map;
public:
  WorkerQueueCompare(unordered_map<int, Worker_stat>& input_map) : map(input_map) {}

  bool operator()(int w1, int w2)
  {
    return map[w1].tag_client_map.size() < map[w2].tag_client_map.size();
  }
};

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.
  int next_tag;

  // data structure for worker queue size
  unordered_map<int, Worker_stat> my_worker_stats;
  unordered_map<Worker_handle, int> handle_tag_map;
  vector<int> active_workers;
  vector<int> to_kill_workers;
  WorkerQueueCompare compare_obj;
  MinMaxHeap<int, vector<int>, WorkerQueueCompare> worker_heap;
  pthread_mutex_t heap_lock = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t next_tag_lock = PTHREAD_MUTEX_INITIALIZER;

  Master_state() : compare_obj(this->my_worker_stats), worker_heap(this->active_workers, this->compare_obj) {}

  void insert_worker(int tag) {
    worker_heap.push(tag);
  }

  int get_min_queue_worker() {
    int retval = worker_heap.findMin();
    return retval;
  }

  int get_max_queue_worker() {
    int retval = worker_heap.findMax();
    return retval;
  }

  void disable_max_queue_worker() {
    worker_heap.popMax();
  }

} mstate;

//void add_worker();

void master_node_init() {
  mstate.next_tag = 0;
}

// TODO on and off of is_worker_starting, is_worker_dying also be set and clear
// by autoscaler
void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  DLOG(INFO) << "ONLINE: worker " << tag << " has come online" << endl;

  Worker_stat stat;
  stat.handle = worker_handle;
  mstate.my_worker_stats[tag] = stat;
  mstate.handle_tag_map[worker_handle] = tag;
  DLOG(INFO) << "handle_tag_map: added " << worker_handle << ":" << tag;

  // stat map needs to be set first because heap will use the map
  // size to compare this element with other elements
  pthread_mutex_lock(&mstate.heap_lock);
  mstate.insert_worker(tag);
  pthread_mutex_unlock(&mstate.heap_lock);
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: ["
             << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  int worker_tag = mstate.handle_tag_map[worker_handle];
  DLOG(INFO) << "worker_tag for " << worker_handle << " is " << worker_tag;
  Worker_stat& stat = mstate.my_worker_stats[worker_tag];

  int tag = resp.get_tag();
  Client_handle client_handle;

  pthread_mutex_lock(&mstate.heap_lock);
  client_handle = stat.tag_client_map[tag];
  DLOG(INFO) << "Worker " << worker_tag << ": client_handle is " << client_handle << " for request " << tag;
  stat.tag_client_map.erase(tag);
  if (find(mstate.to_kill_workers.begin(), mstate.to_kill_workers.end(), worker_tag)
        == mstate.to_kill_workers.end()) {
    mstate.worker_heap.deleteKeyByValue(worker_tag);
    mstate.insert_worker(worker_tag);
  }
  pthread_mutex_unlock(&mstate.heap_lock);

  send_client_response(client_handle, resp);

}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.

  int tag;
  pthread_mutex_lock(&mstate.next_tag_lock);
  tag = mstate.next_tag++;
  pthread_mutex_unlock(&mstate.next_tag_lock);

  Request_msg worker_req(tag, client_req);
  Worker_handle worker_handle;

  pthread_mutex_lock(&mstate.heap_lock);
  int worker_tag = mstate.get_min_queue_worker();
  mstate.worker_heap.popMin();
  Worker_stat& stat = mstate.my_worker_stats[worker_tag];
  stat.tag_client_map[tag] = client_handle;
  DLOG(INFO) << "Worker " << worker_tag << ": added " << tag << ":" << client_handle;
  worker_handle = stat.handle;
  mstate.insert_worker(worker_tag);
  pthread_mutex_unlock(&mstate.heap_lock);

  send_request_to_worker(worker_handle, worker_req);

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
  // TODO this is commented out because the new design should only allow passive monitoring
  // instead of active scaling check. Also, this makes request_new_worker_node() only called
  // by the timer thread after master node initialization
  /*
  if (mstate.next_tag % 2) {
      handle_tick();
  }
  */
}

void handle_pull_request(PullRequest& request, PullResponse& response) {
  // handle request
  pthread_mutex_lock(&mstate.heap_lock);
  for (int i = 0; i < request.todisableworkers_size(); i++) {
    int tag = request.todisableworkers(i);

    mstate.worker_heap.deleteKeyByValue(tag);
    mstate.to_kill_workers.push_back(tag);
  }

  for (int i = 0; i < request.torebornworkers_size(); i++) {
    int tag = request.torebornworkers(i);

    vector<int>::iterator it = find(mstate.to_kill_workers.begin(), mstate.to_kill_workers.end(), tag);
	if (it == mstate.to_kill_workers.end()) {
		DLOG(INFO) << "ERROR: attempt to reborn non disabled worker";
	}
    mstate.to_kill_workers.erase(it);
    mstate.insert_worker(tag);
  }

  // fill response
  response.set_useless(0);
  vector<int>::iterator it;
  for (it = mstate.active_workers.begin();it != mstate.active_workers.end();it++) {
    PullResponse::WorkerStat *workerStat = response.add_workers();
    int tag = *it;
    workerStat->set_tag(tag);
    workerStat->set_queuesize(mstate.my_worker_stats[tag].tag_client_map.size());
  }
  for (it = mstate.to_kill_workers.begin();it != mstate.to_kill_workers.end();) {
    int tag = *it;
    if (mstate.my_worker_stats[tag].tag_client_map.size() == 0) {
      response.add_tokillworkers(tag);
      mstate.my_worker_stats.erase(tag);
      it = mstate.to_kill_workers.erase(it);
    }
    else {
      it++;
    }
  }
  pthread_mutex_unlock(&mstate.heap_lock);
}

/*
void handle_tick() {

  if (mstate.server_ready == false)
    return;

  // probe if any worker can be killed
  for (auto it = mstate.to_kill_workers.begin(); it != mstate.to_kill_workers.end();) {
    if (mstate.my_worker_stats[(*it)].tag_client_map.size() == 0) {
      DLOG(INFO) << "KILL: worker " << *it <<
        " emptied, shutting it down" << endl;

      kill_worker_node(mstate.my_worker_stats[(*it)].handle);
      mstate.my_worker_stats.erase(*it);
      it = mstate.to_kill_workers.erase(it);
      mstate.is_worker_dying = false;
    }
    else {
      ++it;
    }
  }

  // check the min/max. queue size, request a worker node or schedule to kill a worker node
  pthread_mutex_lock(&mstate.heap_lock);

  int min_worker_tag = mstate.get_min_queue_worker();
  int min_queue_size = mstate.my_worker_stats[min_worker_tag].tag_client_map.size();
  int max_worker_tag = mstate.get_max_queue_worker();
  int max_queue_size = mstate.my_worker_stats[max_worker_tag].tag_client_map.size();
  int num_workers = mstate.active_workers.size();

  if (min_queue_size < mstate.prev_min_queue_size) {
    mstate.can_kill_worker = true;
  }
  mstate.prev_min_queue_size = min_queue_size;
  if (num_workers < mstate.max_num_workers && min_queue_size > ADD_WORKER_THRESHOLD) {
    DLOG(INFO) << "BUSY (" << min_queue_size << "): requesting one more worker node" << endl;
    add_worker();
  }
  else if (num_workers > 1 && max_queue_size < KILL_WORKER_THRESHOLD && mstate.can_kill_worker) {
    DLOG(INFO) << "IDLE (" << max_queue_size << "): scheduling shutting down worker "
               << max_worker_tag << endl;

    if (!mstate.is_worker_dying) {
      mstate.to_kill_workers.push_back(max_worker_tag);
      mstate.disable_max_queue_worker();
      mstate.is_worker_dying = true;
    }
  }

  pthread_mutex_unlock(&mstate.heap_lock);

}

void add_worker() {
  if (!mstate.is_worker_starting) {
    if (!mstate.to_kill_workers.empty()) {
        auto begin_it = mstate.to_kill_workers.begin();
        int reborn_worker_tag = *begin_it;

        mstate.insert_worker(reborn_worker_tag);

        DLOG(INFO) << "REBORN worker " << reborn_worker_tag << endl;
        mstate.to_kill_workers.erase(begin_it);
    } else {
      int tag = mstate.next_additional_worker;
      Request_msg req(tag);
      req.set_arg("name", string("my additional worker ")
                  + to_string(mstate.next_additional_worker++));
      request_new_worker_node(req);
      mstate.is_worker_starting = true;
    }
  }
}
*/
