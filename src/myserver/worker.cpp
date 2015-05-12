#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"

#define NUM_THREADS 36
#define NUM_CORES_PER_CPU 6

using namespace std;

template <class T>
class WorkQueue {
private:
  std::vector<T> normal_storage;
  vector<T> fast_storage;
  pthread_mutex_t queue_lock;
  pthread_cond_t queue_cond;

public:

  WorkQueue() {
    pthread_cond_init(&queue_cond, NULL);
    pthread_mutex_init(&queue_lock, NULL);
  }

  T get_work() {
    pthread_mutex_lock(&queue_lock);
    while (normal_storage.size() == 0 && fast_storage.size() == 0) {
      pthread_cond_wait(&queue_cond, &queue_lock);
    }
    T item;
    if (!fast_storage.empty()) {
        item = fast_storage.front();
        fast_storage.erase(fast_storage.begin());
    } else {
        item = normal_storage.front();
        normal_storage.erase(normal_storage.begin());
    }
    pthread_mutex_unlock(&queue_lock);
    return item;
  }

  void put_normal_work(const T& item) {
    pthread_mutex_lock(&queue_lock);
    normal_storage.push_back(item);
    pthread_mutex_unlock(&queue_lock);
    pthread_cond_signal(&queue_cond);
  }

  void put_fast_work(const T& item) {
    pthread_mutex_lock(&queue_lock);
    fast_storage.push_back(item);
    pthread_mutex_unlock(&queue_lock);
    pthread_cond_signal(&queue_cond);
  }
};

static struct Worker_state {

  WorkQueue<Request_msg> worker_queue;

} wstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

void* execute_countprimes_work(void* vargp) {

  int* ptr;
  ptr = (int*) vargp;

  Request_msg dummy_req(0);
  Response_msg dummy_resp(0);
  create_computeprimes_req(dummy_req, *ptr);
  execute_work(dummy_req, dummy_resp);
  *ptr = atoi(dummy_resp.get_response().c_str());

  pthread_exit(0);

}

static void execute_compareprimes_multithread(const Request_msg& req, Response_msg& resp) {

  int params[4];

  params[0] = atoi(req.get_arg("n1").c_str());
  params[1] = atoi(req.get_arg("n2").c_str());
  params[2] = atoi(req.get_arg("n3").c_str());
  params[3] = atoi(req.get_arg("n4").c_str());

  pthread_t tid[3];

  for (int i=0; i<3; i++) {
    pthread_create(&tid[i], NULL, execute_countprimes_work, (void *)&params[i]);
  }

  Request_msg dummy_req(0);
  Response_msg dummy_resp(0);
  create_computeprimes_req(dummy_req, params[3]);
  execute_work(dummy_req, dummy_resp);
  params[3] = atoi(dummy_resp.get_response().c_str());

  for (int i=0; i<3; i++) {
    pthread_join(tid[i], NULL);
  }

  if (params[1]-params[0] > params[3]-params[2])
    resp.set_response("There are more primes in first range.");
  else
    resp.set_response("There are more primes in second range.");

}

void process_request(const Request_msg& req) {

  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.
  Response_msg resp(req.get_tag());

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  double startTime = CycleTimer::currentSeconds();

  if (req.get_arg("cmd").compare("compareprimes") == 0) {

    // The compareprimes command needs to be special cased since it is
    // built on four calls to execute_execute work.  All other
    // requests from the client are one-to-one with calls to
    // execute_work.

    //execute_compareprimes(req, resp);
    execute_compareprimes_multithread(req, resp);

  } else {

    // actually perform the work.  The response string is filled in by
    // 'execute_work'
    execute_work(req, resp);

  }

  double dt = CycleTimer::currentSeconds() - startTime;
  DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

  // send a response string to the master
  worker_send_response(resp);
}





void *hyper_thread(void *vargp) {
  pthread_detach(pthread_self());
  while (1) {
      process_request(wstate.worker_queue.get_work());
  }
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  pthread_t tid;
  for (int i=0;i < NUM_THREADS;i++) {
    pthread_create(&tid, NULL, hyper_thread, NULL);
  }


}

void worker_handle_request(const Request_msg& req) {
  if (req.get_arg("cmd") == "tellmenow") {
      wstate.worker_queue.put_fast_work(req);
  }
  else {
      wstate.worker_queue.put_normal_work(req);
  }
}
