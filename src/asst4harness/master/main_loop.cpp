// Copyright 2013 15418 Course Staff.
// This was most helpful: http://eradman.com/posts/kqueue-tcp.html

// Modified by Rui Shen for multi-threaded event dispatching

#include <assert.h>
#include <boost/unordered_set.hpp>
#include <unordered_map>
#include <errno.h>
#include <sys/types.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/select.h>
#include <unistd.h>
#include <netinet/in.h>
#include <boost/make_shared.hpp>
#include <pthread.h>
#include <event2/event.h>
#include <event2/thread.h>

#include "comm/comm.h"
#include "types/types.h"
#include "server/messages.h"
#include "server/master.h"

#include  "tools/cycle_timer.h"

#define MAX_EVENTS            1024
#define NUM_CLIENT_EVENT_BASE 4
#define NUM_WORKER_EVENT_BASE 1

extern int launcher_fd;
extern int accept_fd;

DEFINE_bool(log_network, false, "Log network traffic.");

#define NETLOG(level) DLOG_IF(level, FLAGS_log_network)

static bool is_server_initialized = false;
static int num_instances_booted = 0;
static double total_worker_seconds;
bool should_shutdown = false;
unsigned pending_worker_requests = 0;
std::map<Worker_handle, double> worker_boot_times;
std::unordered_map<Worker_handle, pthread_mutex_t> worker_write_locks;
boost::unordered_set<Worker_handle> workers;
static struct timeval *timer_timeout;

pthread_mutex_t worker_handle_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t worker_boot_time_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t worker_request_lock = PTHREAD_MUTEX_INITIALIZER;

static struct Event_state {
  struct event_base *main_event_base;
  struct event_base *timer_event_base;

  std::vector<struct event_base *> client_event_bases;
  int client_event_base_size;
  int client_event_base_index;

  std::vector<struct event_base *> worker_event_bases;
  int worker_event_base_size;
  int worker_event_base_index;

public:
  struct event_base *get_client_event_base() {
    struct event_base *retval = client_event_bases[client_event_base_index];
    client_event_base_index
      = (client_event_base_index + 1) % client_event_base_size;
    return retval;
  }
  struct event_base *get_worker_event_base() {
    struct event_base *retval = worker_event_bases[worker_event_base_index];
    worker_event_base_index
      = (worker_event_base_index + 1) % worker_event_base_size;
    return retval;
  }
} estate;

static void close_connection(void* connection_handle) {
  struct event *event = reinterpret_cast<struct event *>(connection_handle);
  CHECK_NE(event_get_fd(event), accept_fd) << "Critical connection failed\n";
  CHECK_NE(event_get_fd(event), launcher_fd) << "Critical connection failed\n";

  // We should never call close_connection() on a worker handle, because
  // kill_worker() first removes the worker from the worker set and then
  // we remove it from the event loop here.
  pthread_mutex_lock(&worker_handle_lock);
  CHECK(workers.find(connection_handle) == workers.end())
    << "Unexpected close of worker handle " << event_get_fd(event);
  pthread_mutex_unlock(&worker_handle_lock);

  NETLOG(INFO) << "Connection closed " << event_get_fd(event);

  PLOG_IF(ERROR, close(event_get_fd(event)))
    << "Error closing fd " << event_get_fd(event);
  LOG_IF(ERROR, event_del(event) < 0)
    << "Error deleting event " << event_get_fd(event);
  event_free(event);
}

// only master initialization and timer thread will call this function
void request_new_worker_node(const Request_msg& req) {
  // HACK(kayvonf): stick the tag in the dictionary to avoid a lot of
  // extra plumbing
  Request_msg modified(req);

  char tmp_buffer[32];
  sprintf(tmp_buffer, "%d", req.get_tag());
  modified.set_arg("tag", tmp_buffer);

  std::string str = modified.get_request_string();

  pthread_mutex_lock(&worker_request_lock);
  DLOG(INFO) << "Requesting worker " << str;
  CHECK_EQ(send_string(launcher_fd, str), 0)
    << "Cannot talk to launcher\n";
  pending_worker_requests++;
  pthread_mutex_unlock(&worker_request_lock);
}

static void accumulate_time(Worker_handle worker_handle) {
  double start_time = worker_boot_times[worker_handle];
  double end_time = CycleTimer::currentSeconds();
  double worker_up_time = end_time - start_time;
  total_worker_seconds += worker_up_time;

  //printf("*** MASTER: accumulating %.2f sec\n", worker_up_time);
}

// only timer thread will call this function
void kill_worker_node(Worker_handle worker_handle) {
  pthread_mutex_lock(&worker_handle_lock);
  NETLOG(INFO) << "HAHA: erasing " << worker_handle;
  CHECK_EQ(workers.erase(worker_handle), 1U) << "Attempt to kill non worker";
  CHECK_EQ(worker_write_locks.erase(worker_handle), 1U) << "Attempt to remove lock of non worker";
  pthread_mutex_unlock(&worker_handle_lock);

  // TODO possible race with close_connection call in handle_worker_read()
  // but if message are correctly handled, there will be no race
  close_connection(worker_handle);

  pthread_mutex_lock(&worker_boot_time_lock);
  accumulate_time(worker_handle);
  worker_boot_times.erase(worker_handle);
  pthread_mutex_unlock(&worker_boot_time_lock);
}

void send_request_to_worker(Worker_handle worker_handle, const Request_msg& job) {
  work_t comm_work;

  std::string contents = job.get_request_string();
  int allocation_size = contents.size();
  comm_work.buf = boost::make_shared<char[]>(allocation_size);
  comm_work.buf_len = allocation_size;
  strncpy(comm_work.buf.get(), contents.c_str(), allocation_size);

  // now perform the send
  pthread_mutex_lock(&worker_handle_lock);
  CHECK(workers.find(worker_handle) != workers.end())
    << "Attempt to send work to invalid worker";
  pthread_mutex_unlock(&worker_handle_lock);

  // TODO fine-grained locking on each worker handle
  struct event* event = reinterpret_cast<struct event*>(worker_handle);
  NETLOG(INFO) << "Sending work (" << job.get_tag() << "," << comm_work << ") to "
               << event_get_fd(event);

  // obtain per-worker write lock
  pthread_mutex_t worker_write_lock;
  pthread_mutex_lock(&worker_handle_lock);
  worker_write_lock = worker_write_locks[worker_handle];
  pthread_mutex_unlock(&worker_handle_lock);

  // send the work
  pthread_mutex_lock(&worker_write_lock);
  int retval = send_work(event_get_fd(event), comm_work, job.get_tag());
  CHECK_EQ(retval, 0)
    << "Unexpected connection failure with worker " << event_get_fd(event);
  pthread_mutex_unlock(&worker_write_lock);
}

void send_client_response(Client_handle client_handle, const Response_msg& resp) {

  resp_t comm_resp;

  std::string resp_str = resp.get_response();
  int allocation_size = resp_str.size();
  comm_resp.buf = boost::make_shared<char[]>(allocation_size);
  comm_resp.buf_len = allocation_size;
  strncpy(comm_resp.buf.get(), resp_str.c_str(), allocation_size);

  // send to comm layer
  struct event* event = reinterpret_cast<struct event*>(client_handle);
  NETLOG(INFO) << "Sending response " << comm_resp << " to " << event_get_fd(event);
  CHECK_EQ(send_resp(event_get_fd(event), comm_resp, 0), 0)
    << "Unexpected connection failure with client " << event_get_fd(event);
}

void server_init_complete() {
  is_server_initialized = true;
}

static void shutdown() {
  LOG(INFO) << "Shutting down";

  /*
  event_base_loopexit(estate.timer_event_base, NULL);
  event_base_free(estate.timer_event_base);
  event_base_loopexit(estate.main_event_base, NULL);
  event_base_free(estate.main_event_base);
  for (int i = 0;i < estate.client_event_base_size;i++) {
    event_base_loopexit(estate.client_event_bases[i], NULL);
    event_base_free(estate.client_event_bases[i]);
  }
  for (int i = 0;i < estate.worker_event_base_size;i++) {
    event_base_loopexit(estate.worker_event_bases[i], NULL);
    event_base_free(estate.worker_event_bases[i]);
  }
  */

  exit(0);
}

static void handle_client_read(int fd, int16_t events, void* arg) {
  assert(events & EV_READ);
  message_t message;
  int tag;
  int err = recv_message(fd, &message, &tag);
  struct event* event = (struct event *)arg;
  if (err < 0) {
    NETLOG(WARNING) << "Connection closed on " << fd;
    close_connection(event);
    return;
  }

  NETLOG(INFO) << "Got message (" << message << "," << tag << ")";

  switch (message) {

    case ISREADY: {

      resp_t comm_resp;
      std::string resp_str( is_server_initialized ? "ready" : "not_ready" );

      int allocation_size = resp_str.size();
      comm_resp.buf = boost::make_shared<char[]>(allocation_size);
      comm_resp.buf_len = allocation_size;
      strncpy(comm_resp.buf.get(), resp_str.c_str(), allocation_size);

      // send to comm layer
      NETLOG(INFO) << "Sending response " << comm_resp << " to " << event_get_fd(event);
      CHECK_EQ(send_resp(event_get_fd(event), comm_resp, 0), 0)
        << "Unexpected connection failure with client " << event_get_fd(event);

      close_connection(event);
      break;
    }

    case WORKER_UP_TIME_STATS: {

      // Accumulate time for all the workers that HAVE NOT yet been shut
      // down
      pthread_mutex_lock(&worker_boot_time_lock);
      for (std::map<Worker_handle, double>::const_iterator it=worker_boot_times.begin();
           it != worker_boot_times.end(); it++)
        accumulate_time(it->first);
      pthread_mutex_unlock(&worker_boot_time_lock);

      resp_t comm_resp;

      char tmp_buffer[128];
      sprintf(tmp_buffer,"%d %.2f", num_instances_booted, total_worker_seconds);
      std::string resp_str(tmp_buffer);

      int allocation_size = resp_str.size();
      comm_resp.buf = boost::make_shared<char[]>(allocation_size);
      comm_resp.buf_len = allocation_size;
      strncpy(comm_resp.buf.get(), resp_str.c_str(), allocation_size);

      // send to comm layer
      NETLOG(INFO) << "Sending response " << comm_resp << " to " << event_get_fd(event);
      CHECK_EQ(send_resp(event_get_fd(event), comm_resp, 0), 0)
        << "Unexpected connection failure with client " << event_get_fd(event);

      close_connection(event);
      break;
    }

    case WORK: {
      // A new request from a client.
      work_t work;
      if (recv_work(fd, &work) < 0) {
        NETLOG(ERROR) << "Unexpected connection close on " << fd;
        close_connection(event);
        return;
      }
      NETLOG(INFO) << "Got new work " << work << " from " << fd;

      // HACK(kayvonf): convert a work_t into a Request_msg to pass to student code
      // Since the content in work_t.buf is not null terminated, this is a big mess
      int len = work.buf_len;
      char* tmp_buffer = new char[len+1];
      strncpy(tmp_buffer, work.buf.get(), len);
      tmp_buffer[len] = '\0';

      Request_msg client_req(0, tmp_buffer);
      delete [] tmp_buffer;

      // TODO add necessary locking for the worker heap
      handle_client_request(event, client_req);
      break;
    }

    default: {
      NETLOG(ERROR) << "Unexpected message " << message << " from " << fd;
      close_connection(event);
      return;
    }

  }
}

static void handle_worker_read(int fd, int16_t events, void* arg) {
  assert(events & EV_READ);
  message_t message;
  int tag;
  int err = recv_message(fd, &message, &tag);
  struct event *event = (struct event *)arg;

  if (err < 0) {
    NETLOG(WARNING) << "Connection closed on " << fd;
    close_connection(event);
    return;
  }

  NETLOG(INFO) << "Got message (" << message << "," << tag << ")";

  switch (message) {

    case RESPONSE: {
      // Worker job is done response.
      resp_t comm_resp;
      if (recv_resp(fd, &comm_resp) < 0) {
        NETLOG(ERROR) << "Unexpected connection close on " << fd;
        close_connection(event);
        return;
      }
      NETLOG(INFO) << "Got worker response (" << tag << "," << comm_resp
        << ") from " << fd << ":" << event;

      // HACK(kayvonf): convert a resp_t into a Response_msg to pass to student code
      // Since the content in resp_t.buf is not null terminated, this is a big mess
      int len = comm_resp.buf_len;
      char* tmp_buffer = new char[len+1];
      strncpy(tmp_buffer, comm_resp.buf.get(), len);
      tmp_buffer[len] = '\0';
      Response_msg resp(tag);
      resp.set_response(tmp_buffer);
      delete [] tmp_buffer;

      handle_worker_response(event, resp);
      break;
    }

    default: {
      NETLOG(ERROR) << "Unexpected message " << message << " from " << fd;
      close_connection(event);
      return;
    }

  }
}

static void handle_accept_id_msg(int fd, int16_t events, void* arg) {
  assert(events & EV_READ);
  NETLOG(INFO) << "ID message on " << fd;
  struct event *event = (struct event *)arg;

  message_t message;
  int tag;
  int err = recv_message(fd, &message, &tag);
  NETLOG(INFO) << "receive ID message errno " << fd << " " << err << " " << strerror(errno);
  if (err < 0) {
    NETLOG(WARNING) << "Connection closed on " << fd;
    close_connection(event);
    return;
  }

  NETLOG(INFO) << "Got message (" << message << "," << tag << ")";

  switch (message) {

    case NEW_CLIENT: {
      // Notification that a new client has connected.
      NETLOG(INFO) << "New client " << tag << " on " << fd;

      struct event *client_event;
      struct event_base *selected_event_base
        = estate.get_client_event_base();
      client_event = event_new(selected_event_base, fd, EV_READ|EV_PERSIST,
                               handle_client_read, event_self_cbarg());
      event_add(client_event, NULL);
      break;
    }

    case NEW_WORKER: {
      pthread_mutex_lock(&worker_request_lock);
      pending_worker_requests--;
      if (should_shutdown && pending_worker_requests == 0) {
        pthread_mutex_unlock(&worker_request_lock);
        shutdown();
      }
      pthread_mutex_unlock(&worker_request_lock);

      // Notification that a worker has booted.
      NETLOG(INFO) << "New worker " << tag << " on " << fd;

      struct event *worker_event;
      struct event_base *selected_event_base
        = estate.get_worker_event_base();
      worker_event = event_new(selected_event_base, fd, EV_READ|EV_PERSIST,
                               handle_worker_read, event_self_cbarg());
      event_add(worker_event, NULL);

      pthread_mutex_lock(&worker_handle_lock);
      NETLOG(INFO) << "HAHA: inserting " << worker_event;
      workers.insert(worker_event);
      pthread_mutex_t write_lock = PTHREAD_MUTEX_INITIALIZER;
      worker_write_locks[worker_event] = write_lock;
      pthread_mutex_unlock(&worker_handle_lock);

      pthread_mutex_lock(&worker_boot_time_lock);
      worker_boot_times[worker_event] = CycleTimer::currentSeconds();
      pthread_mutex_unlock(&worker_boot_time_lock);

      num_instances_booted++;
      handle_new_worker_online(worker_event, tag);
      break;
    }

    case SHUTDOWN: {
      if (pending_worker_requests == 0) {
        shutdown();
      } else {
        should_shutdown = true;
      }
      break;
    }

    default: {
      NETLOG(ERROR) << "Unexpected message " << message << " from " << fd;
      close_connection(event);
      return;
    }

  }
}

static void handle_accept(int fd, int16_t events, void* arg) {
  (void)arg;
  assert(events & EV_READ);

  struct sockaddr addr;
  socklen_t addr_len = sizeof(addr);
  fd = accept(fd, &addr, &addr_len);

  PCHECK(fd >= 0) << "Failure accepting new connection!";
  NETLOG(INFO) << "New connection on " << fd;

  struct event *read_id_event;
  read_id_event = event_new(estate.main_event_base, fd, EV_READ,
                           handle_accept_id_msg, event_self_cbarg());
  event_add(read_id_event, NULL);
}

static void handle_timer(int fd, int16_t events, void* arg) {
  (void)fd;
  (void)events;
  (void)arg;

  NETLOG(INFO) << "Timer tick";
  handle_tick();
}

void harness_init() {
  num_instances_booted = 0;
  total_worker_seconds = 0.0;
}

void *timer_event_thread(void *vargp) {
  pthread_detach(pthread_self());

  struct event_base *timer_event_base;
  timer_event_base = event_base_new();

  // pass the pointer to event_base out
  *(intptr_t *)vargp = (intptr_t)timer_event_base;

  struct event *timer_event;
  timer_event = event_new(timer_event_base, -1, EV_PERSIST,
                          handle_timer, NULL);
  event_add(timer_event, timer_timeout);

  event_base_dispatch(timer_event_base);

  pthread_exit(NULL);
}

void *client_event_thread(void *vargp) {
  pthread_detach(pthread_self());

  struct event_base *client_event_base;
  client_event_base = event_base_new();

  // pass the pointer to event_base out
  *(intptr_t *)vargp = (intptr_t)client_event_base;

  // attempt to dispatch client events
  int retval;
  do {
    retval = event_base_dispatch(client_event_base);
    if (retval == -1) {
      //DLOG(ERROR) << "failed to start dispatching for client_event_base";
      break;
    }
    else if (retval == 1) {
      //DLOG(INFO) << "no events are pending or active for client_event_base "
      //           << (void *)client_event_base << " for now";
    }
  } while (retval != 0);

  pthread_exit(NULL);
}

void *worker_event_thread(void *vargp) {
  pthread_detach(pthread_self());

  struct event_base *worker_event_base;
  worker_event_base = event_base_new();

  // pass the pointer to event_base out
  *(intptr_t *)vargp = (intptr_t)worker_event_base;

  // attempt to dispatch worker events
  int retval;
  do {
    retval = event_base_dispatch(worker_event_base);
    if (retval == -1) {
      //DLOG(ERROR) << "failed to start dispatching for worker_event_base";
      break;
    }
    else if (retval == 1) {
      //DLOG(INFO) << "no events are pending or active for worker_event_base "
      //           << (void *)worker_event_base << " for now";
    }
  } while (retval != 0);

  pthread_exit(NULL);
}

void harness_begin_main_loop(struct timeval* tick_period) {
  // set up libevent for use with pthreads
  CHECK_NE(evthread_use_pthreads(), -1) << "failed to set up libevent for use \
    with pthreads";

  timer_timeout = tick_period;

  struct event_base *main_event_base;
  main_event_base = event_base_new();

  // set up the accept event
  struct event *accept_event;
  accept_event = event_new(main_event_base, accept_fd, EV_READ|EV_PERSIST,
                           handle_accept, event_self_cbarg());
  event_add(accept_event, NULL);
  estate.main_event_base = main_event_base;

  // spawn a separate thread to dispatch timer events
  pthread_t timer_event_tid;
  pthread_create(&timer_event_tid, NULL, timer_event_thread,
                 &estate.timer_event_base);
  DLOG(INFO) << "timer event thread started with ID " << timer_event_tid;

  // spawn dedicated threads to dispatch client events
  estate.client_event_bases.resize(NUM_CLIENT_EVENT_BASE);
  estate.client_event_base_size = NUM_CLIENT_EVENT_BASE;
  estate.client_event_base_index = 0;
  pthread_t client_event_tid;
  for (int i = 0;i < NUM_CLIENT_EVENT_BASE;i++) {
    pthread_create(&client_event_tid, NULL, client_event_thread,
                   &estate.client_event_bases[i]);
    DLOG(INFO) << "client event thread " << i << " started with ID "
               << client_event_tid;
  }

  // spawn dedicated threads to dispatch worker events
  estate.worker_event_bases.resize(NUM_WORKER_EVENT_BASE);
  estate.worker_event_base_size = NUM_WORKER_EVENT_BASE;
  estate.worker_event_base_index = 0;
  pthread_t worker_event_tid;
  for (int i = 0;i < NUM_WORKER_EVENT_BASE;i++) {
    pthread_create(&worker_event_tid, NULL, worker_event_thread,
                   &estate.worker_event_bases[i]);
    DLOG(INFO) << "worker event thread " << i << " started with ID "
               << worker_event_tid;
  }

  NETLOG(INFO) << "Starting main event loop";
  event_base_dispatch(main_event_base);
}
