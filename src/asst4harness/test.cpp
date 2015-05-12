#include <stdio.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <string.h>

int main (int argc, char **argv) {
  printf("LIBEVENT_VERSION_NUMBER: %08x\n", LIBEVENT_VERSION_NUMBER);
#ifdef EVTHREAD_USE_PTHREADS_IMPLEMENTED
  printf("pthreads support implemented\n");
#endif

  const char *v = event_get_version();
  printf("Running with libevent version %s\n", v);

  return 0;
}
