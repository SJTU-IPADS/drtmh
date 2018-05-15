#include "util.h"

#include <assert.h>
#include <stdio.h>


int main(int argc,char **argv) {

  assert(argc > 1);

  double cycles = atof(argv[1]);
  auto one_second_cycle = Breakdown_Timer::get_one_second_cycle();

  double converted = cycles / one_second_cycle;

  fprintf(stdout,"one second cycles %lu\n",one_second_cycle);
  fprintf(stdout,"Takes %f us.\n",converted * 1000000);

  return 0;
}
