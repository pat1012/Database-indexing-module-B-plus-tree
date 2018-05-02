
#include "Bruinbase.h"
#include "SqlEngine.h"
#include <cstdio>

int main()
{
  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}
