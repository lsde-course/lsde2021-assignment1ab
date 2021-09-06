#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "utils.h"

int main(int argc, char *argv[]) {
	char* person_output_file   = makepath(argv[1], "person",   "bin");
	char* interest_output_file = makepath(argv[1], "interest", "bin");
	char* knows_output_file    = makepath(argv[1], "knows",    "bin");
	
	// this does not do anything yet. But it could...
	
	return 0;
}

