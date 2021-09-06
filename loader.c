#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "khash.h"
#include "utils.h"

#define PERSON_FIELD_ID 0
#define PERSON_FIELD_BIRTHDAY 4
#define PERSON_FIELD_LOCATION 8
#define KNOWS_FIELD_PERSON 0
#define KNOWS_FIELD_FRIEND 1
#define INTEREST_FIELD_PERSON 0
#define INTEREST_FIELD_INTEREST 1

// hash map needs long keys (large person ids), but unsigned int is enough for person offsets
KHASH_MAP_INIT_INT64(pht, unsigned int)
khash_t(pht) *person_offsets;

FILE   *person_out;
FILE   *interest_out;
FILE   *knows_out;
Person *person_map;
Person *person;

unsigned long person_id = 0;
unsigned long person_id_prev = 0;
unsigned long knows_id = 0;

// person offset can be smaller, we do not have so many
unsigned int person_offset = 0;
unsigned long knows_offset = 0;
unsigned long interest_offset = 0;

void person_line_handler(unsigned char nfields, char** tokens) {
	int ret;
	khiter_t k;

	person->person_id = atol(tokens[PERSON_FIELD_ID]);
	person->birthday =  birthday_to_short(tokens[PERSON_FIELD_BIRTHDAY]);
	person->location =  atoi(tokens[PERSON_FIELD_LOCATION]);

	// add mapping person id -> offset to hash table
	k = kh_put(pht, person_offsets, person->person_id, &ret);
	kh_value(person_offsets, k) = person_offset;
	// write binary person record to file
	fwrite(person, sizeof(Person), 1, person_out);
	person_offset++;
}

void updatePerson() {
	person = &person_map[kh_value(person_offsets, 
		kh_get(pht, person_offsets, person_id))];
	person_id_prev = person_id;
}

void knows_line_handler(unsigned char nfields, char** tokens) {
	unsigned int knows_person_offset;
	person_id = atol(tokens[KNOWS_FIELD_PERSON]);
	unsigned long knows_id = atol(tokens[KNOWS_FIELD_FRIEND]);
	
	if (person_id != person_id_prev) {
		updatePerson();
		person->knows_first = knows_offset;
		person->knows_n = 0;
	}
	// lookup other person and write offset
	knows_person_offset = kh_value(person_offsets, 
			kh_get(pht, person_offsets, knows_id));
	fwrite(&knows_person_offset, sizeof(unsigned int), 1, knows_out);
	knows_offset++;
	person->knows_n++;
}

void interest_line_handler(unsigned char nfields, char** tokens) {
	unsigned short interest_id;
	person_id = atol(tokens[INTEREST_FIELD_PERSON]);
	interest_id = atoi(tokens[INTEREST_FIELD_INTEREST]);
	if (person_id != person_id_prev) {
		updatePerson();
		person->interests_first = interest_offset;
		person->interest_n = 0;
	}
	fwrite(&interest_id, sizeof(unsigned short), 1, interest_out);
	interest_offset++;
	person->interest_n++;
}

int main(int argc, char *argv[]) {
	char* person_input_file    = makepath(argv[1], "person",   "csv");
	char* interest_input_file  = makepath(argv[1], "interest", "csv");
	char* knows_input_file     = makepath(argv[1], "knows",    "csv");
	char* person_output_file   = makepath(argv[2], "person",   "bin");
	char* interest_output_file = makepath(argv[2], "interest", "bin");
	char* knows_output_file    = makepath(argv[2], "knows",    "bin");
	
	khiter_t k;
	int person_map_fd;
	struct stat st;

	if (argc < 3) {
		fprintf(stderr, "Usage: [csv input path] [output path]\n");
		exit(-1);
	}

	if (stat(argv[2], &st) == -1) {
    	if (mkdir(argv[2], 0700) != 0) {
			fprintf(stderr, "Unable to create output directory %s\n", argv[2]);
			exit(-1);
    	}
	}	

	// first pass person, parse person, write to binary and store bin offset in hash table
	person_offsets = kh_init(pht);	
	person = malloc(sizeof(Person));
	person->interest_n = person->knows_n = 0;
	person_out = open_binout(person_output_file);
	parse_csv(person_input_file, &person_line_handler);

	// mmap person.bin binary file for updates
	fclose(person_out);
	person_map_fd = open(person_output_file, O_RDWR);
	person_map = (Person *) mmap(0, person_offset * sizeof(Person), PROT_READ | PROT_WRITE, 
		MAP_SHARED, person_map_fd, 0);
	if (person_map_fd == 0 || person_map == MAP_FAILED) {
		fprintf(stderr, "Failed to map person binary.\n");
		exit(-1);
	}
	close(person_map_fd);

	// pass through interest and friends, write to binary, set offsets in person
	person_id = 0;
	interest_out = open_binout(interest_output_file);
	parse_csv(interest_input_file, &interest_line_handler);

	person_id = 0;
	knows_out = open_binout(knows_output_file);
	parse_csv(knows_input_file, &knows_line_handler);

	return 0;
}

