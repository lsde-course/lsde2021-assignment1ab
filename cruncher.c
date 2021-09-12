#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "utils.h" // Person defined here, and the mmap calls to load files into virtual memory

unsigned long person_num = 0;

// the database
Person *person_map;
unsigned int *knows_map;
unsigned short *interest_map;

FILE *outfile; // output file to print results into

typedef struct {
	unsigned long  person1_id;
	unsigned long  person2_id;
	unsigned long  person3_id;
    
	unsigned char  score;
} Result;

int result_comparator(const void *v1, const void *v2) {
	Result *r1 = (Result *) v1;
	Result *r2 = (Result *) v2;
	if (r1->score > r2->score)
		return -1;
	else if (r1->score < r2->score)
		return +1;
	else if (r1->person1_id < r2->person1_id)
		return -1;
	else if (r1->person1_id > r2->person1_id)
		return +1;
	else if (r1->person2_id < r2->person2_id)
		return -1;
	else if (r1->person2_id > r2->person2_id)
		return +1;
	else if (r1->person3_id < r2->person3_id)
		return -1;
	else if (r1->person3_id > r2->person3_id)
		return +1;
	return 0;
}

#define ARTIST_FAN -1

signed char get_score(unsigned int person, unsigned short artist, unsigned short artists[]) {
	signed char score = 0;
	for (unsigned long i = person_map[person].interests_first;
	     i < person_map[person].interests_first + person_map[person].interest_n;
	     i++)
	{
		unsigned short interest = interest_map[i];
		if (interest == artists[0]) score++;
		if (interest == artists[1]) score++;
		if (interest == artists[2]) score++;
		if (interest == artist) score = ARTIST_FAN;
		if ((score&3) == 3) break; // early out
	}
	return score;
}

void query(unsigned short qid, unsigned short artist, unsigned short artists[], unsigned short bdstart, unsigned short bdend) {
    unsigned int result_length = 0, result_maxsize = 15000;
    Result* results = (Result*) malloc(result_maxsize * sizeof(Result));
    signed char* person_score = (signed char*) malloc(person_num); //

    // Pass 1
    printf("Running query %d\n", qid);
    for (unsigned int person1 = 0; person1 < person_num; person1++) {
        if (person1 > 0 && person1 % REPORTING_N == 0) {
            printf("%.2f%%\n", 100 * (person1 * 1.0/person_num));
        }
        person_score[person1] = get_score(person1,artist,artists); //
    }

    // Pass 2
    for (unsigned int person1 = 0; person1 < person_num; person1++) {
        if (person1 > 0 && person1 % REPORTING_N == 0) {
            printf("%.2f%%\n", 100 * (person1 * 1.0/person_num));
        }
        // check whether person1 birthday is in range
        if (person_map[person1].birthday < bdstart ||
            person_map[person1].birthday > bdend) continue;

        // checks whether person1 likes artist
        if (person_score[person1] != ARTIST_FAN) continue;

        for (unsigned long knows2 = person_map[person1].knows_first;
             knows2 < person_map[person1].knows_first + person_map[person1].knows_n;
             knows2++)
        {
            unsigned int person2 = knows_map[knows2];

            signed char score2 = person_score[person2];
            if (score2 < 2) continue; // checks whether person2 likely likes artist

            // checks whether person1 and friend2 live in the same city
            if (person_map[person1].location != person_map[person2].location) continue;

            for (unsigned long knows3 = person_map[person2].knows_first;
                 knows3 < person_map[person2].knows_first + person_map[person2].knows_n;
                 knows3++)
            {
                unsigned int person3 = knows_map[knows3];

                // check that person3 > person2 (in order to avoid duplicate triangles)
                if (person2 > person3) continue; // only report triangle when person2 < person3

                // checks whether person1 and person3 live in the same city
                if (person_map[person1].location != person_map[person3].location) continue;

                signed char score3 = person_score[person3];
                if (score3 < 2) continue; // checks whether person3 likely likes artist

                for (unsigned long knows4 = person_map[person3].knows_first;
                     knows4 < person_map[person3].knows_first + person_map[person3].knows_n;
                     knows4++)
                {
                    // check whether they form a triangle, i.e. person1->person2->person3->person1
                    if (person1 != knows_map[knows4]) continue;

                    // add Result record
                    results[result_length].person1_id = person_map[person1].person_id;
                    results[result_length].person2_id = person_map[person2].person_id;
                    results[result_length].person3_id = person_map[person3].person_id;
                    results[result_length].score = score2 + score3;
                    if (++result_length >= result_maxsize) { // realloc result array if we run out of space
                        results = (Result*) realloc(results, (result_maxsize*=2) * sizeof(Result));
                    }
                    break;
                }
            }
        }
    }

    // sort the results
    qsort(results, result_length, sizeof(Result), &result_comparator);

    // output
    for (unsigned int i = 0; i < result_length; i++) {
        fprintf(outfile, "%d|%d|%lu|%lu|%lu\n", qid, results[i].score,
                results[i].person1_id, results[i].person2_id, results[i].person3_id);
    }
    free(results);
}

#define QUERY_FIELD_QID 0
#define QUERY_FIELD_A1 1
#define QUERY_FIELD_A2 2
#define QUERY_FIELD_A3 3
#define QUERY_FIELD_A4 4
#define QUERY_FIELD_BS 5
#define QUERY_FIELD_BE 6

void query_line_handler(unsigned char nfields, char** tokens) {
	unsigned short q_id, q_artist, q_bdaystart, q_bdayend;
	unsigned short q_artists[3];

	q_id         = atoi(tokens[QUERY_FIELD_QID]);
	q_artist     = atoi(tokens[QUERY_FIELD_A1]);
	q_artists[0] = atoi(tokens[QUERY_FIELD_A2]);
	q_artists[1] = atoi(tokens[QUERY_FIELD_A3]);
	q_artists[2] = atoi(tokens[QUERY_FIELD_A4]);
	q_bdaystart  = birthday_to_short(tokens[QUERY_FIELD_BS]);
	q_bdayend    = birthday_to_short(tokens[QUERY_FIELD_BE]);
	
	query(q_id, q_artist, q_artists, q_bdaystart, q_bdayend);
}

int main(int argc, char *argv[]) {
	unsigned long file_length;
	if (argc < 4) {
		fprintf(stderr, "Usage: [datadir] [query file] [results file]\n");
		exit(1);
	}

	/* memory-map files created by loader */
	interest_map = (unsigned short *) mmapr(makepath(argv[1], (char*) "interest", (char*) "bin"), &file_length);
	knows_map    = (unsigned int *)   mmapr(makepath(argv[1], (char*) "knows",    (char*) "bin"), &file_length);
	person_map   = (Person *)         mmapr(makepath(argv[1], (char*) "person",   (char*) "bin"), &file_length);
	person_num   = file_length/sizeof(Person);

  	outfile = fopen(argv[3], "w");  
  	if (outfile == NULL) {
  		fprintf(stderr, "Can't write to output file at %s\n", argv[3]);
		exit(-1);
  	}
  	/* run through queries */
	parse_csv(argv[2], &query_line_handler);
	fclose(outfile);
	return 0;
}
