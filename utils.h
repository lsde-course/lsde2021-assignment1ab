#define REPORTING_N (1<<20)
#define LINEBUFLEN 1024

typedef struct {
	unsigned long  person_id;
	unsigned short birthday;
	unsigned short location;
	unsigned long  knows_first;
	unsigned short knows_n;
	unsigned long  interests_first;
	unsigned short interest_n;
} Person;

void parse_csv(char* fname, void (*line_handler)(unsigned char nfields, char** fieldvals)) {
	long nlines = 0;

   	FILE* stream = fopen(fname, "r");
	if (stream == NULL) {
		fprintf(stderr, "Can't read file at %s\n", fname);
		exit(-1);
	}
	char line[LINEBUFLEN];
	char* tokens[10];
	unsigned int col, idx;
	tokens[0] = line;

	while (fgets(line, LINEBUFLEN, stream)) {
		col = 0;
		// parse the csv line into array of strings
		for (idx=0; idx<LINEBUFLEN; idx++) { 
			if (line[idx] == '|' || line[idx] == '\n') {
				line[idx] = '\0';
				col++;
				tokens[col] = &line[idx+1];
			} // lookahead to find end of line
			if (line[idx+1] == '\0') {
				break;
			}
		}
		(*line_handler)(col, tokens);
		nlines++;
		if (nlines % REPORTING_N == 0) {
			printf("%s: read %lu lines\n", fname, nlines);
		}
	}
	fclose(stream);
}


FILE* open_binout(char* filename) {
	FILE* outfile;
	outfile = fopen(filename, "wb");
	if (outfile == NULL) {
		fprintf(stderr, "Could not open %s for writing\n", filename);
		exit(-1);
	}
	return outfile;
}

/* 
	convert birthday from date to four-digit integer with month and day 
	easier comparisions and less storage space
*/
unsigned short birthday_to_short(char* date) {
	unsigned short bdaysht;
	char dmbuf[3];
	dmbuf[2] = '\0';
	dmbuf[0] = *(date + 5);
	dmbuf[1] = *(date + 6);
	bdaysht = atoi(dmbuf) * 100;
	dmbuf[0] = *(date + 8);
	dmbuf[1] = *(date + 9);
	bdaysht += atoi(dmbuf);
	return bdaysht;
}

void* mmapopen(char* filename, unsigned long *filelen, int write) {
	int fd;
	struct stat sbuf;
	void *mapaddr;
	int fopenmode = write ? O_RDWR : O_RDONLY;
	int mmapmode = write ? PROT_WRITE : PROT_READ;

	if ((fd = open(filename, fopenmode)) == -1) {
		fprintf(stderr, "failed to open %s\n", filename);
		exit(1);
    }

    if (stat(filename, &sbuf) == -1) {
        fprintf(stderr, "failed to stat %s\n", filename);
        exit(1);
    }
    
    mapaddr = mmap(0, sbuf.st_size, mmapmode, MAP_SHARED, fd, 0);
    if (mapaddr == MAP_FAILED) {
        fprintf(stderr, "failed to mmap %s\n", filename);
        exit(1);
    }
    *filelen = sbuf.st_size;
    return mapaddr;
}

void* mmapr(char* filename, unsigned long *filelen) {
	return mmapopen(filename, filelen, 0);
}

void* mmaprw(char* filename, unsigned long *filelen) {
	return mmapopen(filename, filelen, 1);
}

char* makepath(char* dir, char* file, char* ext) {
	char* out = (char*) malloc(strlen(dir)+strlen(file)+strlen(ext)+3);
	sprintf(out, "%s/%s.%s", dir, file, ext);
	return out;
}


