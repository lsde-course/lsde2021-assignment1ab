# remove the # in the following line to enable reorg compilation (and running)
all: cruncher # reorg

cruncher: cruncher.c utils.h
	g++ -I. -O3 -o cruncher cruncher.c

loader: loader.c utils.h
	g++ -I. -O3 -o loader loader.c

reorg: reorg.c utils.h
	g++ -I. -O3 -o reorg reorg.c

clean:
	rm -f loader cruncher reorg
