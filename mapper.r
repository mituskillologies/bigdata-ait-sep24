#!/usr/bin/Rscript
# Wordcount mapper of R-Programming
input = file('stdin', open = 'r')
while (length(line <- readLines(input, n = 1)) > 0) {
	words = unlist(strsplit(line, "[[:space:]]+"))
	for (word in words)
		cat(word,"\t1\n", sep='')
}
close(input)
