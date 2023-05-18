#! /usr/bin/python
import fileinput

f = open("out.txt", "a")

for line in fileinput.input():
    f.write(line)
    f.write("\n")

f.close()
