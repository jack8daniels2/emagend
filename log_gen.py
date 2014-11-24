#!/usr/bin/env python
import sys, random

def main(count):
    for x in xrange(count):
        first_number = random.randint(0, 255)
        second_number = random.randint(0, 255)
        third_number = random.randint(0, 255)
        fourth_number = random.randint(0, 255)
        yield "%d.%d.%d.%d\n" % (first_number, second_number, third_number, fourth_number)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(int(sys.argv[1]))
