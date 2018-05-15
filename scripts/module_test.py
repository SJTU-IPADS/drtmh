#!/usr/bin/env python


from run_test3 import calculate_results
import sys


if __name__ == "__main__":
    assert(len(sys.argv) > 1)
    log_file_name = sys.argv[1]

    print(calculate_results(log_file_name))
