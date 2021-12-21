#!/usr/bin/env python
import sys
from log_parsing_algorithm import Drain

def drain_parse(input_dir, output_root, log_file, log_format, regex, st = 0.5, depth = 4):
    output_dir = output_root + 'Drain_result/'

    parser = Drain.LogParser(log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex)
    parser.parse(log_file)
