#!/usr/bin/python

import os
import glob
import sys

DEPENDENCIES = ['jsoup-1.7.2.jar', 'jwat-gzip-1.0.0.jar',
                'jwat-common-1.0.0.jar', 'jwat-warc-1.0.0.jar', 'gson-2.2.4.jar',
                'log4j-1.2.17.jar']

GRADLE_ROOT = os.path.expanduser('~/.gradle')

jars = []

for root, dirs, files in os.walk(GRADLE_ROOT):
    for phile in files:
        if phile in DEPENDENCIES:
            jars.append(os.path.abspath(os.path.join(root, phile)))

ret = 0
if len(jars) != len(DEPENDENCIES):
    print 'Jar dependencies not resolved!'
    ret = 1

print ','.join(jars)
sys.exit(ret)
