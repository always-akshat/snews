__author__ = 'akshat'

import psutil
import os
import gc
import sys
import cPickle

def memory_dump():
    dump = open("memory.pickle", 'w')
    for obj in gc.get_objects():
        i = id(obj)
        size = sys.getsizeof(obj, 0)
        #    referrers = [id(o) for o in gc.get_referrers(obj) if hasattr(o, '__class__')]
        referents = [id(o) for o in gc.get_referents(obj) if hasattr(o, '__class__')]
        if hasattr(obj, '__class__'):
            cls = str(obj.__class__)
            cPickle.dump({'id': i, 'class': cls, 'size': size, 'referents': referents}, dump)

rss = psutil.Process(os.getpid()).get_memory_info().rss
# Dump variables if using more than 100MB of memory
if rss > 100 * 1024 * 1024:
    memory_dump()


