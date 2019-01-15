# import sys
# from pyspark import SparkContext
# import re
# import glob


# if __name__ == "__main__":
# 	# def countJpg

# 	# init counter
# 	n = 0
# 	# read each file in folder
# 	logs = glob.glob('/home/cloudera/cse427s/hw9/countjpg_stubs/weblogs/*')
# 	# print(logs)
# 	# get a log
# 	for alog in logs:
# 		# open a log
# 	    with open (alog) as f:
# 	    	# iterate line
# 	    	for line in f:
# 	    		# if jpg
# 	    		if ('jpg' in line):
# 	    			n += 1
# 	    			# print(n)
# 	print(n)

import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) < 2:   
    print >> sys.stderr, "Usage: JPGsCount <file>"
    exit(-1)
	
  sc = SparkContext()

  counts = sc.textFile(sys.argv[1]) \
    .filter(lambda x: 'jpg' in x)
  
  print counts.count()
  sc.stop()

