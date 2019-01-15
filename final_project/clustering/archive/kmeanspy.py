import re
import sys
import random as rd
from pyspark import SparkContext
from math import sqrt, cos
import unicodedata
import matplotlib.pyplot as plt


# given a (latitude/longitude) point and an array of current center points,
# returns the index in the array of the center closest to the given point
def closestPoint(point, Centroids, measure):
    # print Centroids
    dists = []
    if measure == "GreatCircle":
        for x in Centroids:
            dists.append(GreatCircleDistance(point, x.location))
    elif measure == "Euclidean":
        for x in Centroids:
            dists.append(EuclideanDistance(point, x.location))
    else:
        print >> sys.stderr, "Unspecified distance measure"
        return exit(-1)
    min_dist = min(dists)
    return dists.index(min_dist)

# given two points, returns the great circle distance of the two
def GreatCircleDistance(p1, p2):
    radius = 6371  # radius of Earth
    x = (p2[1] - p1[1]) * cos((p2[0] + p1[0]) / 2)
    y = p2[0] - p1[0]
    return radius * sqrt(x * x + y * y)

# given two points, returns the Euclidean distance of the two
def EuclideanDistance(p1, p2):
    # p1 = unicodedata.decimal(p1)
    # print type(p1)
    # print type(p1[0])

    dist = sqrt(pow(float(p1[0]) - float(p2[0]), 2) + pow(float(p1[0]) - float(p2[0]), 2))
    # print dist
    return dist

# given two points, return a point which is the sum of the two points
def addPoints(p1, p2):
    p3 = [0,0]
    p3[0] = float(p1[0]) + float(p2[0])
    p3[1] = float(p1[1]) + float(p2[1])
    # print "sum"
    # print p3
    return p3


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: k-means"
        exit(-1)

    # read data
    sc = SparkContext()
    data = sc.textFile(sys.argv[1])
    k = int(sys.argv[2])         # parameter k
    measure = sys.argv[3]   # parameter distance measure
    convergeDist = 0.11                         # default convergeDist
    done = False                             # flag of whether k-means calculation is done
    
    # pre-process
    f= data.map(lambda x: x.replace('u\'', "")).map(lambda x: x.replace('\'',"")).map(lambda x: x.replace('(',""))
    points= f.map(lambda x:x.split(",")).map(lambda x:[i.strip() for i in x]).map(lambda x: (float(x[0]),float(x[1])))
   
    # pointSamples = points.takeSample(False, 100)
    pointSamples = points.collect()

    # points = points
    # randomly select k centroids
    centroids = points.takeSample(False,k)
    # for c in centroids:
    #     print c


    class Centroid:
      def __init__(self, location, index):
        self.location = location
        self.index = index
        self.cluster = []
    # p1 = Person("John", 36)    
      def add(self, point):
        self.cluster.append(point)

    # add element to object list
    Centroids = []
    i = 0
    for element in centroids:
        # C = Centroid((element[0],element[1]), i)
        C = Centroid(element, i)
        Centroids.append(C)
        i = i+1


    # for C in Centroids:
    #     print "Centroids"
    #     print C.index
    #     print C.location

    n =0
    flag = False
    while not done:
    # for n in range(3):
        print "n " + str(n)
        n = n+1
    # assign points to centroids
        for p in pointSamples:
        # for p in pointSamples.collect():
            # get the closest centroid index for a point p
            closest = closestPoint(p,Centroids,measure)
            Centroids[closest].add(p)

    # generate new centroids
        i =0
        for C in Centroids:
            # print C.cluster
            sum = [0,0]
            j=0
            for member in C.cluster:
                # print str(j) + " out of " + str(len(C.cluster))
                j = j+1
                # print "member-------"
                # print member
                # print "nextmember-------"
                # print "localSum-------"
                sum = addPoints(member,sum)
                # print sum

            # end of looping members
            print "totalSum-------"
            print sum
            print "centroid index" + str(i)
            print "old center"
            print C.location

            center = [sum[0]/len(C.cluster), sum[1]/len(C.cluster)]
            print "center"
            print center
            newC = Centroid(center, i)
            dist = EuclideanDistance(C.location, newC.location)
            C = newC
            # plt.scatter(newC.location[0], newC.location[1],s=0.01, c='r')
            print "new center"
            print newC.location
            Centroids[i]=C
            i = i+1
            # print(newC)
            # print "dist" + str(dist)
            if dist < convergeDist:
                flag = True
            else:
                flag = False

        if flag is True:
            done = True

    for C in Centroids:
        print C.location
    print len(Centroids)


