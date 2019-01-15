import re
import sys
import random as rd
from pyspark import SparkContext
from math import sqrt, cos , sin, radians, asin
import unicodedata
import matplotlib.pyplot as plt
import numpy as np
# from math import sqrt, cos , sin, radians, asin


# usage:
# spark-submit --master local[3] {kmeans.py} {file:/home/cloudera/cse427s/final_project/data/devicestatus_etl} 4 GreatCircle {outpath}


# given a (latitude/longitude) point and an array of current center points,
# returns the index in the array of the center closest to the given point

def closestPoint(point, Centroids, measure):
    # print "calclate dist"
    # print point
    point= [float(point[0]), float(point[1])]
    dists = []
    if measure == "GreatCircle":
        for x in Centroids:
            x= [float(x[0]), float(x[1])]
            # print x
            dists.append(GreatCircleDistance(point, x))
    elif measure == "Euclidean":
        for x in Centroids:
            # print "x"
            # print x
            x= [float(x[0]), float(x[1])]
            dists.append(EuclideanDistance(point, x))
    else:
        print >> sys.stderr, "Unspecified distance measure"
        return exit(-1)
    min_dist = min(dists)
    # print min_dist
    return dists.index(min_dist)

# given two points, returns the great circle distance of the two
def GreatCircleDistance(p1, p2):

    lon1 = p1[0]
    lat1 = p1[1]
    lon2 = p2[0]
    lat2 =p2[1]

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6371* c
    return km
    # p1[0] lon1
    # p1[1] lat1
    # p2[0] lon2
    # p2[1] lat2
    radius = 3745  # radius of Earth
    x = (p2[0] - p1[0]) * cos((p2[1] + p1[1]) / 2)
    y = p2[1] - p1[1]
    return radius * sqrt(x * x + y * y)

# given two points, returns the Euclidean distance of the two
def EuclideanDistance(p1, p2):
    dist = sqrt(pow(float(p1[0]) - float(p2[0]), 2) + pow(float(p1[1]) - float(p2[1]), 2))
    return dist

# given two points, return a point which is the sum of the two points
def addPoints(p1, p2):
    p3 = [0,0]
    p3[0] = p1[0] + p2[0]
    p3[1] = p1[1] + p2[1]
    return p3


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: k-means"
        exit(-1)
    if len(sys.argv) == 5:
        outPath = sys.argv[4]

    # input/variables
    sc = SparkContext()
    data = sc.textFile(sys.argv[1])
    k = int(sys.argv[2])         # parameter k
    measure = sys.argv[3]   # parameter distance measure

    print "hi"

    # pre-process deviceData
    # f= data.map(lambda x: x.replace('u\'', "")).map(lambda x: x.replace('\'',"")).map(lambda x: x.replace('(',""))
    # points= f.map(lambda x:x.split(",")).map(lambda x:[i.strip() for i in x]).map(lambda x: (float(x[1]),float(x[0])))

    # pre-process dbpedia
    points = data.map(lambda x: x.split(' ')).filter(lambda x: len(x)==3).map(lambda x: (float(x[1]),float(x[0])))
    print points.take(3)

    # pre-process clouddata
    # points = data.map(lambda x: x.split(" ")).map(lambda x: (float(x[3]),float(x[2])))

    # pre-process synthetic location
    # points= data.map(lambda x: x.split("\t")).filter(lambda x: x[0] != u'').filter(lambda x: len(x)==3).map(lambda x: (float(x[1]),float(x[0])))

    # choose sample
    # pointSamples = points.sample(False, 10)

    # choose all
    pointSamples = points


    # randomly select k centroids, return a list
    # CLocations is a list
    Clocations = pointSamples.takeSample(False,k)
    Clocations = [(-92.428695388438356, 37.678402383180696), (-118.81807632238025, 43.528540831125518), (1.3053046110130599, 48.868905688982494), (21.476507451975102, -1.3578519410476355), (-61.771292915188468, -22.111241199707109), (130.57602342733574, 16.843211248420044), (50.822517982940937, 34.485121611056258), (-77.161138602937797, 39.01460966158723), (18.854495984628645, 49.996905788421309), (83.850651858338026, 21.604277898029569)]



    centroids = sc.parallelize(Clocations)
    centroids = centroids.zipWithIndex().map(lambda (k,v): (v,k))

#   lets 

    # function related variables
    convergeDist = 0.5                  # default convergeDist
    done = False                        # flag of whether k-means calculation is done
    n =0
    flag = False


    while not done:
    # for n in range(3):
        print "n " + str(n)
        n = n+1

        # k=index of centroid  v= ocation of datapoint
        # return location
        closest = pointSamples.map(lambda x: (closestPoint(x, Clocations, measure), x))
        closest=closest.persist()

        number = closest.countByKey()    
        # calculate new centroid
        sum = closest.reduceByKey(addPoints)
        # print "sum"
        # print sum.take(4)
        # print "result"
        newClocations = sum.map(lambda (index, pointsSum): (index, (pointsSum[0]/number[index],pointsSum[1]/number[index])))
        newClocations=newClocations.persist()

        print "oldC, list"
        print Clocations
        print "oldC, rdd"
        print centroids.take(4)




        # try not use rdd?

        # join first for calculating dist
        join = centroids.join(newClocations)
        join=join.persist()

        print "join"
        print centroids.take(2)
        print newClocations.take(2)
        print "join result"
        print join.take(5)
        if measure == "Euclidean":
            dist = join.map(lambda x: EuclideanDistance(x[1][0],x[1][1]))
        if measure == "GreatCircle":
            dist = join.map(lambda x: GreatCircleDistance(x[1][0],x[1][1]))


        # updatae Clocations
        Clocations = newClocations.collect()
        Clocations.sort()
        print "sorted"
        print Clocations
        temp =[]
        for C in Clocations:
            temp.append(C[1])
        Clocations = temp


        print "update"
        print Clocations

        # update centroids
        centroids = sc.parallelize(Clocations).zipWithIndex().map(lambda (k,v): (v,k))
        # centroids = newClocations.collectAsMap()


        distList = dist.collect()
        print distList

        for d in distList:
            if d<convergeDist:
                flag=True
            else:
                flag=False
                break

        if flag is True:
            print "flag"
            done = True





# post processing
    # print prep
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    plt.axis('equal')

    print ("Centroids-----------------------------")
    # print centroids
    print Clocations
    # visulization
    print ("clusters-----------------------------")
    # print cluster
    output = closest.groupByKey()

    if len(sys.argv) == 5:
        output.repartition(1).saveAsTextFile(outPath)

    # collect clusters for visualization
    clusters = output.map(lambda x: (x[0], x[1])).collect()
    # print clusters

    # colors = "rbgcmykw"
    colors = ["aqua","aquamarine","beige","chocolate","crimson","coral","indigo","lime","purple","red", "blue" , "green", "orange", "orchid","lavender","goldenrod"]
    color_index = 0

    for cluster in clusters:
        # print cluster[0]
        # visualize cluster
        npCluster = np.reshape(list(cluster[1]), (-1,2))
        ax1.scatter(npCluster[:,0], npCluster[:,1],s=1,facecolors=colors[color_index],edgecolor='none', marker='o',alpha=0.2)
        color_index = color_index + 1 

    # visualize centroids
    npClocations = np.reshape(Clocations,(-1,2))
    ax1.scatter(npClocations[:,0], npClocations[:,1],s=5,c='black')
    # plt.show()
    plt.savefig('k8.png', dpi =2000)

