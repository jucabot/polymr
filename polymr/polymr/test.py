"""
    Test protocol
"""
from polymr.common import TestMapRed1, TestMapRed2, TestMapRed3

print "Test engine - Single core"
m = TestMapRed1("_test/sample.txt","_test/count_sample1-single.txt")
m.run(debug=True)
m = TestMapRed2("_test/sample.txt","_test/count_sample2-single.txt")
m.run(debug=True)
m = TestMapRed3("_test/sample.txt","_test/count_sample3-single.txt")
m.run(debug=True)

print "Test engine - multi core"
m = TestMapRed1("_test/sample.txt","_test/count_sample1-multi.txt")
m.run()
m = TestMapRed2("_test/sample.txt","_test/count_sample2-multi.txt")
m.run()
m = TestMapRed3("_test/sample.txt","_test/count_sample3-multi.txt")
m.run()

print "Test engine - local hadoop"
m = TestMapRed1("_test/sample.txt","_test/count_sample1-hadoop.txt")
m.run(engine="local-hadoop")
m = TestMapRed2("_test/sample.txt","_test/count_sample2-hadoop.txt")
m.run(engine="local-hadoop")
m = TestMapRed3("_test/sample.txt","_test/count_sample3-hadoop.txt")
m.run(engine="local-hadoop")

print "Test engine - cluster"
m = TestMapRed1("_test/sample.txt","_test/count_sample1-cluster.txt")
m.run(engine="cluster")
m = TestMapRed2("_test/sample.txt","_test/count_sample2-cluster.txt")
m.run(engine="cluster")
m = TestMapRed3("_test/sample.txt","_test/count_sample3-cluster.txt")
m.run(engine="cluster")

print "Test completed"