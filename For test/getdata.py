#!/usr/bin/env python
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import *


s = xmlrpclib.ServerProxy("http://127.0.0.1:51236")

key = "/hello.txt&&data"
#key = "/mytestdir/southamerica/chile/santiago.txt&&data"
res = s.get(Binary(key))
print "getting key = ",key 
if "value" in res:
	print pickle.loads(res["value"].data)
else:
	print "None"
#print pickle.loads(s.list_contents())
'''
res = s.getserver()
alldata = pickle.loads(res)
print "\n\n"
print "alldata =======> ",alldata

'''
#logic = s.terminate()