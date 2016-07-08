#!/usr/bin/env python
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import *


s = xmlrpclib.ServerProxy("http://127.0.0.1:51235")

print pickle.loads(s.list_contents())
'''
res = s.getserver()
alldata = pickle.loads(res)
print "\n\n"
print "alldata =======> ",alldata

'''
#logic = s.terminate()