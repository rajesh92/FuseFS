#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename),
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import sys
import time

data_pn={}
s = {}
#meta_s = xmlrpclib.ServerProxy('http://127.0.0.1:51200')
#global meta_s

# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Remove expired entries
    rect={}
    arr={}
    #print "SIMP:LE HT GET"
    temp = str(key)
    global qr,qw
    global meta_s

    #print "\n\nPRINTING LIST OF CONTENTS --->>>> \n", s[0].list_contents()
    #print "\n\nPRINTING LIST OF CONTENTS uplicked --->>>> \n", pickle.loads(s[0].list_contents())
    
    

    if temp[-4:] == "data":
      i = 0
      phi = 0

      for i in range(0,int(qw)): #qr to qw
        #for i in data_pn:
        #print "here i is ---> ",i
        try:
          rv = s[i].get(key)
          #print "\n\nrv -----> ",i, rv
          rect[i] = rv
          tempp = pickle.loads(rect[i]["value"].data) # if the key is not present in server it will execute the exception block
          arr[phi] = i
          #tempp = pickle.loads(rect[arr[phi]]["value"].data) # if the key is not present in server it will execute the exception block
          #arr.insert(phi, i)
          phi = phi + 1
          print "\nSever ", i, " is available for reading..., phi = ", phi
        except Exception,e:
          print "\nSever or key", i, " is not available for reading...\n Error :",e
        
        #i = i + 1
      if phi < int(qr):
        print "\nSufficient # of servers are not available for the read ..., qr = ", qr, type(qr), " phi = ", phi, type(phi)
        return {}
      
      else:
        print "\nSufficient # of servers are available for the read ..."
        max_phi = phi
        i = 0
        phi = 0
        #while arr[phi] < (int(qw)-2):
        for phi in range(int(qr)-2):
          '''
          print "\n\nSAMPLE -------------------_0__> ", pickle.loads(rect[arr[phi]]["value"].data)
          print "\n\n"
          print "\n\nSAMPLE -------------------_1__> ", pickle.loads(rect[arr[phi+1]]["value"].data)
          print "\n\n"
          print "\n\nSAMPLE -------------------_2__> ", pickle.loads(rect[arr[phi+2]]["value"].data)
          print "\n\n"
          '''
          #if pickle.loads(rect[i]["value"].data) != pickle.loads(rect[i+1]["value"].data) and pickle.loads(rect[i+1]["value"].data) != pickle.loads(rect[i+2]["value"].data) and pickle.loads(rect[i]["value"].data) == pickle.loads(rect[i+2]["value"].data):
          if pickle.loads(rect[arr[phi]]["value"].data) != pickle.loads(rect[arr[phi+1]]["value"].data) and pickle.loads(rect[arr[phi+1]]["value"].data) != pickle.loads(rect[arr[phi+2]]["value"].data) and pickle.loads(rect[arr[phi]]["value"].data) == pickle.loads(rect[arr[phi+2]]["value"].data): 
            #rect[i+1] corrupt
            #rect[i+1] = rect[i]
            rect[arr[phi+1]] = rect[arr[phi]]
            logic = s[arr[phi+1]].put(key, Binary(rect[arr[phi]]["value"].data), 10000)
            #print "Case 1 executed"
          
          #if pickle.loads(rect[i]["value"].data) == pickle.loads(rect[i+1]["value"].data) and pickle.loads(rect[i+1]["value"].data) != pickle.loads(rect[i+2]["value"].data) and pickle.loads(rect[i]["value"].data) != pickle.loads(rect[i+2]["value"].data):
          if pickle.loads(rect[arr[phi]]["value"].data) == pickle.loads(rect[arr[phi+1]]["value"].data) and pickle.loads(rect[arr[phi]]["value"].data) != pickle.loads(rect[arr[phi+2]]["value"].data) and pickle.loads(rect[arr[phi]]["value"].data) != pickle.loads(rect[arr[phi+2]]["value"].data):
            #rect[i+2] corrupt
            #rect[i+2] = rect[i]
            rect[arr[phi+2]] = rect[arr[phi]]
            logic = s[arr[phi+2]].put(key, Binary(rect[arr[phi]]["value"].data), 10000)
            #print "Case 2 executed"
          
          #if pickle.loads(rect[i]["value"].data) != pickle.loads(rect[i+1]["value"].data) and pickle.loads(rect[i+1]["value"].data) == pickle.loads(rect[i+2]["value"].data) and pickle.loads(rect[i]["value"].data) != pickle.loads(rect[i+2]["value"].data):
          if pickle.loads(rect[arr[phi]]["value"].data) != pickle.loads(rect[arr[phi+1]]["value"].data) and pickle.loads(rect[arr[phi+1]]["value"].data) == pickle.loads(rect[arr[phi+2]]["value"].data) and pickle.loads(rect[arr[phi]]["value"].data) != pickle.loads(rect[arr[phi+2]]["value"].data):  
            #rect[i] corrupt
            #rect[i] = rect[i+1]
            rect[arr[phi]] = rect[arr[phi+1]]
            logic = s[arr[phi]].put(key, Binary(rect[arr[phi+1]]["value"].data), 10000)
            #print "Case 3 executed"
    
        return rect[arr[0]]

    else:
      rv = meta_s.get(key)
      return rv


        
    #print "\n Rect0value.data ---->", Binary(rect[0]["value"]).data
    
    #print "\n PICKLEvalue.data ---->", pickle.loads(Binary(rect[0]["value"]).data)
    
    #print "GETTING KEY = ",rv, "where key is ", key
    
    # return rect[arr[0]]

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    end = datetime.now() + timedelta(seconds = ttl)
    #print "SIMP:LE HT PUT"
    global qr,qw
    #print qr,qw
    counterr = 0
    #print "key = ",key, "value = ",value
    temp = str(key)
    #if key == Binary("meta")
    global meta_s

    if (temp[-4:] == "data"): # or (temp[-5:] == "nodes"):
      for i in data_pn:

        connected = False
        while connected != True: 
          try:
            connected = s[i].tryconnect() #checks if i is connected or not
          except:
            #print "\n Server ", i," is not connected ... \n"
            print "Waiting for connection ..."
            time.sleep(1)
        #print "general loop server ",i

        ## TODO : Change the following logic.....
        
        #while connected != True: #wait and tell user that write is taking too long
          #print "Waiting for connection"
          #connected = s[i].tryconnect()
        if i > 0:
          if s[i-1].count() > s[i].count():
            print "server ", i, " has restarted"
            res = s[i-1].getserver()
            #print "\n\n"
            #print "res ========> ",res
            #print "\n\n"
            alldata = pickle.loads(res)
            #print "\n\n"
            #print "alldata =======> ",alldata
            #print "\n\n"
            #print "dumps =======> ",pickle.dumps(alldata)
            logic = s[i].putserver(pickle.dumps(alldata))
        if i == 1:
          if s[i-1].count() < s[i].count():
            print "server ", i-1, " has restarted!!!!! "
            res = s[i].getserver()
            #alldata = pickle.loads(res["value"].data)
            alldata = pickle.loads(res)
            logic = s[i-1].putserver(pickle.dumps(alldata))
        ##print "connected!!!!"
        
      for i in data_pn:
        counterr = counterr+1
        logic = s[i].put(key, value, 10000)
    
    else:
      logic = meta_s.put(key, value, 10000)
    
    #print "PUT DONE",counterr
    return True
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  '''
  if len(sys.argv) < 5 :
    print 'usage: %s <mountpoint> <qr> <qw> <meta_pn> <data_pn_0>....<data_pn_n>' % sys.argv[0]
    exit(1)
  '''

  print 'usage: python %s <mountpoint> <qr> <qw> <meta_pn> <data_pn_0>....<data_pn_n>' % sys.argv[0]
  ol={}
  #print optlist
  #print args
  #print "\nSpace\n\n"
  global qr,qw
  # TODO: Modular code 
  qr = args[0]
  qw = args[1]
  meta_pn = args[2]
  print "\nMeta Server Port No: ",meta_pn
  global meta_s
  meta_s = xmlrpclib.ServerProxy(meta_pn)
  #data_pn=[]
  #data_pn[0] = 0
  index = 0  
  
  for pn in args[3:]:
    data_pn[index] = pn
    index = index + 1
  
  for xy in data_pn:
    print "Data Server", xy, " Port No : ",data_pn[xy]
    s[xy] = xmlrpclib.ServerProxy(data_pn[xy])
  
 
  for k,v in optlist:
    ol[k] = v
    print ol[k]

  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    print "helper funct called PUT\n"
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    print "HELPER GET"
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
