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
from SimpleXMLRPCServer import *

#global quit
#quit = 0
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

  def list_contents(self):
    #print "\n\nList keys ---> \n",self.data.keys()
    return pickle.dumps(self.data.keys())

  def putserver(self,things):
    #print "\n\n things ----------> \n", things
    #print " things type ---> ", type(things),"\n"
    #print " type pickle loads ---> ", type(pickle.loads(things)),"\n"
    self.data = pickle.loads(things)
    #print "\n\n self.data ----------> \n", self.data
    #print " selfdata type ---> ", type(self.data)
    return True

  def getserver(self):
    #print "\n\n self.data ----------> \n", self.data
    #print " selfdata type ---> ", type(self.data)
    #print "\n\n dumps.data ----------> \n",pickle.dumps(self.data)
    return pickle.dumps(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Remove expired entries
    self.check()
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data:
      ent = self.data[key]
      now = datetime.now()
      if ent[1] > now:
        ttl = (ent[1] - now).seconds
        rv = {"value": Binary(ent[0]), "ttl": ttl}
      else:
        del self.data[key]
    return rv

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    #print "\nvalue put is --->", value
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key.data] = (value.data, end)
    #print self.data
    #print '\n', self.data[key.data]
    return True

  def corrupt(self, key):
    # Remove expired entries
    #print "\n\nself.data ---> ", self.data 
    qvalue = "corrupted value"
    value = Binary(pickle.dumps(qvalue))
    ttl = 10000
    self.put(key,value,ttl)
    return qvalue

  def terminate(self):
    #global quit
    print "\nTerminating....\n"
    file_server.quit = 1
    return 1

  def tryconnect(self):
    #print "\nconnected!!\n"
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
      
class FileServer(SimpleXMLRPCServer):
  def serve_forever(self):
    #global quit
    self.quit = 0
    while not self.quit:
      #print "\nhandling request..." 
      self.handle_request()      

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  print args
  print "python metaserver.py <port_no>\nExample: python metaserver.py 51234 \n\n"

  port = int(args[0])
  for i in args[1:]:
    print "ignoring argument ",i

  '''
  if "--port" in ol:
    port = int(ol["--port"])  
  '''
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)

# Start the xmlrpc server
def serve(port):
  global file_server
  #file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server = FileServer(('127.0.0.1', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.count)
  file_server.register_function(sht.tryconnect)
  file_server.register_function(sht.list_contents)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.terminate)
  file_server.register_function(sht.putserver)
  file_server.register_function(sht.getserver)
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
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
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
