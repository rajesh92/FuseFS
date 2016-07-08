To run the code:

Sample Arguments:

python FileSystem.py fusemount http://127.0.0.1:51234

python mediator.py 3 5 http://127.0.0.1:51200 http://127.0.0.1:51235 http://127.0.0.1:51236 http://127.0.0.1:51237 http://127.0.0.1:51238 http://127.0.0.1:51239

python dataserver.py 51235 &
python dataserver.py 51236 &
python dataserver.py 51237 &
python dataserver.py 51238 &
python dataserver.py 51239 &
python metaserver.py 51200


Tested successfully for Qr = 3 Qw = 5


