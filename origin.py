from datetime import date, datetime, timedelta
import redis
import json
import time
import random

POOL = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
my_server = redis.Redis(connection_pool=POOL)

counter = 0

while True:
	counter = counter + 1
	my_server.lpush("test", 
		json.dumps(
			{
				"id": "camera+feed+"+str(counter%3),
				"origin": {
					"datetime": datetime.now(),
				},
				"value": random.choice([0,1,1,1,1,1,1,1,1,1,1,1])
			}, 
			default=str
		)
	)
	# print(counter)
	time.sleep(0.1)