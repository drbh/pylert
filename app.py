from datetime import date, datetime, timedelta
from dateutil import parser
import redis
import json
import time
import requests

# PLEASE CHOOSE to run based on time message
# if both are set true, then the messages
# specified time will be used

# recieved
SHOULD_RUN_BASED_ON_MESSAGE_RECEIVE_TIME = False

# says in origin attr
SHOULD_RUN_BASED_ON_MESSAGES_SPECIFIED_TIME = True

# set alert threshold seconds up herrr
ALERT_THRESHOLD_SECONDS = 2000000000000

# set your webhook url up here
SLACK_WEB_HOOK_URL = 'https://hooks.slack.com/services/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'

# Redis DB for I/O (using lists to push and pop)
POOL = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
my_server = redis.Redis(connection_pool=POOL)

# init out dict thats gonna be our cache
cache = {}

def sent_slack_message(message):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"text": message})
    response = requests.post(
        SLACK_WEB_HOOK_URL,
        headers=headers,
        data=data)
    return 0


def json_serial(obj):
    if isinstance(obj, (datetime, date, timedelta)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def process(above):
    obj_in = json.loads(above)
    reference = obj_in['id']
    if reference not in cache:
        cache[reference] = {
            "id": reference,
            "starttime": datetime.now(),
            "count": 0,
            "flagrun": 0,
            "flagstart": parser.parse(obj_in["origin"]["datetime"]),
            "haventsentalert": True
        }
    entry = cache[reference]
    entry["count"] = entry["count"] + 1
    entry["seentime"] = datetime.now()
    entry["timesincestart"] = entry["seentime"] - entry["starttime"]
    entry["lastvalue"] = obj_in["value"]
    entry["lasttime"] = parser.parse(obj_in["origin"]["datetime"])
    entry["timesincelast"] = datetime.now() - entry["lasttime"]
    if entry["lastvalue"] == 1:
        entry["flagrun"] = entry["flagrun"] + 1
    else:
        entry["flagrun"] = 0

        if SHOULD_RUN_BASED_ON_MESSAGE_RECEIVE_TIME:
            entry["flagstart"] = datetime.now()

        if SHOULD_RUN_BASED_ON_MESSAGES_SPECIFIED_TIME:
            entry["flagstart"] = entry["lasttime"]

        entry["haventsentalert"] = True

    entry["timesinceflagstart"] = datetime.now() - entry["flagstart"]
    if entry["timesinceflagstart"] > timedelta(seconds=ALERT_THRESHOLD_SECONDS):
        if entry["haventsentalert"]:
            my_server.lpush("alert", json.dumps(entry, default=str, indent=4))
            entry["haventsentalert"] = False
            # send notification
            sent_slack_message(
                '```' + json.dumps(entry, default=str, indent=4) + '```')

# sent_slack_message('```'+"APP START"+'```')
while True:
    res = my_server.rpop("test")
    if res is not None:
        process(res)

    print("\033c")
    print(json.dumps(cache, default=str, indent=4))
    # time.sleep(0.01)
