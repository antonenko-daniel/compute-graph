import os, sys
import math
from datetime import datetime
from dateutil import tz

parentPath = os.path.abspath("../..")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop

EARTH_RADIUS = 6371

def length_from_lat_lon_lat(start, end):
    delta_longitude = math.radians(end[0] - start[0])
    delta_lattitude = math.radians(end[1] - start[1])
    average_lattitude = math.radians(end[1] + start[1]) / 2

    return math.sqrt((EARTH_RADIUS ** 2 ) * (delta_lattitude ** 2) + 
            (EARTH_RADIUS ** 2 ) * (math.cos(average_lattitude) ** 2) * (delta_longitude ** 2)
           )

def length_calculator_mapper(line):
    yield {"edge_id" : line["edge_id"], "length" : length_from_lat_lon_lat(line["start"], line["end"])}
    
def parse_time(string):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Russia/Moscow')
    moment = datetime.strptime(string, '%Y%m%dT%H%M%S.%f')
    moment = moment.replace(tzinfo=from_zone)
    return moment.astimezone(to_zone)

WEEKDAYS = {
    0 : 'Mon',
    1 : 'Tue',
    2 : 'Wed',
    3 : 'Thu',
    4 : 'Fri',
    5 : 'Sat',
    6 : 'Sun'
}

def duration_and_time_mapper(line):
    try:
        enter_time = parse_time(line['enter_time'])
        leave_time = parse_time(line['leave_time'])
    except Exception:
        return
    delta_t = (leave_time - enter_time).total_seconds() / 3600
    weekday = WEEKDAYS[enter_time.weekday()]
    hour = enter_time.hour
    yield {"weekday" : weekday, "hour" : hour, "delta_t" : delta_t, "edge_id" : line["edge_id"]}

def average_velocity_reducer(table):
    total_time = 0.
    total_distance = 0.
    for line in table:
        total_time += line["delta_t"]
        total_distance += line["length"]
    yield {"weekday" : line['weekday'], 'hour' : line['hour'], "speed" : total_distance / total_time}

travel_times_filename = '../data/travel_times.txt'
graph_data_filename = '../data/graph_data.txt'


graph_data = mrop.ComputeGraph(source=graph_data_filename)
graph_data.map(length_calculator_mapper)
graph_data.finalize()

# graph_data.run()
# graph_data.save_to_file('gd.txt')


travel_time = mrop.ComputeGraph(source=travel_times_filename)
travel_time.map(duration_and_time_mapper)
travel_time.finalize()

# travel_time.run()
# travel_time.save_to_file('tt.txt')

average_velocity = mrop.ComputeGraph(source=travel_time)
average_velocity.join(on=graph_data, keys=('edge_id',), strategy='inner')
average_velocity.sort(('weekday', 'hour'))
average_velocity.reduce(average_velocity_reducer, keys=('weekday', 'hour'))
average_velocity.finalize()

average_velocity.run()
average_velocity.save_to_file('average_velocity.txt')
