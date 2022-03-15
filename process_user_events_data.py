#Process event details
import pandas as pd 
import json


def binary_search(arr, low, high, x):
    if high >= low:
        mid = (high + low) // 2
        if arr[mid] == x:
            return mid
        elif arr[mid] > x:
            return binary_search(arr, low, mid - 1, x)
        else:
            return binary_search(arr, mid + 1, high, x)
    else:
        return -1
 



data_set_path = "./dataset/event-recommendation-engine-challenge"

list_of_events = pd.read_csv(data_set_path+'/events.csv.gz', header=0)
mapped_event_details = {}
headings = ["event_id", "user_id", "start_time", "city", "state", "zip", "country", "lat", "lng"]
for index in range(list_of_events['event_id']):
    selected_col = [ 
        list_of_events['event_id'][index],
        list_of_events['user_id'][index],
        list_of_events['start_time'][index],
        list_of_events['city'][index],
        list_of_events['state'][index],
        list_of_events['zip'][index],
        list_of_events['country'][index],
        list_of_events['lat'][index],
        list_of_events['lng'][index],
    ]
    mapped_event_details[int(list_of_events['event_id'][index])] = selected_col
# list_of_events = [int(current_event_id) for current_event_id in list_of_events['event_id']]

tweets = []
list_of_ids = []
num = 0
while num < 19:
    temp_file_number = "" if num == 0 else str(num)
    for line in open(data_set_path+'/json/events'+temp_file_number+'.json', 'r'):
        try:
            temp_storage = json.loads(line)
            event_id = int(temp_storage["event_url"].split('/')[-2])
            list_of_ids.append(event_id)
        # tweets.append(json.loads(line))
        except:
            print("An exception occurred")
    num += 1

fount_ids = 0
list_of_events.sort()
list_of_ids.sort()

# for i in list_of_events:
    # result = binary_search(list_of_ids, 0, len(list_of_ids)-1, i)
    # if result != -1:
        # fount_ids += 1
# over_lap_count = count(set(list_of_events) - set(list_of_ids))

#writing formatted data 
# cities = pd.DataFrame([['Sacramento', 'California'], ['Miami', 'Florida']], columns=['City', 'State'])
# cities.to_csv('cities.csv')
print("hello")