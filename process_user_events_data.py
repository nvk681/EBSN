#Process event details
from pydoc import describe
import pandas as pd 
import json
import re

TAG_RE = re.compile(r'<[^>]+>')

def remove_tags(text):
    html_stripped_string = TAG_RE.sub('', text)
    return html_stripped_string.replace(",", "")


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
list_if_event_ids_points_of_intrest = [] 
headings = ["event_id", "user_id", "start_time", "city", "state", "zip", "country", "lat", "lng", "name", "group_name"]


for index in range(len(list_of_events['event_id'])):
    selected_col = [ 
        str(list_of_events['event_id'][index]),
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
    list_if_event_ids_points_of_intrest.append(int(list_of_events['event_id'][index]))


list_if_event_ids_points_of_intrest.sort()
num = 0


while num < 19:
    temp_file_number = "" if num == 0 else str(num)
    for line in open(data_set_path+'/json/events'+temp_file_number+'.json', 'r'):
        try:
            temp_storage = json.loads(line)
            event_id = int(temp_storage["event_url"].split('/')[-2])
            result = binary_search(list_if_event_ids_points_of_intrest, 0, len(list_if_event_ids_points_of_intrest)-1, event_id)
            if result != -1:
                details_of_intrest = []
                
                name = remove_tags(temp_storage["name"])
                # descrition = remove_tags(temp_storage["description"])
                group = remove_tags(temp_storage["group"]["name"])
                city = remove_tags(temp_storage["venue"]["country"])
                state = remove_tags(temp_storage["venue"]["state"])
                country = remove_tags(temp_storage["venue"]["country"])
                lat = (temp_storage["venue"]["lat"])
                lon = (temp_storage["venue"]["lon"])
                print("Event ID: ", event_id)
                # print("Name: ", name)
                # print("Description: ", descrition)
                # print("Group Name: ", group)
                event_details = mapped_event_details[event_id]
                event_details[3] = city
                event_details[4] = state
                event_details[6] = country
                event_details[7] = lat
                event_details[8] = lon
                print("Before: ", event_details)
                event_details = event_details + [name, group]
                print("After: ", event_details)
                mapped_event_details[event_id] = event_details
        except:
            print("An exception occurred")
    num += 1

for key in mapped_event_details:
    if len(mapped_event_details[key]) == 9:
        mapped_event_details[key] = mapped_event_details[key] + ["", ""]


#writing formatted data 
cities = pd.DataFrame(mapped_event_details.values(), columns = headings)
cities.to_csv('processed_data_without_description.csv')
print("Hello")