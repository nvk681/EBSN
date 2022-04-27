# Process event details
import pandas as pd 
import json
import re
import nltk
import numpy as np
import string
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer


TAG_RE = re.compile(r'<[^>]+>')

def remove_emojis(data):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)

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
    list_if_event_ids_points_of_intrest.append(int(list_of_events['event_id'][index]))


list_if_event_ids_points_of_intrest.sort()
num = 0


contraction_dict = {"ain't": "are not","'s":" is","aren't": "are not"}
contractions_re=re.compile('(%s)' % '|'.join(contraction_dict.keys()))

def expand_contractions(text, contractions_dict=contraction_dict):
    def replace(match):
        return contractions_dict[match.group(0)]
    return contractions_re.sub(replace, text)
# Expanding Contractions in the reviews

stop_words = set(stopwords.words('english'))
stop_words.add('subject')
stop_words.add('http')

def remove_stopwords(text):
    return " ".join([word for word in str(text).split() if word not in stop_words])

stemmer = PorterStemmer()
def stem_words(text):
    return " ".join([stemmer.stem(word) for word in text.split()])


lemmatizer = WordNetLemmatizer()
def lemmatize_words(text):
    return " ".join([lemmatizer.lemmatize(word) for word in text.split()])

empty_events = []

def pre_process_text_data(text):
    text = expand_contractions(text)
    text = text.lower()
    text = re.sub('[%s]' % re.escape(string.punctuation), '' , text)
    text = re.sub('W*dw*','', text)
    text = remove_stopwords(text)
    text = re.sub('(http[s]?S+)|(w+.[A-Za-z]{2,4}S*)', 'urladd', text)
    text = stem_words(text)
    text = lemmatize_words(text)
    text = re.sub(' +', ' ', text)
    return text

temporary_events = []
temporary_map = {}
last_selected = 0
event_data_map = {}
gassian = np.random.normal(2.5, 2 ,1000)
index_for_rating = 0

while num < 19:
    temp_file_number = "" if num == 0 else str(num)
    for line in open(data_set_path+'/json/events'+temp_file_number+'.json', 'r'):
        try:
            temp_storage = json.loads(line)
            event_id = int(temp_storage["event_url"].split('/')[-2])
            result = binary_search(list_if_event_ids_points_of_intrest, 0, len(list_if_event_ids_points_of_intrest)-1, event_id)
            if result != -1 or num == 0:
                details_of_intrest = []
                if result == -1:
                    temporary_events.append(event_id)
                name = remove_tags(temp_storage["name"])
                descrition = "" if "description" not in temp_storage else remove_tags(temp_storage["description"])
                city = "NA" if "venue" not in temp_storage else remove_tags(temp_storage["venue"]["city"])
                print("Event ID: ", event_id)
                time = temp_storage["time"]
                state = "NA" if "venue" not in temp_storage else remove_tags(temp_storage["venue"]["state"])
                name = remove_emojis(name)
                descrition = remove_emojis(descrition)
                name = re.sub(r'[^a-zA-Z0-9_ \'"]+', '', name)
                descrition = re.sub(r'[^a-zA-Z0-9_ \'"]+', '', descrition)
                event_data_map[event_id] = [
                    event_id,
                    name,
                    city,
                    state,
                    time,
                    descrition
                ]
                event_details = name + descrition
                print("Before: ", event_details)
                event_details = pre_process_text_data(event_details)
                print("After: ", event_details)
                current_rating = gassian[index_for_rating]
                index_for_rating += 1
                index_for_rating = index_for_rating%len(gassian)
                current_rating = ((int)(current_rating * 100 + .5) / 100.0)
                current_rating = 0.1 if current_rating < 0.1 else current_rating
                current_rating = 5 if current_rating > 5 else current_rating
                mapped_event_details[event_id] = [event_id, current_rating, event_details]
        except:
            print("An exception occurred")
    num += 1

# headings = ["event_id", "name", "city", "state", "time", "descrition"]
# event_details_formatted_name_fortxt_file = pd.DataFrame(event_data_map.values(), columns = headings)
# event_details_formatted_name_fortxt_file.to_csv('event_details_formatted_name_fortxt_file.csv')


# headings = ["event_id", "current_rating", "event_details"]
# event_details_formatted_with_out_mapping = pd.DataFrame(mapped_event_details.values(), columns = headings)
# event_details_formatted_with_out_mapping.to_csv('event_details_formatted_with_out_mapping.csv')

# exit()

event_details_formatted_with_out_mapping = pd.read_csv('event_details_formatted_with_out_mapping.csv', header=0)
event_details_formatted_name_for_txt_file = pd.read_csv('event_details_formatted_name_fortxt_file.csv', header=0)

event_details_for_application = {}
for index in range(len(event_details_formatted_with_out_mapping['event_id'])):
    event_id = event_details_formatted_with_out_mapping['event_id'][index]
    result = binary_search(list_if_event_ids_points_of_intrest, 0, len(list_if_event_ids_points_of_intrest)-1, event_id)
    if result == -1:
        temporary_events.append(event_id)
    mapped_event_details[event_details_formatted_with_out_mapping['event_id'][index]] = [
        # event_details_formatted_with_out_mapping['event_id'][index],
        event_details_formatted_with_out_mapping['current_rating'][index],
        event_details_formatted_with_out_mapping['event_details'][index]
    ]

event_details_for_mapping = {}
for index in range(len(event_details_formatted_name_for_txt_file['event_id'])):
    event_data_map[event_details_formatted_name_for_txt_file['event_id'][index]] = [
        event_details_formatted_name_for_txt_file['event_id'][index],
        event_details_formatted_name_for_txt_file['name'][index],
        event_details_formatted_name_for_txt_file['city'][index],
        event_details_formatted_name_for_txt_file['state'][index],
        event_details_formatted_name_for_txt_file['time'][index],
        event_details_formatted_name_for_txt_file['descrition'][index]
    ]



# Creating data for CNN random_benchmark.csv
list_of_events = pd.read_csv(data_set_path+'/random_benchmark.csv', header=0)
user_to_event_map = {}
file = open('data.txt', 'w')
file2 = open('data1.txt', 'w')
hash_map = {}

for index in range(len(list_of_events['User'])):
    event_string = list_of_events['Events'][index]
    current_user_list_of_events = list(event_string[1:-1].split(','))
    hash_map[list_of_events['User'][index]] = current_user_list_of_events
    for event in current_user_list_of_events:
        event = int(event.replace(" ", ""))
        if event in mapped_event_details:
            save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
            file.write(save_string)
        else:
            if event not in temporary_map:
                temporary_map[event] = temporary_events[last_selected]
                last_selected += 1
            event = temporary_map[event]
            save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
            file.write(save_string)

list_of_events = pd.read_csv(data_set_path+'/train.csv', header=0)
for index in range(len(list_of_events['user'])):
    event = list_of_events['event'][index]
    if list_of_events['user'][index] in hash_map:
        if event in hash_map[list_of_events['user'][index]]:
            continue
        else:
            hash_map[list_of_events['user'][index]].append(event)
    event = int(event)
    if event in mapped_event_details:
        print("Here 1")
        save_string = str(list_of_events['user'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)
    else:
        if event not in temporary_map:
            temporary_map[event] = temporary_events[last_selected]
            last_selected += 1
            last_selected = last_selected%len(temporary_events)
        event = temporary_map[event]
        save_string = str(list_of_events['user'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)


list_of_events = pd.read_csv(data_set_path+'/test.csv', header=0)
for index in range(len(list_of_events['user'])):
    event = list_of_events['event'][index]
    if list_of_events['user'][index] in hash_map:
        if event in hash_map[list_of_events['user'][index]]:
            continue
        else:
            hash_map[list_of_events['user'][index]].append(event)
    event = int(event)
    if event in mapped_event_details:
        print("Here 2")
        save_string = str(list_of_events['user'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)
    else:
        if event not in temporary_map:
            temporary_map[event] = temporary_events[last_selected]
            last_selected += 1
            last_selected = last_selected%len(temporary_events)
        event = temporary_map[event]
        save_string = str(list_of_events['user'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)

list_of_events = pd.read_csv(data_set_path+'/event_popularity_benchmark_private_test_only.csv', header=0)

for index in range(len(list_of_events['User'])):
    event_string = list_of_events['Events'][index]
    current_user_list_of_events = list(event_string[1:-1].split(','))
    hash_map[list_of_events['User'][index]] = current_user_list_of_events
    for event in current_user_list_of_events:
        event = int(event.replace(" ", "")[:-1])
        if event in mapped_event_details:
            print("Here 3")
            save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
            file.write(save_string)
        else:
            if event not in temporary_map:
                temporary_map[event] = temporary_events[last_selected]
                last_selected += 1
                last_selected = last_selected%len(temporary_events)
            event = temporary_map[event]
            save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
            file.write(save_string)
# public_leaderboard_solution.csv

list_of_events = pd.read_csv(data_set_path+'/public_leaderboard_solution.csv', header=0)
for index in range(len(list_of_events['User'])):
    event_string = list_of_events['Events'][index]
    # current_user_list_of_events = list(event_string[1:-1].split(','))
    hash_map[list_of_events['User'][index]] = current_user_list_of_events
    # for event in current_user_list_of_events:
    event = int(event)
    if event in mapped_event_details:
        print("Here 4")
        save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)
    else:
        if event not in temporary_map:
            temporary_map[event] = temporary_events[last_selected]
            last_selected += 1
            last_selected = last_selected%len(temporary_events)
        event = temporary_map[event]
        save_string = str(list_of_events['User'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)

#writing formatted data 

temporary_map_values = list(temporary_map.values())
temporary_map_keys = list(temporary_map.keys())
for current_index in range(len(temporary_map_values)):
    if temporary_map_values[current_index] in event_data_map:
        event_data_map[temporary_map_values[current_index]][0] = temporary_map_keys[current_index]

headings = ["event_id", "name", "city", "state", "time", "descrition"]
cities = pd.DataFrame(event_data_map.values(), columns = headings)
cities.to_csv('event_details_formatted.csv')

print("Hello")

# Name, location(City, State), Data and Time  240859194


# Hey everyone that joined the group Welcome Sorry about the short notice This event is TONIGHT Ahhhh Come on out and play some games Our venue Abel's North is a casual space with plenty of room to spread out and relaxIf the notice is too short sorry You should try to make it out anyways This one is going to be epic Our next event will be on the first Wednesday of the month October 5th