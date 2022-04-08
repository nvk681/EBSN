# Process event details
import pandas as pd 
import json
import re
import nltk

import string
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer


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
                descrition = "" if "description" not in temp_storage else remove_tags(temp_storage["description"])
                city = "NA" if "venue" not in temp_storage else remove_tags(temp_storage["venue"]["country"])
                print("Event ID: ", event_id)
                event_details = name + descrition
                print("Before: ", event_details)
                event_details = pre_process_text_data(event_details)
                print("After: ", event_details)
                mapped_event_details[event_id] = [city, event_details]
        except:
            print("An exception occurred")
    num += 1

# Creating data for CNN random_benchmark.csv
list_of_events = pd.read_csv(data_set_path+'/random_benchmark.csv', header=0)
user_to_event_map = {}
file = open('data.txt', 'w')
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
        pass

list_of_events = pd.read_csv(data_set_path+'/train.csv', header=0)
for index in range(len(list_of_events['user'])):
    event = list_of_events['event'][index]
    if list_of_events['user'][index] in hash_map:
        if event in hash_map[list_of_events['user'][index]]:
            continue
        else:
            hash_map[list_of_events['user'][index]].append(event)
    # event = int(event.replace(" ", ""))
    if event in mapped_event_details:
        print("Here 1")
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
    # event = int(event.replace(" ", ""))
    if event in mapped_event_details:
        print("Here 2")
        save_string = str(list_of_events['user'][index])+"::"+str(event)+"::"+str(mapped_event_details[event][0])+"::"+str(mapped_event_details[event][1])+"\n"
        file.write(save_string)



print("Hello")