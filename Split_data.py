# -*- coding: utf-8 -*-
"""
Created on Sun Apr 10 20:26:17 2022

@author: pavan
"""

import datetime
import random
import math
import sys
import os
from time import sleep
from tqdm import tqdm

train_ratio=8
valid_ratio=1
test_ratio=1

data_path='data.txt'

all_tuple_list = []
user2item = {}
item2user = {}
user2item2line = {}
with open(data_path, 'r', errors='ignore') as f:
    for line in tqdm(f.readlines()):
        content = line.strip().split('::')
        u = content[0]
        i = content[1]
        all_tuple_list.append((u, i))

        if u in user2item:
            user2item[u].append(i)
        else:
            user2item[u] = [i]
        if i in item2user:
            item2user[i].append(u)
        else:
            item2user[i] = [u]

        if u in user2item2line:
            user2item2line[u][i] = line
        else:
            user2item2line[u] = {i: line}

# split rating data
train_set = set()
for (u, item_list) in tqdm(user2item.items()):
    i = random.choice(item_list)
    train_set.add((u, i))
for (i, user_list) in tqdm(item2user.items()):
    u = random.choice(user_list)
    train_set.add((u, i))

total_num = len(all_tuple_list)
train_num = int(train_ratio / (train_ratio + valid_ratio + test_ratio) * total_num)
valid_num = int(valid_ratio / (train_ratio + valid_ratio + test_ratio) * total_num)

while len(train_set) < train_num:
    train_set.add(random.choice(all_tuple_list))
remains_list = list(set(all_tuple_list) - train_set)

valid_set = set()
while len(valid_set) < valid_num:
    valid_set.add(random.choice(remains_list))
test_set = set(remains_list) - valid_set

def write_to_file(path, data_set):
    with open(path, 'w', encoding='utf-8', errors='ignore') as f:
        for (u, i) in data_set:
            line = user2item2line[u][i].strip()
            content = line.split('::')
            new_content = '::'.join(content[2:])
            f.write(u + '::' + i + '::' + new_content + '\n')

# save data
save_dir=r"/Users/venkatakrishnachaitanyanuthalapati/University/Adv CN/EBSN/dataset"
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
#print(get_now_time() + 'writing rating data to ' + save_dir)
write_to_file(save_dir + 'train', train_set)
write_to_file(save_dir + 'valid', valid_set)
write_to_file(save_dir + 'test', test_set)

print("Ran the code")