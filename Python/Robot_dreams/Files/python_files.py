import json
import os

# define dictionary
dd = {"name": "Anne", "salary": 6000}
print(dd)
print(dd['name'])
print(dd['salary'])
# print(dd['age'])          # keyError

print(dd.get('name'))               # Anne
print(dd.get('age'))                # None
print(dd.get('age', "No age"))      # No age

for i in dd:
    print(i)
print(dd.keys())
print(dd.values())

# define set of dictionaries
dd = [
    {"name": "Anne", "salary": 6000},
    {"name": "Rob", "salary": 6500},
    {"name": "Sara", "salary": 7000}
    ]
for i in dd:
    print(i)
    print(i['name'])
    print(i['salary'])

# add key and default value
for i in dd:
    i['age'] = 40
print(dd)

# working with json files
# read previously created json file and save to dict
with open('data/sample_file.json', 'r') as json_file:
    py_dict = json.load(json_file)      # load json file to dictionary

print(py_dict)
print(type(py_dict))        # class list
print(type(py_dict[0]))     # class dict

# alternate read json file
json_file2 = open('data/sample_file.json', 'r')
py_dict2 = json.load(json_file2)
print(py_dict == py_dict2)
json_file2.close()

# change py_dict
for dct in py_dict:
    dct['age'] = 40

# and write dict to new json file
with open('./data/sample_file2.json', 'w') as json_file2:
    json.dump(py_dict, json_file2)      # dump dict to json file

# good way to point file path
dir_name = 'data/'
file_name = 'sample_file2.json'
with open(os.path.join('../Start', dir_name, file_name), 'w') as json_file2:
    json.dump(py_dict, json_file2)
print(os.path.join('../Start', 'data', 'sample_file2.json'))







