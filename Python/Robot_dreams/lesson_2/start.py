a = 1

if a != 1:
    print('Ok')
else:
    print('not Ok')

l = [1, 2, 3, 4, 5]
for element in l:
    print(element + 1)

py_list_full = list(('Bob', 'Anne', 'Kate', 1, True, [1, 2, 3]))  # mutable - can add or change, ordered list

py_list = ['Bob', 'Anne', 'Kate']  # mutable - can add or change, ordered list
py_tuple = ('Bob', 'Anne', 'Kate')  # immutable, ordered list
py_set = {'Bob', 'Anne', 'Kate'}  # unique list, changeable, not ordered list, fast!
py_set = set(('Bob', 'Anne', 'Kate'))
py_list1 = list(py_set)

print(py_list)
print(py_set)
print(py_list[0])  # Bob
print(py_tuple[0])  # Bob
# print(py_set[0])   # error!

for i in py_list:
    print(i)
for i in py_tuple:
    print(i)
for i in py_set:
    print(i)  # not ordered!

# change (add) to list
print(py_list)
py_list.append('Rob')
print(py_list)
py_list.append(['Rob', 'Eric'])
print(py_list)
py_list.extend(['Rob', 'Eric'])  # concatenate 2 lists
print(py_list)

# change (add) to set
print(py_set)
py_set.add('Rob')
print(py_set)
py_set.add('Rob')
py_set.add('Anne')
print(py_set)

# set methods
friends = {'Bob', 'Anne', 'Kate', 'Rob'}
friends_abroad = {'Bob', 'Kate'}
local = friends.difference(friends_abroad)
print(local)
friends2 = local.union(friends_abroad)
print(friends2)
print(friends == friends2)  # True

python = {'Bob', 'Anne', 'Kate', 'John'}
scala = {'Rob', 'Anne', 'Christine', 'Kate'}
both = python.intersection(scala)
print(both)
know_one = python.symmetric_difference(scala)
print(know_one)

# slicing notation
l = "1 2 3 4 5 6 7 8".split(" ")
print(l)
for i in l:
    print(i)

print(l[0])
print(l[1])
print(l[2])
print(l[0:8])
print(l[0:8:1])
print(l[0:8:2])  # every 2nd
print(l[0:8:3])  # every 3d
print(l[:4])  # all until 5
print(l[3:])  # all from 4
print(l[::2])  # every 2nd
print(l[::-1])  # back order
print(l[:4:-1])  # back order from 4
print(l[:-2])  # without last 2
