"""
Name: Chris Jackson
Email: jack1391@msu.edu
Time To Completion: 1.5-2 hours?
Comments:

Sources:
        Used:
            https://thispointer.com/python-how-to-find-all-indexes-of-an-item-in-a-list/
            https://www.geeksforgeeks.org/python-list-index/

            to find multiple indices in a list

        Used:
            https://docs.python.org/3/library/json.html#json.dump
            https://docs.python.org/3/library/json.html#json.load
            
            to save and load Database to a file (json)
"""

import json


class Collection():
    def __init__(self):
        self.data = []

    def insert(self, document):
        self.data.append(document)

    def find_all(self):
        return self.data

    def delete_all(self):
        self.data = []

    def find_one(self, where_dict):
        all_matches = self.find(where_dict)
        if len(all_matches) > 0:
            return all_matches[0]
        else:
            return None

    def find(self, where_dict):
        if len(where_dict) == 0:
            return self.find_all()

        ret = []
        for line in self.data:
            matches = False
            for key, value in where_dict.items():
                if key in line.keys():
                    if type(line[key]) == dict:
                        for x, y in value.items():
                            if x in line[key].keys():
                                if line[key][x] == y:
                                    matches = True
                                else:
                                    matches = False
                                    break
                            else:
                                matches = False
                                break
                        else:
                            continue
                        break
                    else:
                        if line[key] == value:
                            matches = True
                        else:
                            matches = False
                            break
                else:
                    matches = False
                    break
            
            if matches:
                ret.append(line)

        return ret

    def count(self, where_dict):
        all_matches = self.find(where_dict)
        return len(all_matches)

    def delete(self, where_dict):
        if where_dict == dict():
            self.delete_all()
        else:
            all_matches = self.find(where_dict)
            
            for match in all_matches:
                self.data.pop( self.data.index(match) )

    def update(self, where_dict, changes_dict):
        all_matches = self.find(where_dict)

        indexList = []
        currIndex = 0
        for match in all_matches:
            while True:
                try:
                    index = self.data.index(match, currIndex)
                    indexList.append(index)
                    currIndex += 1
                except ValueError:
                    break

        for key, value in changes_dict.items():
            for indx in indexList:
                self.data[indx][key] = value

    def map_reduce(self, map_function, reduce_function):
        
        reduce_list = []
        for item in self.data:
            reduce_list.append(map_function(item))

        return reduce_function(reduce_list)



class Database():
    def __init__(self, filename):
        self.filename = filename
        self.collections = dict()

        try:
            with open(self.filename) as f:
                jsonData = json.load(f)
            
            for collection in jsonData:
                for key, value in collection.items():
                    temp = Collection()
                    for item in value:
                        temp.insert(item)
                    self.collections[key] = temp
        except IOError:
            pass

    def get_collection(self, name):
        if name not in self.collections:
            self.collections[name] = Collection()

        return self.collections[name]

    def get_names_of_collections(self):
        return sorted(list(self.collections.keys()))

    def drop_collection(self, name):
        if name in self.collections.keys():
            self.collections.pop(name)

    def close(self):
        if len(self.collections) == 0:
            temp = dict()
            with open(self.filename, 'w') as f:
                json.dump(temp, f)
        else:
            data = []
            for key, value in self.collections.items():
                temp = dict()
                temp[key] = value.find_all()
                
                data.append(temp)
                
            with open(self.filename, 'w') as f:
                json.dump(data, f)


