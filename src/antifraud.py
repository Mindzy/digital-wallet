# example of program that detects suspicious transactions
# fraud detection algorithm

import sys
import pandas as pd
import networkx as nx
    

def read_transaction_data(input_path):
    raw_data = pd.read_csv(input_path, 'rU'
                             , index_col=False, engine='python')
    raw_data.columns = ['id1']

    def raw(x): return x.split(', ')[1:3]

    def key(x): return long(x[0])

    def val(x): return long(x[1])
    raw_data['id1'] = raw_data['id1'].map(raw)
    raw_data['id2'] = raw_data['id1'].map(val)
    raw_data['id1'] = raw_data['id1'].map(key)
    return raw_data
# Create the class transaction


class Transaction:
    def __init__(self):
        self.adjacency_dict = {}
        self.training_graph = nx.Graph()

    # Create adjacency list as dict
    def create_adjacency_list(self, training_data):
        for i in range(len(training_data.index)):
            # assign each row to a variable to avoid repeated search
            training_data_row = training_data.ix[i, :]
            row_item = (long(training_data_row[0]), long(training_data_row[1]))
            self.__adjacency_training__(row_item, 0, 1)
            self.__adjacency_training__(row_item, 1, 0)
        return "Function create_adjacency_list() is done"

    # Training method
    def __adjacency_training__(self, row_item, index_id, value_id):
        if row_item[index_id] in self.adjacency_dict:
            # if id1 is in dict, get the value list
            adjacency_dict_value_list = self.adjacency_dict[row_item[index_id]]
            if row_item[value_id] not in adjacency_dict_value_list:
                # if id2 not in value list, append the id2 in the value list
                adjacency_dict_value_list.append(long(row_item[value_id]))
        else:
            # if id1 is not in dict, create a value list with id2 in it
            self.adjacency_dict[row_item[index_id]] = [row_item[value_id]]

    def feature(self, test_data, feature_num, output_path):
        if feature_num == 1:
            deep = 1
        elif feature_num == 2:
            deep = 2
        elif feature_num == 3:
            deep = 4
        else:
            return "feature_ValueError"
        fd = open(output_path, 'w')
        for i in test_data.index:
            test_row_item = test_data.ix[i, :]
            key = test_row_item[0]
            if key in self.adjacency_dict:
                to_find = test_row_item[1]
                if self.__search_helper__(key, to_find, 0, deep):
                    fd.write("trusted\n")
                else:
                    fd.write("unverified\n")
                    self.__adjacency_training__(test_row_item, 0, 1)
                    self.__adjacency_training__(test_row_item, 1, 0)
            else:
                fd.write("unverified\n")
                self.__adjacency_training__(test_row_item, 0, 1)
                self.__adjacency_training__(test_row_item, 1, 0)
        fd.close()
        return "Feature " + str(feature_num) + " is done."

    def __search_helper__(self, key, to_find, count, deep):
        # use dfs to search whether the id2 is found
        if count > deep:
            return False
        # check
        value_list = self.adjacency_dict[key]
        if to_find in value_list:
            return True
        else:
            for value in value_list:
                self.__search_helper__(value, to_find, count + 1, deep)
        return False

    def __convert_dict_to_nx_graph__(self, training_dict):
        for index in training_dict:
            for value in training_dict[index]:
                self.training_graph.add_edge(index, value)

    def feature_nx_graph(self, test_data, feature_num, output_path):
        if feature_num == 1:
            deep = 1
        elif feature_num == 2:
            deep = 2
        elif feature_num == 3:
            deep = 4
        else:
            return "feature_ValueError"
        self.__convert_dict_to_nx_graph__(self.adjacency_dict)
        fd = open(output_path, 'w')
        for i in test_data.index:
            test_row_item = test_data.ix[i, :]
            key = test_row_item[0]
            val = test_row_item[1]
            nodes = self.training_graph.nodes()
            if (key in nodes) and (val in nodes):
                path_length = nx.shortest_path_length(self.training_graph, key, val)
                if path_length <= deep:
                    fd.write("trusted\n")
                else:
                    fd.write("unverified\n")
            else:
                fd.write("unverified\n")
                self.training_graph.add_edge(key, val)
        fd.close()

def main():
    argv = sys.argv

    record_data = read_transaction_data(argv[1])
    stream_data = read_transaction_data(argv[2])
    
    feature_1 = Transaction()
    feature_1.create_adjacency_list(record_data)
    feature_1.feature(stream_data, 1, argv[3])
    
    feature_2 = Transaction()
    feature_2.create_adjacency_list(record_data)
    feature_2.feature(stream_data, 2, argv[4])
    
    feature_3 = Transaction()
    feature_3.create_adjacency_list(record_data)
    feature_3.feature_nx_graph(stream_data, 3, argv[5])
    
    return "Job is done"
#%%
if __name__ == "__main__":
    main()