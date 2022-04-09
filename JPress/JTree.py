# -------------------------------------------------------------------------
# This data structure represents a group of JSON docs in the form
# of a standardized tree structure.
# -------------------------------------------------------------------------

import json
import string
import functools
import numpy as np
import pandas as pd
import nested_lookup as nl
import random
import math
from tqdm import tqdm
import time as t

class JTree:
    def __init__(self, keys, child_keys, child_nodes, core_frame, abs_path):
        self.keys        = keys
        self.child_keys  = child_keys
        self.child_nodes = child_nodes
        self.core_frame  = core_frame

    @staticmethod
    def array2string(js):
        '''
        :param js: list of dictionaries
        :return: an equivalent list of JSON like dictionaries with their arrays as string
        '''
        t = type(js)
        if t == dict:
            temp = {}
            for k in js.keys():
                temp[k] = JTree.array2string(js[k])
            return temp
        if t == list:
            return str(js)
        return js

    @staticmethod
    def random_sampler(filename, percent):
        '''
        Takes a random sample of a file that consists of JSON documents per each line.
        :param filename: The source file address.
        :param percent: The percentage of the original number of rows to be sampled.
        :return: A list of sampled lines from the source
        '''
        sample = []

        with open(filename, 'rb', buffering=2000000000) as f:
            # Estimate the average size of each line:
            for i in range(1000):
                f.readline()
            estimate = f.tell()/1000

            f.seek(0, 2)
            filesize = f.tell()

            k = math.floor((percent/100) * (filesize/estimate))
            random_set = sorted(random.sample(range(filesize), k))

            for i in tqdm(range(k)):
                f.seek(random_set[i])
                # Skip current line (because we might be in the middle of a line)
                f.readline()
                # Append the next line to the sample set
                sample.append(f.readline().rstrip())

        return sample

    @staticmethod
    def JTreeBuilder(filepath, sample_percent=100):
        '''
        This function builds a JTree object from a given source file and loads the documents into memory.
        :param filepath: The original file containing JSON documents in one-per-line format.
        :param sample_percent: The percentage of documents to be sampled if the file is too large.
        :return: Returns a JTree object pointer that points to the root of the tree.
        '''
        core = ak = None

        def load():
            # This function returns a list of all nested keys that occur in the given dataset (or the sample).
            def all_keys(jlist):
                key_list = list(map(nl.get_all_keys, jlist))
                keys     = functools.reduce(lambda a,b: list(set(a + b)), key_list)
                return keys
            if sample_percent < 100:
                raw_jsons = JTree.random_sampler(filepath, sample_percent)

            else:
                with open(filepath, 'r') as f:
                    raw_jsons = f.readlines()

            jlist     = list(map(json.loads, raw_jsons)) # convert the text into dictionary
            jlist     = list(map(JTree.array2string, jlist)) # convert arrays into string (for now)
            print("Normalizing JSON...")
            core      = pd.json_normalize(jlist)
            ak        = all_keys(jlist)
            print("Load finished")
            return core, ak

        def build(keylist, core, abs_path):
            single_keys  = list(filter(lambda x: len(x) == 1, keylist))
            abs_single_keys = list(map(lambda x: abs_path + x, single_keys))
            nested_paths = list(filter(lambda x: len(x) >  1, keylist))

            if len(nested_paths) is 0:
                return JTree(keys=single_keys, child_keys=[], child_nodes=[],
                             core_frame=core[list(map(lambda x: '.'.join(x), abs_single_keys))],
                             abs_path=abs_single_keys)

            root_keys   = sorted(list(set(list(map(lambda x: x[0], nested_paths)))))
            objs        = []

            for k in root_keys:
                relevant = list(filter(lambda x: x[0] == k, nested_paths))
                k_paths  = list(map(lambda x: x[1:], relevant))
                k_objs   = build(keylist=k_paths, core=core, abs_path=abs_path + [k])
                objs     += [k_objs]
            print("Build finished.")
            return JTree(keys=single_keys, child_keys=root_keys, child_nodes=objs,
                         core_frame=core, abs_path=abs_single_keys)


        core, ak = load()

        print('Raw data is loaded ☑')
        cand_seperators = '.' + string.punctuation.replace('.', '')

        # Select a path separator (a special character) which does not occur in any of the nested key names
        for c in cand_seperators:
            flag = False
            for k in ak:
                if c in k:
                    flag = True
                    print(c + ' is in ' + str(k))
            if flag is False:
                sep = c
                break

        print('Separator is set to ' + sep + ' ☑')

        raw_keys = core.keys().to_list()
        raw_keys = list(map(lambda x: x.split(sep), raw_keys)) # Separates JSON paths by "sep" variable ('.' by default)
        raw_keys = list(map(lambda x: [len(x)] + x, raw_keys)) # Appends length of each path to the separated form
        raw_keys = list(map(tuple, raw_keys))                  # Converts lists to tuple to be sorted with multi-keys.
        raw_keys = sorted(raw_keys)                            # Sorts the keys (according to length first and \
                                                               # alphabetically next)

        raw_keys = list(map(list, raw_keys))                   # Convert back to list
        key_list = list(map(lambda x: x[1:], raw_keys))        # Remove the length
        raw_keys = list(map(lambda x: '.'.join(x), key_list))  # Convert back to string paths

        # Rearrange the keys in the normalized form
        core     = core[raw_keys]
        json_tree= build(keylist=key_list, core=core, abs_path=[])

        return json_tree
