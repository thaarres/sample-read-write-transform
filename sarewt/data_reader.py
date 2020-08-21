import h5py
import os
import numpy as np
from glob import glob

import util as ut

class DataReader():
    '''
        reads
        - events (dijet constituents & dijet features)
        - images
        - results
        from single files and directories
    '''

    def __init__(self, path):
        self.path = path
        self.jet_constituents_key = 'jetConstituentsList'
        self.jet_features_key = 'eventFeatures'
        self.dijet_feature_names = 'eventFeatureNames'
        self.constituents_feature_names = 'particleFeatureNames'
        self.mjj_cut = 1100.

    def get_file_list(self):
        flist = []
        for path, _, _ in os.walk(self.path, followlinks=True):
            print('reading ', path)
            flist += glob(path + '/' + '*.h5')
        flist.sort()
        return flist

    def read_data_from_file(self, key, path=None):
        if path is None:
            path = self.path
        with h5py.File(path,'r') as f:
            return np.asarray(f.get(key))

    def read_constituents_and_dijet_features_from_file(self,path):
        with h5py.File(path,'r') as f:
            features = np.array(f.get(self.jet_features_key))
            constituents = np.array(f.get(self.jet_constituents_key))
            return [constituents, features]


    def read_events_from_dir(self, max_N=1e9):
        '''
        read dijet events (jet constituents & jet features) from files in directory
        :param max_N: limit number of events
        :return: concatenated jet constituents and jet feature array + corresponding particle feature names and event feature names
        '''
        print('reading', self.path)

        constituents_concat = []
        features_concat = []

        flist = self.get_file_list()
        print('num files in dir:', len(flist))

        for i_file, fname in enumerate(flist):
            try:
                constituents, features = self.read_constituents_and_dijet_features_from_file(fname)
                constituents, features = ut.filter_arrays_on_value(constituents, features, filter_arr=features[:, 0], filter_val=self.mjj_cut) # 0: mjj_idx
                constituents_concat.extend(constituents)
                features_concat.extend(features)
            except OSError as e:
                print("\nCould not read file ", fname, ': ', repr(e))
            except IndexError as e:
                print("\nNo data in file ", fname, ':', repr(e))
            if len(constituents_concat) > max_N:
                break

        print('\nnum files read in dir ', self.path, ': ', i_file + 1)

        for i_file, fname in enumerate(flist):
            try:
                particle_feature_names = self.read_labels(self.constituents_feature_names, fname)
                dijet_feature_names = self.read_labels(self.dijet_feature_names, fname)
                break
            except Exception as e:
                print("\nCould not read file ", fname, ': ', repr(e))

        return [np.asarray(constituents_concat), particle_feature_names, np.asarray(features_concat), dijet_feature_names]


    def read_jet_features_from_dir(self):

        print('reading', self.path)

        features_concat = []

        flist = self.get_file_list()
        print('num files in dir:', len(flist))

        for i_file, fname in enumerate(flist):
            try:
                features = self.read_data_from_file(key=self.jet_features_key, path=fname)
                features_concat.extend(features)
            except OSError as e:
                print("\nCould not read file ", fname, ': ', repr(e))
            except IndexError as e:
                print("\nNo data in file ", fname, ':', repr(e))

        print('\nnum files read in dir ', self.path, ': ', i_file + 1)

        for i_file, fname in enumerate(flist):
            try:
                dijet_feature_names = self.read_labels(self.dijet_feature_names, fname)
                break
            except Exception as e:
                print("\nCould not read file ", fname, ': ', repr(e))

        return [np.asarray(features_concat), dijet_feature_names]



    def read_jet_constituents(self):
        return self.read_data_from_file( self.jet_constituents_key )

    def read_jet_features(self):
        return self.read_data_from_file(self.jet_features_key)

    def read_labels(self,key=None,path=None):
        if key is None:
            key = self.dijet_feature_names
        if path is None:
            path = self.path
        return [ l.decode("utf-8") for l in self.read_data_from_file( key, path ) ] # decode to unicode if (from byte str of Python2)
