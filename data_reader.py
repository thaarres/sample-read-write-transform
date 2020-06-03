import h5py
import numpy as np

class DataReader():
    ''' reads only from single files, not directories '''

    def __init__(self, path):
        self.path = path
        self.jet_constituents_key = 'jetConstituentsList'
        self.jet_features_key = 'eventFeatures'
        self.label_key = 'eventFeatureNames'

    def read_data(self, key):
        with h5py.File(self.path,'r') as f:
            return np.asarray(f.get(key))

    def read_jet_constituents(self):
        return self.read_data( self.jet_constituents_key )

    def read_jet_features(self):
        return self.read_data(self.jet_features_key)

    def read_labels(self):
        return [ l.decode("utf-8") for l in self.read_data( self.label_key ) ] # decode to unicode if (from byte str of Python2)
