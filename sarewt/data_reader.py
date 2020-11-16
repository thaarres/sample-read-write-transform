import h5py
import os
import numpy as np
from glob import glob
import pandas as pd
import operator

import sarewt.util as ut

class DataReader():
	'''
		reads events (dijet constituents & dijet features)
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
		''' return *sorted* recursive file-list in self.path '''
		flist = []
		for path, _, _ in os.walk(self.path, followlinks=True):
			if "MAYBE_BROKEN" in path:
				continue
			flist += glob(path + '/' + '*.h5')
		flist.sort()
		return flist


	def read_data_from_file(self, key, path=None):
		if path is None:
			path = self.path
		with h5py.File(path,'r') as f:
			return np.asarray(f.get(key))


	def read_constituents_and_dijet_features_from_file(self, path, dtype='float32'):
		with h5py.File(path,'r') as f:
			features = np.array(f.get(self.jet_features_key), dtype=dtype)
			constituents = np.array(f.get(self.jet_constituents_key), dtype=dtype)
			return [constituents, features]


	def count_files_events_in_dir(self):

		features_concat = []

		flist = self.get_file_list()

		for i_file, fname in enumerate(flist):
			try:
				features = self.read_data_from_file(key=self.jet_features_key, path=fname)
				features_concat.extend(features)
			except OSError as e:
				print("\nCould not read file ", fname, ': ', repr(e))
			except IndexError as e:
				print("\nNo data in file ", fname, ':', repr(e))
			except Exception as e:
				print("\nCould not read file ", fname, ': ', repr(e))

		return len(flist), np.asarray(features_concat).shape[0]


	def make_cuts(self, constituents, features, **cuts):
		if 'mJJ' in cuts:
			constituents, features = ut.filter_arrays_on_value(constituents, features, filter_arr=features[:, 0], filter_val=cuts['mJJ'])
		if 'sideband' in cuts:
			constituents, features = ut.filter_arrays_on_value(constituents, features, filter_arr=np.abs(features[:, -2]), filter_val=cuts['sideband'])
		if 'signalregion' in cuts:
			constituents, features = ut.filter_arrays_on_value(constituents, features, filter_arr=np.abs(features[:, -2]), filter_val=cuts['signalregion'], comp=operator.le)
		return constituents, features


	def read_events_from_file(self, fname, **cuts):
		constituents = []
		features = []
		try:
			constituents, features = self.read_constituents_and_dijet_features_from_file(fname)
			if cuts:
				constituents, features = self.make_cuts(constituents, features, **cuts)
		except OSError as e:
			print("\n[ERROR] Could not read file ", fname, ': ', repr(e))
		except IndexError as e:
			print("\n[ERROR] No data in file ", fname, ':', repr(e))
		except Exception as e:
			print("\nCould not read file ", fname, ': ', repr(e))

		return np.asarray(constituents), np.asarray(features)


	def read_event_parts_from_dir(self, max_sz_mb=1e9, **cuts):
		'''
		file parts generator
		'''
		print('reading', self.path)

		constituents_concat = []
		features_concat = []
		n_file = 0

		flist = self.get_file_list()

		for i_file, fname in enumerate(flist):
			constituents, features = self.read_events_from_file(fname, **cuts)
			constituents_concat.extend(constituents)
			features_concat.extend(features)
			sz_mb_total = (np.asarray(constituents_concat).nbytes + np.asarray(features_concat).nbytes) / 1024**2
			if sz_mb_total > max_sz_mb: # if event sample size exceeding max size, yield next chunk and reset
				print('\nnum files read for file part from ', self.path, ': ', i_file + 1 - n_file) 
				yield (np.asarray(constituents_concat), np.asarray(features_concat))
				constituents_concat = []
				features_concat = []
				n_file = i_file + 1


	def read_events_from_dir(self, max_n=1e9, features_to_df=False, **cuts):
		'''
		read dijet events (jet constituents & jet features) from files in directory
		:param max_n: limit number of events
		:return: concatenated jet constituents and jet feature array + corresponding particle feature names and event feature names
		'''
		print('reading', self.path)

		constituents_concat = []
		features_concat = []

		flist = self.get_file_list()

		for i_file, fname in enumerate(flist):
			constituents, features = self.read_events_from_file(fname, **cuts)
			constituents_concat.extend(constituents)
			features_concat.extend(features)
			if len(constituents_concat) > max_n:
				break

		print('\nnum files read in dir ', self.path, ': ', i_file + 1)

		particle_feature_names, dijet_feature_names = self.read_labels_from_dir(flist)

		features = pd.DataFrame(np.asarray(features_concat),columns=dijet_feature_names) if features_to_df else np.asarray(features_concat)
		return [np.asarray(constituents_concat), particle_feature_names, features, dijet_feature_names]


	def read_constituents_from_dir(self):
		''' read constituents of jet 1 and jet 2 from all file parts in directory '''
		constituents, *_ = self.read_events_from_dir()
		return constituents


	def read_constituents_parts_from_dir(self, max_sz_mb):
		for (constituents, features) in self.read_event_parts_from_dir(max_sz_mb=max_sz_mb):
			yield constituents


	def read_jet_features_from_dir(self, apply_mjj_cut=True):

		#print('reading', self.path)

		features_concat = []

		flist = self.get_file_list()

		for i_file, fname in enumerate(flist):
			try:
				features = self.read_data_from_file(key=self.jet_features_key, path=fname)
				if apply_mjj_cut:
					features, = ut.filter_arrays_on_value(features, filter_arr=features[:, 0], filter_val=self.mjj_cut) # 0: mjj_idx # TODO: when filter() is passed only one array, has to be unpacked by caller as a, = filter() => need to change variadic *arrays argument!
				features_concat.extend(features)
			except OSError as e:
				print("\nCould not read file ", fname, ': ', repr(e))
			except IndexError as e:
				print("\nNo data in file ", fname, ':', repr(e))

		print('{} events read in {} files in dir {}'.format(np.asarray(features_concat).shape[0], i_file + 1, self.path))

		dijet_feature_names, = self.read_labels_from_dir(flist=flist, keylist=[self.dijet_feature_names])

		return [np.asarray(features_concat), dijet_feature_names]


	def read_jet_features_from_dir_to_df(self, apply_mjj_cut=True):
		features, names = self.read_jet_features_from_dir(apply_mjj_cut)
		return pd.DataFrame(features,columns=names)


	def read_constituents_from_file(self):
		''' return array of shape [N x 2 x 100 x 3] with
			N examples, each with 2 jets, each with 100 highest pt particles, each with features eta phi pt
		'''
		return self.read_data_from_file( self.jet_constituents_key )

	def read_jet_features_from_file(self):
		return self.read_data_from_file(self.jet_features_key)

	def read_labels(self,key=None,path=None):
		if key is None:
			key = self.dijet_feature_names
		if path is None:
			path = self.path
		return [ l.decode("utf-8") for l in self.read_data_from_file( key, path ) ] # decode to unicode if (from byte str of Python2)


	def read_labels_from_dir(self, flist=None, keylist=None):
		if flist is None:
			flist = self.get_file_list()
		if keylist is None:
			keylist = [self.constituents_feature_names, self.dijet_feature_names]

		labels = []
		for i_file, fname in enumerate(flist):
			try:
				for key in keylist:
					labels.append(self.read_labels(key, fname))
				break
			except Exception as e:
				print("\nCould not read file ", fname, ': ', repr(e))
				labels = []

		return labels



class CaseDataReader(DataReader):

	# set different keys
	def __init__(self, path):
		DataReader.__init__(self, path)
		self.jet_features_key = 'jet_kinematics'
		self.dijet_feature_names_val = ['mJJ', 'DeltaEtaJJ', 'j1Pt', 'j1Eta', 'j1Phi', 'j1M', 'j2Pt', 'j2Eta', 'j2Phi', 'j2M', 'j3Pt', 'j3Eta', 'j3Phi', 'j3M']
		self.jet1_constituents_key = 'jet1_PFCands'
		self.jet2_constituents_key = 'jet2_PFCands'
		self.constituents_feature_names_val = ['Px', 'Py', 'Pz', 'E']
		self.truth_label_key = 'truth_label'

	# TODO: how to exclude "test" file? (own get_file_list function?)

	def read_jet_constituents_from_file(self, file):
		''' return jet constituents as array of shape N x 2 x 100 x 4
			(N examples, each with 2 jets, each jet with 100 highest-pt particles, each particle with px, py, pz, E features)
		'''
		if isinstance(file, str):
			file = h5py.File(file,'r') 
		j1_constituents = np.array(file.get(self.jet1_constituents_key)) # (576902, 100, 4)
		j2_constituents = np.array(file.get(self.jet2_constituents_key)) # (576902, 100, 4)
		return np.stack([j1_constituents, j2_constituents], axis=1)


	def read_constituents_and_dijet_features_from_file(self, path):
		with h5py.File(path,'r') as f:
			features = np.array(f.get(self.jet_features_key))
			constituents = self.read_jet_constituents_from_file(f)
			return [constituents, features]

	def read_labels(self, key, path=None):
		''' labels are not provided in CASE dataset '''
		if key == self.dijet_feature_names:
			return self.dijet_feature_names_val
		if key == self.constituents_feature_names:
			return self.constituents_feature_names_val


	def read_events_from_dir(self, max_n=1e9):
		'''
		read dijet events (jet constituents & jet features) from files in directory
		:param max_n: limit number of events
		:return: concatenated jet constituents and jet feature array + corresponding particle feature names and event feature names
		'''
		print('reading', self.path)

		constituents_concat = []
		features_concat = []
		truth_labels_concat = []

		flist = self.get_file_list()

		for i_file, fname in enumerate(flist):
			try:
				constituents, features = self.read_constituents_and_dijet_features_from_file(fname)
				#constituents, features = ut.filter_arrays_on_value(constituents, features, filter_arr=features[:, 0], filter_val=self.mjj_cut) # 0: mjj_idx
				truth_labels = self.read_data_from_file(self.truth_label_key, fname)
				constituents_concat.extend(constituents)
				features_concat.extend(features)
				truth_labels_concat.extend(truth_labels)
			except OSError as e:
				print("\nCould not read file ", fname, ': ', repr(e))
			except IndexError as e:
				print("\nNo data in file ", fname, ':', repr(e))
			if len(constituents_concat) > max_n:
				break

		print('\nnum files read in dir ', self.path, ': ', i_file + 1)

		return [np.asarray(constituents_concat), self.constituents_feature_names_val, np.asarray(features_concat), self.dijet_feature_names_val, np.asarray(truth_labels_concat)]
