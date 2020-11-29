import unittest
import os
import numpy as np
import sarewt.data_reader as dare



class DataReaderTestCase(unittest.TestCase):

	def setUp(self):
		self.dir_path = '/eos/user/k/kiwoznia/data/VAE_data/baby_events/qcd_sqrtshatTeV_13TeV_PU40'
		self.reader = dare.DataReader(self.dir_path)
		self.file_paths = fnames = [os.path.join(self.dir_path, 'qcd_sqrtshatTeV_13TeV_PU40_'+str(n)+'.h5') for n in [34,721,96]] 


	# test reading events from single file
	def test_read_events_from_file_no_cuts(self):
		constituents, features = self.reader.read_events_from_file(self.file_paths[0])
		self.assertEqual(constituents.shape, (57964, 2, 100, 3))
		self.assertEqual(features.shape, (57964, 11))
		# mean pt, dPhi, dEta
		self.assertAlmostEqual(np.mean(constituents[...,-1]), 4.276516632181256, places=4)
		self.assertAlmostEqual(np.mean(constituents[...,-2]), -2.0955217934427573e-05, places=4)
		self.assertAlmostEqual(np.mean(constituents[...,-3]), -0.00046528394023909826)
		# limits dEta, dPhi 
		self.assertAlmostEqual(np.min(constituents[...,-2]), -0.7999, places=3)
		self.assertAlmostEqual(np.min(constituents[...,-3]), -0.7999, places=3)
		self.assertAlmostEqual(np.max(constituents[...,-2]), 0.7999, places=3)
		self.assertAlmostEqual(np.max(constituents[...,-3]), 0.7999, places=3)
		# mean mjj, dEtaJJ, dPhiJJ
		self.assertAlmostEqual(np.mean(features[:,0]), 741.7274080953777, places=3)
		self.assertAlmostEqual(np.mean(features[:,-2]), -0.0025248032581180646, places=3)
		self.assertAlmostEqual(np.mean(features[:,-1]), 0.002353363388548727, places=3)



	# test event chunk generation from dir with num events splitting
	def test_events_generated_by_num(self):

		read_events_n = 0
		parts_n = 1500
		
		for (constituents, features) in self.reader.generate_event_parts_from_dir(parts_n=parts_n):
		
			# check number of events read
			self.assertLessEqual(len(constituents), parts_n)
			self.assertEqual(len(constituents), len(features))
			self.assertGreater(len(constituents), 0)
			read_events_n += len(constituents)
		
			# check shape of events read
			self.assertEqual(len(constituents.shape), 4)
			self.assertEqual(constituents.shape[1], 2) # 2 jets
			self.assertEqual(constituents.shape[2], 100) # 100 particles
			self.assertEqual(constituents.shape[3], 3) # eta, phi, pt
			self.assertEqual(len(features.shape), 2)
			self.assertEqual(features.shape[1], 11) # 11 features


		# check that all events in dir have been read	
		self.assertEqual(read_events_n, 57964+36+58096)


	# test event chunk generation from dir with size events splitting 
	# TODO: implement
	def test_events_generated_by_size(self):
		pass


	# test applying cuts to event sets
	def test_make_cuts(self):

		idx_mjj, idx_dEta, idx_dPhi = 0, -2, -1

		before_cut_n = 10
		constituents = np.ones((before_cut_n,100,3))
		features = np.ones((before_cut_n,12))

		# check cutting on mJJ
		features[:, idx_mjj] = [1500, 2000, 3000, 4, 5, 6, 1110, 2220, 9090, 10] 
		cuts = {'mJJ': 1100}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),sum(features[:,idx_mjj]>cuts['mJJ']))
		self.assertEqual(len(features_after),len(const_after))

		# check cutting on sideband | dEta | > 1.4
		# no events in sideband
		features[0, idx_dEta] = 1.4
		cuts = {'sideband': 1.4}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),0)
		self.assertEqual(len(const_after),sum(np.abs(features[:,idx_dEta])>cuts['sideband']))
		self.assertEqual(len(features_after),len(const_after))
		# all events in signalregion
		cuts = {'signalregion': 1.4}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),len(constituents))
		self.assertEqual(len(const_after),sum(np.abs(features[:,idx_dEta])<=cuts['signalregion']))
		self.assertEqual(len(features_after),len(const_after))
		# mixed
		features[:, idx_dEta] = [-100, -0.5, 0, 3, 1.4, 1.3, -1.4, -1.4, 200, 10]
		cuts = {'sideband': 1.4}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),sum(np.abs(features[:, idx_dEta])>cuts['sideband']))
		self.assertEqual(len(features_after),len(const_after)) 
		cuts = {'signalregion': 1.4}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),sum(np.abs(features[:, idx_dEta])<=cuts['signalregion']))
		self.assertEqual(len(features_after),len(const_after))

		# mJJ and dEta
		cuts = {'mJJ': 1100, 'sideband': 1.4}
		const_after, features_after = self.reader.make_cuts(constituents, features, **cuts)
		self.assertEqual(len(const_after),sum(np.logical_and(features[:,idx_mjj]>cuts['mJJ'],np.abs(features[:, idx_dEta])>cuts['sideband'])))
		self.assertEqual(len(features_after),len(const_after)) 



if __name__ == '__main__':
	unittest.main()
