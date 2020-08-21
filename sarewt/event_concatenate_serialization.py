import argparse
import numpy as np

import data_reader as dr
import data_writer as dw

def compute_num_file_parts(constituents, features, sz_mb):
    sz_mb_total = (constituents.nbytes + features.nbytes) / 1024**2
    n_parts = int(np.ceil(sz_mb_total/sz_mb))
    print('divinding dataset of size {:6.2f} MB into {:d} chunks of size {:6.2f} MB'.format(sz_mb_total, n_parts, sz_mb))
    return n_parts


def split_concat_data(constituents, features, n_parts):
    return [np.array_split(constituents, n_parts), np.array_split(features, n_parts)]


def write_file_parts(constituents, constituent_names, features, feature_names, keys, file_name, sz_mb):
    n_file_parts = compute_num_file_parts(constituents, features, sz_mb)
    constituents_parts, features_parts = split_concat_data(constituents, features, n_file_parts)
    ext_idx = file_name.rindex('.')
    for i, (constituents_i, features_i) in enumerate(zip(constituents_parts, features_parts)):
        file_name_part = file_name[:ext_idx] + "_{:03d}".format(i) + file_name[ext_idx:] 
        write_file([constituents_i, constituent_names, features_i, feature_names], keys, file_name_part)        


def write_file(data, keys, file_name):
    print('writing {} events to {}'.format(data[0].shape[0], file_name))
    dw.write_data_to_file(data, keys, file_name)


def read_concat_write(indir, file_name, n_max, sz_mb):    
    reader = dr.DataReader(indir)
    constituents_concat, particle_feature_names, features_concat, dijet_feature_names = reader.read_events_from_dir(n_max)
    keys = [l.encode('utf-8') for l in ['jetConstituentsList', 'particleFeatureNames', 'eventFeatures', 'eventFeatureNames']]
    particle_feature_names = [n.encode('utf-8') for n in particle_feature_names]
    dijet_feature_names = [n.encode('utf-8') for n in dijet_feature_names]
    if sz_mb:
        write_file_parts(constituents_concat, particle_feature_names, features_concat, dijet_feature_names, keys=keys, file_name=file_name, sz_mb=sz_mb)
    else:
        write_file([constituents_concat, particle_feature_names, features_concat, dijet_feature_names], keys=keys, file_name=file_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='read, concatenate and write events from all files in directory')
    parser.add_argument('-in', dest='indir', type=str, help='input directory')
    parser.add_argument('-out', dest='outfile', type=str, default='out.h5', help='output file name/path')
    parser.add_argument('-n', dest='num_evts', type=int, default=1e9, help='max number of events for output dataset')
    parser.add_argument('-mb', dest='sz_mb', type=int, help='split concatenated dataset in multiple files, each of size mb [MB]')

    args = parser.parse_args()

    print('concatenating data in', args.indir)

    read_concat_write(args.indir, args.outfile, args.num_evts, args.sz_mb)