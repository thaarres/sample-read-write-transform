import argparse

import data_reader as dr
import data_writer as dw

def read_concat_write(indir, outfile, max_n):
    reader = dr.DataReader(indir)
    data = reader.read_events_from_dir(max_n)
    keys = ['jetConstituentsList', 'particleFeatureNames', 'eventFeatures', 'eventFeatureNames']
    dw.write_data_to_file(data, keys, outfile)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='read, concatenate and write events from all files in directory')
    parser.add_argument('-in', dest='indir', type=str, help='input directory')
    parser.add_argument('-out', dest='outfile', type=str, default='out.h5', help='output file name/path')
    parser.add_argument('-n', dest='num_evts', type=int, default=1e9, help='max number of events for output dataset')

    args = parser.parse_args()

    print('concatenating data in', args.indir)

    read_concat_write(args.infile, args.outfile, args.num_evts)