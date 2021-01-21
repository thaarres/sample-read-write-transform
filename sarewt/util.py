import operator
import pofah.jet_sample as js

def filter_arrays_on_value(*arrays, filter_arr, filter_val, comp=operator.gt):
    idx_after_cut = comp(filter_arr,filter_val)
    #print('{0} events passed mass cut at {1}'.format(sum(idx_after_cut), filter_val))
    return [a[idx_after_cut] for a in arrays] # a[multi_idx] = smart idx => data copied in result (unlike slicing)


def mask_arrays(*arrays, mask):
    return [a[idx_after_cut] for a in arrays] # a[multi_idx] = smart idx => data copied in result (unlike slicing)


def get_mask_for_cuts(cut_dict, features):

    mask = np.ones(len(features), dtype=bool)
    
    for key, value in cut_dict.items():
    
        # | dEtaJJ | cuts into sideband and signalregion
        if key == 'sideband':
            mask *= np.abs(features[:, js.JetSample.FEAT_IDX['DeltaEtaJJ']]) > 1.4
        else if key == 'signalregion':
            mask *= np.abs(features[:, js.JetSample.FEAT_IDX['DeltaEtaJJ']]) <= 1.4
    
        # mJJ and jet-pt cuts
        else if key == 'mJJ' or key == 'j1Pt' or key == 'j2Pt':
            mask *= features[:, js.JetSample.FEAT_IDX[key]] > value
    
        # |jet-eta| cuts
        else if key == 'j1Eta':
            mask *= np.abs(features[:, js.JetSample.FEAT_IDX[key]]) < value
        else if key == 'j2Eta':
            mask *= np.abs(features[:, js.JetSample.FEAT_IDX['DeltaEtaJJ']] + features[:, js.JetSample.FEAT_IDX[key]]) < value

