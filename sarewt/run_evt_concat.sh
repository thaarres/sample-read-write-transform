IN="/eos/project/d/dshep/TOPCLASS/DijetAnomaly/qcd_sqrtshatTeV_13TeV_PU40_NEW"
OUT="/eos/user/k/kiwoznia/data/VAE_data/events/qcd_sqrtshatTeV_13TeV_PU40_NEW_signalregion_parts/qcd_sqrtshatTeV_13TeV_PU40_NEW_signalregion.h5"
python3 -u event_concatenate_serialization.py -in "${IN}" -out "${OUT}" -mb 2000 --signal

