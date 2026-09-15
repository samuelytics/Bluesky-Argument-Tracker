import os
import pickle
def load_pickle(filename):
    with open(filename, 'rb') as handle:
        b = pickle.load(handle)
    return b
def dump_pickle(filepath, filename, var, warn=True): 
    current_folder = os.getcwd()
    #os.chdir(filepath)
    if filename in os.listdir(filepath) and warn: 
        okay = 'O'
        while okay.lower() not in ['y','n']: 
            okay = input("WARNING: Filename is already present in directory. Overwrite? Y/N: ")
        if okay.lower() == 'n':
            os.chdir(current_folder)
            return
    with open(os.path.join(filepath,filename),'wb') as handle:
        pickle.dump(var, handle)
    return