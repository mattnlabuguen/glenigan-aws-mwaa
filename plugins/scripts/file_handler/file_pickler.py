import os
import pickle


class FilePickler:
    def __init__(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pickle_file_path = os.path.join(script_dir, '../output')
        self.pickle_file_path = pickle_file_path

    def load(self, file_name: str):
        print(self.pickle_file_path)
        with open(f'{self.pickle_file_path}/{file_name}.pkl', "rb") as pickle_file:
            raw_data_list = pickle.load(pickle_file)

        return raw_data_list

    def dump(self, data: list, file_name: str):
        with open(f'{self.pickle_file_path}/{file_name}.pkl', "wb") as pickle_file:
            pickle.dump(data, pickle_file)