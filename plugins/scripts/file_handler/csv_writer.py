import os
import pandas as pd


class CsvWriter:
    def __init__(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file_path = os.path.join(script_dir, '../output')
        self.output_file_path = output_file_path

    def write(self, data: list, file_name: str):
        df = pd.DataFrame(data)
        df.to_csv(f'{self.output_file_path}/{file_name}.csv', index=False)