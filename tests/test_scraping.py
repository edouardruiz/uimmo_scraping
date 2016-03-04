import os
import tempfile

import pandas as pd

from unittest import TestCase

from immo_scraping import scraping


class TestScraping(TestCase):
    def test_print_ci_annonces_df_tuples(self):
        ci_annonces_df_tuples = [
            (750101, pd.DataFrame({'a': [0, 1], 'b': [2, 3]})),
            (750101, pd.DataFrame({'a': [4, 5], 'b': [6, 7]})),
            (750102, pd.DataFrame({'a': [8, 9], 'b': [10, 11]})),
            (750103, pd.DataFrame({'a': [12, 13], 'b': [14, 15]})),
        ]
        with tempfile.TemporaryDirectory() as temp_folder:
            scraping.print_ci_annonces_df_tuples(ci_annonces_df_tuples, temp_folder)
            self.assertTrue(os.path.isfile(os.path.join(temp_folder, '750101.xlsx')))
            self.assertTrue(os.path.isfile(os.path.join(temp_folder, '750102.xlsx')))
            self.assertTrue(os.path.isfile(os.path.join(temp_folder, '750103.xlsx')))
