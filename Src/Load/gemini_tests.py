import unittest
from unittest.mock import patch
import pandas as pd
import pandas.testing as pdt
from gemini import *


class TestReplaceNaN(unittest.TestCase):
    def test_replace_nan_with_none(self):
        self.assertIsNone(replace_nan(float('nan')))
    
    def test_replace_non_nan_value(self):
        self.assertEqual(replace_nan(10), 10)
    
    def test_replace_non_float_value(self):
        self.assertEqual(replace_nan('hello'), 'hello')
    
    def test_replace_non_numeric_value(self):
        self.assertEqual(replace_nan('NaN'), 'NaN')


class TestLoadCityData(unittest.TestCase):
    @patch('gemini.glob.glob')
    @patch('gemini.pd.read_csv')
    def test_load_city_data(self, mock_read_csv, mock_glob):
        # Arrange
        data_path = "./Data"
        file_patterns = {
            'residential_buildings': f"{data_path}/GOSP_2909_CTAB_*",
            'apartments_sold': f"{data_path}/RYNE_3783_CTAB_*",
            'median_price': f"{data_path}/RYNE_3787_CTAB_*",
            'mean_price': f"{data_path}/RYNE_3788_CTAB_*",
            'vacancies': f"{data_path}/RYNE_4294_CTAB_*",
            'avg_salary': f"{data_path}/WYNA_2497_CTAB_*"
        }
        mock_glob.side_effect = lambda pattern: [pattern.replace('*', 'file1.csv')]
        mock_read_csv.return_value = pd.DataFrame({'column1': [1, 2, 3]})

        # Act
        city_data = load_city_data(data_path)

        # Assert
        self.assertEqual(len(city_data), len(file_patterns))
        for key, pattern in file_patterns.items():
            self.assertIn(key, city_data)
            self.assertEqual(city_data[key].shape, (3, 1))
            mock_glob.assert_any_call(pattern)
            mock_read_csv.assert_any_call(pattern.replace('*', 'file1.csv'), sep=';')


class TestLoadJobOffers(unittest.TestCase):
    @patch('gemini.glob.glob')
    @patch('gemini.pd.read_csv')
    @patch('gemini.pd.concat')
    def test_load_job_offers(self, mock_concat, mock_read_csv, mock_glob):
        # Arrange
        data_path = "./Data"
        file_pattern = f"{data_path}/*_soft_eng_jobs_pol.csv"
        files = ['file1.csv', 'file2.csv']
        dfs = ['df1', 'df2']
        expected_result = 'concatenated_df'

        mock_glob.return_value = files
        mock_read_csv.side_effect = dfs
        mock_concat.return_value = expected_result

        # Act
        result = load_job_offers(data_path)

        # Assert
        mock_glob.assert_called_once_with(file_pattern)
        mock_read_csv.assert_called_with('file2.csv')
        mock_concat.assert_called_once_with(['df1', 'df2'], ignore_index=True)
        self.assertEqual(result, expected_result)


class TestLoadApartmentPrices(unittest.TestCase):
    @patch('gemini.glob.glob')
    @patch('gemini.pd.read_csv')
    def test_load_apartment_prices(self, mock_read_csv, mock_glob):
        # Arrange
        mock_glob.return_value = ['file1.csv', 'file2.csv']
        mock_read_csv.side_effect = [
            pd.DataFrame({'price': [1000, 2000]}),
            pd.DataFrame({'price': [3000, 4000]})
        ]

        expected_result = pd.DataFrame({'price': [1000, 2000, 3000, 4000]})

        # Act
        result = load_apartment_prices('data_path')

        # Assert 
        pd.testing.assert_frame_equal(result, expected_result)
        mock_glob.assert_called_once_with('data_path/apartments_pl_*')
        mock_read_csv.assert_called_with('file2.csv')


if __name__ == '__main__':
    unittest.main()