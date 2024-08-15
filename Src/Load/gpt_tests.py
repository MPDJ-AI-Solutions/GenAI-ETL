import unittest
from unittest.mock import patch
import pandas as pd
import pandas.testing as pdt
from chatgpt import *


class TestReplaceNaN(unittest.TestCase):
    def test_replace_nan_with_none(self):
        self.assertIsNone(replace_nan(float('nan')))
    
    def test_replace_non_nan_value(self):
        self.assertEqual(replace_nan(10), 10)
    
    def test_replace_non_float_value(self):
        self.assertEqual(replace_nan('hello'), 'hello')
    
    def test_replace_non_numeric_value(self):
        self.assertEqual(replace_nan('NaN'), 'NaN')


class TestGetConfig(unittest.TestCase):
    @patch('chatgpt.ConfigParser')
    def test_get_config(self, mock_config_parser):
        # Arrange
        mock_parser = mock_config_parser.return_value
        mock_parser.has_section.return_value = True
        mock_parser.items.return_value = [
            ('dbname', 'test_db'),
            ('user', 'test_user'),
            ('password', 'test_password'),
            ('host', 'test_host'),
            ('port', 'test_port')
        ]

        expected_result = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'test_host',
            'port': 'test_port'
        }
        
        
        # Act
        result = get_config()

        # Assert 
        self.assertEqual(result, expected_result)
        mock_parser.read.assert_called_once_with('./Src/Database/chatgpt_connection.cfg')
        mock_parser.has_section.assert_called_once_with('database')
        mock_parser.items.assert_called_once_with('database')


class TestCleanColumnValues(unittest.TestCase):

    def test_clean_column_values(self):
        # Arrange 
        city_data = {
            'residential_buildings': pd.DataFrame({
                'Nazwa': ['City (1)', 'Another City (1)']
            }),
            'sold_apartments': pd.DataFrame({
                'Nazwa': ['Powiat m. City', 'Powiat m. Another City']
            }),
            'median_prices': pd.DataFrame({
                'Nazwa': ['Powiat m. City', 'Powiat m. Another City']
            }),
            'mean_prices': pd.DataFrame({
                'Nazwa': ['Powiat m. City', 'Powiat m. Another City']
            }),
            'vacancies': pd.DataFrame({
                'Nazwa': ['DOLNOŚLĄSKIE', 'KUJAWSKO-POMORSKIE']
            }),
            'average_salary': pd.DataFrame({
                'Nazwa': ['Powiat m. City', 'Powiat m. Another City']
            })
        }
        
        
        # Act 
        cleaned_data = clean_column_values(city_data)

        # Assert 
        self.assertEqual(cleaned_data['residential_buildings']['Nazwa'].tolist(), ['City', 'Another City'])
        self.assertEqual(cleaned_data['sold_apartments']['Nazwa'].tolist(), ['City', 'Another City'])
        self.assertEqual(cleaned_data['median_prices']['Nazwa'].tolist(), ['City', 'Another City'])
        self.assertEqual(cleaned_data['mean_prices']['Nazwa'].tolist(), ['City', 'Another City'])
        self.assertEqual(cleaned_data['vacancies']['Nazwa'].tolist(), ['Wrocław', 'Bydgoszcz'])
        self.assertEqual(cleaned_data['average_salary']['Nazwa'].tolist(), ['City', 'Another City'])
    

class TestCleanCityData(unittest.TestCase):
     
    def test_clean_city_data(self):
        # Arrange
        input = {
            'residential_buildings': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A (1)', 'City B (1)', 'City C (1)']
            }),
            'sold_apartments': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['Powiat m. City A', 'Powiat m. City B', 'Powiat m. City C']
            }),
            'median_prices': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['Powiat m. City A', 'Powiat m. City B', 'Powiat m. City C']
            }),
            'mean_prices': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['Powiat m. City A', 'Powiat m. City B', 'Powiat m. City C']
            }),
            'vacancies': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['DOLNOŚLĄSKIE', 'ŚLĄSKIE', 'MAZOWIECKIE']
            }),
            'average_salary': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['Powiat m. City A', 'Powiat m. City B', 'Powiat m. City C']
            })
        }

        expected_output = {
            'residential_buildings': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A', 'City B', 'City C']
            }),
            'sold_apartments': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A', 'City B', 'City C']
            }),
            'median_prices': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A', 'City B', 'City C']
            }),
            'mean_prices': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A', 'City B', 'City C']
            }),
            'vacancies': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['Wrocław', 'Katowice', 'Warszawa']
            }),
            'average_salary': pd.DataFrame({
                'Kod': ['01', '02', '03'],
                'Nazwa': ['City A', 'City B', 'City C']
            })
        }

        # Act
        cleaned_city_data = clean_column_values(input)

        # Assert
        for key in cleaned_city_data:
            pdt.assert_frame_equal(cleaned_city_data[key], expected_output[key])


class TestMapApartmentPricesBooleanColumns(unittest.TestCase):
    def test_map_apartment_prices_boolean_columns(self):
        # Arrange
        data = {
            'hasParkingSpace': ['yes', 'no', 'yes'],
            'hasBalcony': ['no', 'yes', 'no'],
            'hasElevator': ['yes', 'yes', 'no'],
            'hasSecurity': ['no', 'yes', 'yes'],
            'hasStorageRoom': ['yes', 'no', 'yes']
        }
        
        expected_data = {
            'hasParkingSpace': [True, False, True],
            'hasBalcony': [False, True, False],
            'hasElevator': [True, True, False],
            'hasSecurity': [False, True, True],
            'hasStorageRoom': [True, False, True]
        }

        df = pd.DataFrame(data)
        expected_result = pd.DataFrame(expected_data)

        # Act
        result = map_apartment_prices_boolean_columns(df)

        # Assert 
        self.assertTrue(result.equals(expected_result))


if __name__ == '__main__':
    unittest.main()