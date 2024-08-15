import unittest
import math
from unittest.mock import patch, MagicMock
from claude import *

class TestReplaceNaN(unittest.TestCase):
    def test_replace_nan_with_none(self):
        self.assertIsNone(replace_nan(float('nan')))
    
    def test_replace_non_nan_value(self):
        value = 10
        self.assertEqual(replace_nan(value), value)
    
    def test_replace_non_float_value(self):
        value = 'test'
        self.assertEqual(replace_nan(value), value)
    
    def test_replace_non_numeric_value(self):
        value = 'NaN'
        self.assertEqual(replace_nan(value), value)
    
    def test_replace_nan_in_list(self):
        values = [1, float('nan'), 2, float('nan')]
        expected = [1, None, 2, None]
        self.assertEqual([replace_nan(value) for value in values], expected)
    
    def test_replace_nan_in_tuple(self):
        values = (1, float('nan'), 2, float('nan'))
        expected = (1, None, 2, None)
        self.assertEqual(tuple(replace_nan(value) for value in values), expected)


class TestClaude(unittest.TestCase):
    def test_map_voivodeship_to_capital(self):
        # Create a sample dataframe
        df = pd.DataFrame({
            'Nazwa': ['DOLNOŚLĄSKIE', 'KUJAWSKO-POMORSKIE', 'LUBELSKIE'],
        })

        # Apply the function
        result = map_voivodeship_to_capital(df)

        # Define the expected output
        expected = pd.DataFrame({
            'Nazwa': ['Wrocław', 'Bydgoszcz', 'Lublin']
        })

        # Assert the result
        pd.testing.assert_frame_equal(result, expected)


class TestRemovePrefix(unittest.TestCase):
    def test_remove_prefix(self):
        # Test case 1: Remove prefix 'Powiat m. ' from column 'Nazwa'
        df = pd.DataFrame({'Nazwa': ['Powiat m. Warsaw', 'Powiat m. Krakow']})
        expected_df = pd.DataFrame({'Nazwa': ['Warsaw', 'Krakow']})
        result_df = remove_prefix(df, 'Nazwa', 'Powiat m. ')
        self.assertTrue(result_df.equals(expected_df))

        # Test case 2: Remove prefix ' (1)' from column 'Nazwa'
        df = pd.DataFrame({'Nazwa': ['City (1)', 'Town (1)']})
        expected_df = pd.DataFrame({'Nazwa': ['City', 'Town']})
        result_df = remove_prefix(df, 'Nazwa', ' (1)')
        self.assertTrue(result_df.equals(expected_df))


class TestMapYesNoToBool(unittest.TestCase):
    def test_map_yes_no_to_bool(self):
        # Create a sample dataframe
        df = pd.DataFrame({'col1': ['yes', 'no', 'yes'], 'col2': ['no', 'yes', 'no']})

        # Map 'yes' and 'no' to True and False
        expected_result = pd.DataFrame({'col1': [True, False, True], 'col2': [False, True, False]})

        # Apply the function
        result = map_yes_no_to_bool(df, ['col1', 'col2'])

        # Assert the expected result
        pd.testing.assert_frame_equal(result, expected_result)


class TestCamelToSnake(unittest.TestCase):
    def test_single_word(self):
        # Test with a single word
        input_word = "hello"
        expected_output = "hello"
        self.assertEqual(camel_to_snake(input_word), expected_output)

    def test_camel_case(self):
        # Test with camel case input
        input_word = "helloWorld"
        expected_output = "hello_world"
        self.assertEqual(camel_to_snake(input_word), expected_output)

    def test_multiple_words(self):
        # Test with multiple words in camel case
        input_word = "helloWorldPython"
        expected_output = "hello_world_python"
        self.assertEqual(camel_to_snake(input_word), expected_output)

    def test_numbers(self):
        # Test with numbers in the input
        input_word = "helloWorld123"
        expected_output = "hello_world123"
        self.assertEqual(camel_to_snake(input_word), expected_output)

    def test_acronyms(self):
        # Test with acronyms in the input
        input_word = "helloWorldAPI"
        expected_output = "hello_world_api"
        self.assertEqual(camel_to_snake(input_word), expected_output)



if __name__ == '__main__':
    unittest.main()


