import unittest
from predictv2 import get_data_database, define_dataframe

class testApi(unittest.TestCase):
    def test_data(self):
        datos = get_data_database('localhost')
        self.assertTrue(datos)
        self.assertFalse(define_dataframe(datos).empty)

if __name__ == '__main__':
    unittest.main()
