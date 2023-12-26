import unittest
from exercise import clean_data, chunk_generator, main


class TestEmployeePairing(unittest.TestCase):
    def test_clean_data(self):
        employees = [
            {"department": "R&D", "name": "emp1", "age": 46},
            {"department": "Sales", "name": "emp2", "age": 28},
            {"department": "R&D", "name": "emp3", "age": 33},
            {"department": "R&D", "name": "emp4", "age": 29},
            {"department": "R&D", "name": "emp1", "age": 46},  # Duplicate
        ]
        cleaned_employees = list(clean_data(employees))
        self.assertEqual(len(cleaned_employees), 4)

    def test_chunk_generator(self):
        """Test the chunk_generator function."""
        employees = [
            {"department": "R&D", "name": "emp1", "age": 46},
            {"department": "Sales", "name": "emp2", "age": 28},
            {"department": "R&D", "name": "emp3", "age": 33},
            {"department": "R&D", "name": "emp4", "age": 29},
            {"department": "Eng", "name": "emp5", "age": 50},
        ] * 5  # 4 duplicate employees
        chunks = list(chunk_generator(employees, 2))
        self.assertEqual(len(chunks), 3)

    def test_main_function(self):
        """Test the main function."""
        employees_list = [
            {"department": "R&D", "name": "emp1", "age": 46},
            {"department": "Sales", "name": "emp2", "age": 28},
            {"department": "R&D", "name": "emp3", "age": 33},
            {"department": "R&D", "name": "emp4", "age": 29},
            {"department": "R&D", "name": "emp1", "age": 46},  # Duplicate
            {"department": "Sales", "name": "emp2", "age": 28},  # Duplicate
        ]

        unique_pairs = main(employees_list, 2)
        self.assertIsInstance(unique_pairs, list)
        self.assertTrue(
            all(isinstance(pair, tuple) and len(pair) == 2 for pair in unique_pairs)
        )
        self.assertEqual(len(unique_pairs), 4)


if __name__ == "__main__":
    unittest.main()
