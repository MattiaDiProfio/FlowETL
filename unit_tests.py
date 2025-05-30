import unittest
from docker_services.dags.planning_utils import *

class TestDataTaskNodes(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # define an internal representation containing missing values, duplicated rows, and numerical outliers
        self.source = [
            ["studentID", "Name",        "Age",    "Salary", "City"         ],
            [1,           "Alice",        25,      None,     "New York"     ],
            [2,           None,           30,      60000,     None          ],
            [3,           "Charlie",      None,    None,     "Chicago"      ],
            [None,        "David",        40,      None,     None           ],
            [5,           "Eve",          22,      45000,    "Houston"      ],
            [6,           "Frank",        None,    70000,    "Phoenix"      ],
            [7,           "Grace",        None,    None,     None           ],
            [None,        "Hank",         28,      52000,    "San Antonio"  ],
            [9,           "Ivy",          None,    None,    None            ],
            [10,          "Jack",         None,    75000,    None           ],
            [1,           "Alice",        25,      None,    "New York"      ],
            [2,           None,           30,      60000,    None           ],
            [5,           "Eve",          None,    None,    "Houston"       ],
            [11,          "Outlier",      None,    1000000,  "Neverland"    ],
            [12,          "Extreme",      1234,    None,     "Desert"       ],
            [13,          "Huge Outlier", 9999,    99999999, "Utopia"       ],
        ]

    def assertInternalRepresentationsEquality(self, original_ir, transformed_ir):
        """ 
        Custom assertion to check that two internal representations contain the same rows, order is disregarded
        """

        # convert each row in both IRs to a hashed version of itself
        hashed_original_ir = [ "".join([ str(cell) for cell in row ]) for row in original_ir ]
        hashed_transformed_ir = [ "".join([ str(cell) for cell in row ]) for row in transformed_ir ]

        # check that all rows in the hashed version of the original ir are in the hashed version of the transformed, and vice versa
        if sorted(hashed_original_ir) != sorted(hashed_transformed_ir) or len(hashed_original_ir) != len(hashed_transformed_ir):
            raise self.failureException("Mismatch between the two internal representation")

    def test_custom_assertion(self):
        """ 
        Test that the custom assertion works as expected 
        """

        a = [[1,2,3], [4,5,6]]
        b = [[4,5,6], [1,2,3]]
        c = [[1,2,3], [6,5,4]]

        self.assertInternalRepresentationsEquality(a, b)
        try:
            # expected to fail, because regardless of row order, the two IRs are different
            self.assertInternalRepresentationsEquality(b, c)
        except AssertionError:
            pass

    def test_impute_missing_values(self):
        """
        Test if the missing value handler with an "impute" strategy works correctly
        this strategy should replace all None values with a placeholder appropiate for the 
        column type
        """

        expected = [
            ["studentID", "Name",        "Age",    "Salary", "City"         ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,    "NA"           ],
            [3,           "Charlie",      0.0,     0.0,      "Chicago"      ],
            [0.0,         "David",        40,      0.0,      "NA"           ],
            [5,           "Eve",          22,      45000,    "Houston"      ],
            [6,           "Frank",        0.0,     70000,    "Phoenix"      ],
            [7,           "Grace",        0.0,     0.0,      "NA"           ],
            [0.0,         "Hank",         28,      52000,    "San Antonio"  ],
            [9,           "Ivy",          0.0,     0.0,      "NA"           ],
            [10,          "Jack",         0.0,     75000,    "NA"           ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,    "NA"           ],
            [5,           "Eve",          0.0,     0.0,      "Houston"      ],
            [11,          "Outlier",      0.0,     1000000,  "Neverland"    ],
            [12,          "Extreme",      1234,    0.0,      "Desert"       ],
            [13,          "Huge Outlier", 9999,    99999999, "Utopia"       ],
        ]
        
        inferred_schema = infer_schema(self.source)
        actual = missing_value_handler(self.source, inferred_schema, 'impute')
        self.assertInternalRepresentationsEquality(expected, actual)

    def test_duplicate_row_handler(self):
        """
        Test if the duplicate row handler works as expected. It should remove all duplicate rows from the IR 
        """

        expected = [
            ["studentID", "Name",        "Age",    "Salary", "City"         ],
            [1,           "Alice",        25,      None,     "New York"     ],
            [2,           None,           30,      60000,     None          ],
            [3,           "Charlie",      None,    None,     "Chicago"      ],
            [None,        "David",        40,      None,     None           ],
            [5,           "Eve",          22,      45000,    "Houston"      ],
            [6,           "Frank",        None,    70000,    "Phoenix"      ],
            [7,           "Grace",        None,    None,     None           ],
            [None,        "Hank",         28,      52000,    "San Antonio"  ],
            [9,           "Ivy",          None,    None,     None           ],
            [10,          "Jack",         None,    75000,    None           ],
            [5,           "Eve",          None,    None,     "Houston"      ],
            [11,          "Outlier",      None,    1000000,  "Neverland"    ],
            [12,          "Extreme",      1234,    None,     "Desert"       ],
            [13,          "Huge Outlier", 9999,    99999999, "Utopia"       ]
        ]

        actual = duplicate_values_handler(self.source)
        self.assertInternalRepresentationsEquality(expected, actual)

    def test_drop_numerical_outliers(self):
        """
        Test if the outlier value handler with an "drop" strategy works correctly
        this strategy should drop all rows containing numerical outliers

        NOTE : this task node requires that we handling missing values first!
        """

        expected = [
            ["studentID", "Name",        "Age",    "Salary", "City"         ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,     "NA"          ],
            [3,           "Charlie",      0.0,     0.0,      "Chicago"      ],
            [0.0,         "David",        40,      0.0,      "NA"           ],
            [5,           "Eve",          22,      45000,    "Houston"      ],
            [6,           "Frank",        0.0,     70000,    "Phoenix"      ],
            [7,           "Grace",        0.0,     0.0,      "NA"           ],
            [0.0,         "Hank",         28,      52000,    "San Antonio"  ],
            [9,           "Ivy",          0.0,     0.0,      "NA"           ],
            [10,          "Jack",         0.0,     75000,    "NA"           ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,    "NA"           ],
            [5,           "Eve",          0.0,     0.0,      "Houston"      ],
        ]

        inferred_schema = infer_schema(self.source)
        partially_cleaned_source = missing_value_handler(self.source, inferred_schema, 'impute')
        actual = outlier_handler(partially_cleaned_source, inferred_schema, 'drop')
        self.assertInternalRepresentationsEquality(expected, actual)

    def test_impute_numerical_outliers(self):
        """
        Test if the outlier value handler with an "impute" strategy works correctly
        this strategy should replace all numerical outliers with the median of their column

        NOTE this task node requires that we handling missing values first!
        """

        expected = [
            ["studentID", "Name",        "Age",    "Salary", "City"         ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,    "NA"           ],
            [3,           "Charlie",      0.0,     0.0,      "Chicago"      ],
            [0.0,         "David",        40,      0.0,      "NA"           ],
            [5,           "Eve",          22,      45000,    "Houston"      ],
            [6,           "Frank",        0.0,     70000,    "Phoenix"      ],
            [7,           "Grace",        0.0,     0.0,      "NA"           ],
            [0.0,         "Hank",         28,      52000,    "San Antonio"  ],
            [9,           "Ivy",          0.0,     0.0,      "NA"           ],
            [10,          "Jack",         0.0,     75000,    "NA"           ],
            [1,           "Alice",        25,      0.0,      "New York"     ],
            [2,           "NA",           30,      60000,    "NA"           ],
            [5,           "Eve",          0.0,     0.0,      "Houston"      ],
            [11,          "Outlier",      0.0,     22500.0,  "Neverland"    ],
            [12,          "Extreme",      23.5,    0.0,      "Desert"       ],
            [13,          "Huge Outlier", 23.5,    22500.0,  "Utopia"       ]
        ]

        inferred_schema = infer_schema(self.source)
        partially_cleaned_source = missing_value_handler(self.source, inferred_schema, 'impute')
        actual = outlier_handler(partially_cleaned_source, inferred_schema, 'impute')
        self.assertInternalRepresentationsEquality(expected, actual)

    def test_infer_schema(self):
        """
        Test that the infer_schema method works as expected.
        """

        nums_and_strings_column = [['columnHeader'], ['0.5'], [9], [34], ['221']]
        all_strings_column = [['columnHeader'], ['a'], ['hello'], ['test'], ['dissertation']]
        boolean_column_1 = [['columnHeader'], [True], [False], [True], [True], [False], [False]]
        boolean_column_2 = [['columnHeader'], ['Y'], ['N'], ['N'], ['Y'], ['Y'], ['Y']]
        ambiguous_column = [['columnHeader'], ['mattia'], [{'k': 10}], [0.4]]
        complex_column = [['columnHeader'], [[1, 2, 3]], [['hello', 'world']], [[{'k': 'v'}, 10]]]
        numerical_column = [['columnHeader'], [1], [2.0], [3.0], [4.5], [5], [6], [7], [8]]

        self.assertEqual(infer_schema(nums_and_strings_column), {'columnHeader' : 'number'})
        self.assertEqual(infer_schema(all_strings_column), {'columnHeader' : 'string'})
        self.assertEqual(infer_schema(boolean_column_1), {'columnHeader' : 'boolean'})
        self.assertEqual(infer_schema(boolean_column_2), {'columnHeader' : 'boolean'})
        self.assertEqual(infer_schema(ambiguous_column), {'columnHeader' : 'ambiguous'})
        self.assertEqual(infer_schema(complex_column), {'columnHeader' : 'complex'})
        self.assertEqual(infer_schema(numerical_column), {'columnHeader' : 'number'})

    def test_data_quality_calculator_node(self):
        """
        Test that the data quality calculation process works as expected.
        """

        inferred_schema = infer_schema(self.source)
        dq_before = compute_dq(self.source, inferred_schema)

        # apply a plan which handles data task nodes an optimistic way -> impute over dropping
        missing = missing_value_handler(self.source, inferred_schema, 'impute')
        duplicate = duplicate_values_handler(missing)
        outlier = outlier_handler(duplicate, inferred_schema, 'impute')
        
        dq_after = compute_dq(outlier, inferred_schema)
        self.assertTrue(dq_after > dq_before)


if __name__ == '__main__':
    unittest.main()