import unittest
import polars as pl
from polars.testing import assert_frame_equal
from datetime import datetime
from alpha_utils import (parse_data,
                         check_new_field,
                         check_removed_field,
                         update_records,
                         insert_new_records,
                         run_end_to_end)


class TestDfFunctions(unittest.TestCase):
    """
    Unit testing for the functions in the alpha_utils.py file
    """
    def test_new_field(self):
        """
        Check if a new field is added in the source dataframe that is not in the target dataframe
        """
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31', '2020-09-30'],
            'reportedCurrency': ['USD', 'USD', 'USD'],
            'totalRevenue': [1000, 2000, 3000],
            'netIncome': [100, 200, 300]
        })
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'reportedCurrency': ['USD', 'USD'],
            'totalRevenue': [1000, 2000]
        })
        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'reportedCurrency': ['USD', 'USD'],
            'totalRevenue': [1000, 2000],
            'netIncome': [100, 200]
        })
        print(source)
        print(target)
        result = check_new_field(df_target=target, df_source=source, id_col='fiscalDateEnding')
        # assert final equals result
        # sort the order of the columns
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)),
                         None)

    def test_removed_field(self):
        """
        Test if a field is removed from the source dataframe that is not in the target dataframe
        """
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31', '2020-09-30'],
            # 'reportedCurrency': ['USD', 'USD', 'USD'],
            'totalRevenue': [1000, 2000, 3000],
            'netIncome': [100, 200, 300]
        })
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'reportedCurrency': ['USD', 'USD'],
            'totalRevenue': [1000, 2000]
        })
        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31', '2020-09-30'],
            'reportedCurrency': [None, None, None],
            'totalRevenue': [1000, 2000, 3000],
            'netIncome': [100, 200, 300]
        })
        result = check_removed_field(df_target=target, df_source=source)
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        # assert final equals result
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)),
                         None)

    def test_remove_and_new_field(self):
        """
        Test if a field is removed from the source dataframe that is not in the target dataframe
        and a new field is added in the source dataframe that is not in the target dataframe
        """
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31', '2020-09-30'],
            'totalRevenue': [1000, 2000, 3000],
            'netIncome': [100, 200, 300]
        })
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'reportedCurrency': ['USD', 'USD'],
            'totalRevenue': [1000, 2000]
        })
        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'reportedCurrency': ['USD', 'USD'],
            'totalRevenue': [1000, 2000],
            'netIncome': [100, 200]
        })

        final2 = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31', '2020-09-30'],
            'reportedCurrency': [None, None, None],
            'totalRevenue': [1000, 2000, 3000],
            'netIncome': [100, 200, 300]
        })

        result = check_new_field(df_target=target, df_source=source, id_col='fiscalDateEnding')
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()

        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)),
                         None)
        result2 = check_removed_field(df_target=result, df_source=source)
        final2_columns = list(final2.columns).sort()
        result2_columns = list(result2.columns).sort()

        # assert final equals result
        self.assertEqual(assert_frame_equal(final2.select(final2_columns), result2.select(result2_columns)),
                         None)
        # check if the columns are the same
        self.assertEqual(len(result.columns), len(result2.columns))

    def test_update_records(self):
        """
        Test the update records function
        """
        update_time = datetime.now()
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31'],
            'totalRevenue': [1000],
            'is_current': [True],
            'update_time': [update_time]
        })
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'totalRevenue': [2000, 3000],
        })
        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2021-03-31'],
            'totalRevenue': [1000, 2000],
            'is_current': [False, True],
            'update_time': [update_time, update_time]
        })
        result = update_records(target=target, source=source, on='fiscalDateEnding', update_time=update_time)
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)), None)

    def test_insert_new_records(self):
        """
        Test updating and inserting new records to the target dataframe
        """
        update_time = datetime.now()
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31'],
            'totalRevenue': [1000],
            'is_current': [True],
            'update_time': [update_time]
        })
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'totalRevenue': [2000, 3000],
        })
        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2021-03-31', '2020-12-31'],
            'totalRevenue': [1000, 2000, 3000],
            'is_current': [False, True, True],
            'update_time': [update_time, update_time, update_time]
        })
        final = final.sort('fiscalDateEnding')

        result = update_records(target=target, source=source, on='fiscalDateEnding', update_time=update_time)
        final_result = insert_new_records(target=result, source=source, id_col='fiscalDateEnding', update_time=update_time)
        final_columns = list(final.columns).sort()
        result_columns = list(final_result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), final_result.select(result_columns)), None)

    def test_end_to_end(self):
        """
        Test the end-to-end process of updating and inserting new records
        """
        update_time = datetime.now()
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31'],
            'totalRevenue': [1000],
            'is_current': [True],
            'update_time': [update_time]
        })
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'Sales': [2000, 3000],
            'Currency': ['USD', 'USD']
        })

        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2021-03-31', '2020-12-31'],
            'totalRevenue': [1000, None, 3000],
            'is_current': [False, True, True],
            'update_time': [update_time, update_time, update_time],
            'Sales': [2000, 2000, 3000],
            'Currency': ['USD', 'USD', 'USD']
        })
        final = final.sort('fiscalDateEnding')
        result = run_end_to_end(target=target, source=source, id_col='fiscalDateEnding', update_time=update_time)
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)), None)

    def test_end_to_end2(self):
        """
        Test the end-to-end process of updating and inserting new records
        test case 2
        """
        update_time = datetime.now()
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31'],
            'totalRevenue': [1000],
            'is_current': [True],
            'update_time': [update_time]
        })
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'totalRevenue': [2000, 3000]
        })

        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2021-03-31', '2020-12-31'],
            'totalRevenue': [1000, 2000, 3000],
            'is_current': [False, True, True],
            'update_time': [update_time, update_time, update_time]
        })
        final = final.sort('fiscalDateEnding')
        result = run_end_to_end(target=target, source=source, id_col='fiscalDateEnding', update_time=update_time)
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)), None)



    def test_end_to_end3(self):
        """
        Test the end-to-end process of updating and inserting new records
        test case 3
        """
        update_time = datetime.now()
        target = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31'],
            'totalRevenue': [1000],
            'is_current': [True],
            'update_time': [update_time]
        })
        source = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2020-12-31'],
            'sales': [2000, 3000]
        })

        final = pl.DataFrame({
            'fiscalDateEnding': ['2021-03-31', '2021-03-31', '2020-12-31'],
            'totalRevenue': [1000, None, None],
            'is_current': [False, True, True],
            'sales': [2000, 2000, 3000],
            'update_time': [update_time, update_time, update_time]
        })
        final = final.sort('fiscalDateEnding')
        result = run_end_to_end(target=target, source=source, id_col='fiscalDateEnding', update_time=update_time)
        final_columns = list(final.columns).sort()
        result_columns = list(result.columns).sort()
        self.assertEqual(assert_frame_equal(final.select(final_columns), result.select(result_columns)), None)

if __name__ == '__main__':
    # test_new_field()
    # test_removed_field()
    # test_remove_and_new_field()
    unittest.main()
