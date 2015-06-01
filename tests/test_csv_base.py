from datetime import datetime
from csvrelational import CSVBase
from csvrelational.parsers import is_integer, is_datetime, is_float, is_string


def test_csv_loading():

    class User(CSVBase):
        __filename__ = './data/users-1.csv'
        __separator__ = ','
        __primary_key__ = 'id'

        id = is_integer()
        username = is_string()
        first_name = is_string()
        last_name = is_string()
        age = is_integer()
        date_of_birth = is_datetime('%Y-%m-%d')
        score = is_float()


    user_df = User.get_dataframe()

    row3 = user_df.loc[176]
    assert row3['username'] == 'eve'
    assert row3['first_name'] == 'Eve'
    assert row3['date_of_birth'] == datetime(1966, 3, 1, 0, 0)
    assert (row3['score'] - 8.9) < 0.0001

    row1 = user_df.loc[172]
    assert row1['username'] == 'adam'
    assert row1['first_name'] == 'Adam'
    assert row1['last_name'] == 'First'
    assert row1['date_of_birth'] == datetime(1954, 5, 29, 0, 0)
    assert (row1['score'] - 4.5) < 0.0001

    row4 = user_df.loc[194]
    assert row4['username'] == 'max'
    assert row4['first_name'] == 'Max'
    assert row4['last_name'] == 'von Sydow'
    assert row4['date_of_birth'] == datetime(1927, 7, 15, 0, 0)
    assert row4['age'] == 54
    assert (row4['score'] - 7.6) < 0.0001