from datetime import datetime
from csvrelational import CSVBase
from csvrelational.parsers import is_integer, is_datetime, is_float, is_string, is_foreign_key
from conftest import User, Email


def test_csv_loading():

    class UserCSV(CSVBase):
        __filename__ = './tests/data/users-1.csv'
        __separator__ = ','
        __primary_key__ = 'id'

        id = is_integer()
        username = is_string()
        first_name = is_string()
        last_name = is_string()
        age = is_integer()
        date_of_birth = is_datetime('%Y-%m-%d')
        score = is_float()


    user_df = UserCSV.get_dataframe()

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


def test_csv_to_database(db_session):

    class UserCSV(CSVBase):
        __filename__ = './tests/data/users-1.csv'
        __model__ = User
        __separator__ = ','
        __primary_key__ = 'id'

        id = is_integer()
        username = is_string()
        first_name = is_string()
        last_name = is_string()
        age = is_integer()
        date_of_birth = is_datetime('%Y-%m-%d')
        score = is_float()

    UserCSV.save_to_db(db_session)

    df = UserCSV.get_dataframe()

    for id in df['id']:
        user_csv = df.loc[id]
        db_id = user_csv['db_id']
        user_db = db_session.query(User).filter(User.id==db_id).first()
        assert user_db.username == user_csv['username']
        assert user_db.first_name == user_csv['first_name']
        assert user_db.last_name == user_csv['last_name']
        assert user_db.score == user_csv['score']


def test_csv_to_database_relation(db_session):

    class UserCSV(CSVBase):
        __filename__ = './tests/data/users-1.csv'
        __model__ = User
        __separator__ = ','
        __primary_key__ = 'id'

        id = is_integer()
        username = is_string()
        first_name = is_string()
        last_name = is_string()
        age = is_integer()
        date_of_birth = is_datetime('%Y-%m-%d')
        score = is_float()

    class EmailCSV(CSVBase):
        __filename__ = './tests/data/emails-1.csv'
        __model__ = Email
        __separator__ = ','
        __primary_key__ = 'id'
        __unique__ = ['address']

        id = is_integer()
        user_id = is_foreign_key(UserCSV, backref='emails')
        address = is_string()

    UserCSV.save_to_db(db_session)
    EmailCSV.save_to_db(db_session)

    user = db_session.query(User).filter(User.username == 'max').first()

    assert len(user.emails) == 1
    assert user.emails[0].address == 'max@test.com'

    user = db_session.query(User).filter(User.username == 'madadam').first()

    assert len(user.emails) == 2
    assert set([email.address for email in user.emails]) == set(['madadam@test.com', 'madadam2@test.com'])
