import pandas
from csvrelational.database import (make_search_expression,
                                    make_dict,
                                    exclude_keys,
                                    get_or_create,
                                    clear_models)


class CSVBaseMeta(type):
    def __new__(metaclass, classname, classparents, classattrs):
        csv_filename = classattrs.get('__filename__')
        sep = classattrs.get('__separator__')
        index_column = classattrs.get('__primary_key__')

        if classname != 'CSVBase':
            if not csv_filename:
                raise Exception('CSV file needs to be specified')

            if not index_column:
                raise Exception('A primary key column must be set')

            if not sep:
                sep = ','

            parsers = {}
            for (key, value) in classattrs.iteritems():
                if key.find('_') != 0:
                    parsers[key] = value

            classattrs['_parsers'] = parsers

            dataframe = pandas.read_csv(csv_filename, sep=sep, usecols=parsers.keys())

            for key in dataframe.keys():
                classattrs[key] = getattr(dataframe, key)

            classattrs['cols'] = dataframe.keys()
            for column_name in parsers.keys():
                if column_name in classattrs['cols']:
                    cleaned_values = []
                    parser = parsers[column_name][0]
                    for value in dataframe[column_name]:
                        cleaned_values.append(parser(value))
                    dataframe[column_name] = cleaned_values

            dataframe.index = dataframe[index_column]

            classattrs['_dataframe'] = dataframe

        return type.__new__(metaclass, classname, classparents, classattrs)

    def get_dataframe(self):
        return self._dataframe

    def get_from_db(self, csv_pk):
        df = self.get_dataframe()
        row = df.loc[csv_pk]
        Model = self.__model__

        if Model is None:
            raise Exception('__model__ is not set')

        if not self.__unique__:
            search_keys = self.__primary_key__
            object_dict = {self.__primary_key__: row['db_id']}
        else:
            search_keys = self.__unique__
            object_dict = make_dict(row, search_keys)

        return Model.query.filter(make_search_expression(Model, object_dict, search_keys)).first()

    def get_db_id(self, csv_pk):
        df = self.get_dataframe()
        row = df.loc[csv_pk]
        if 'db_id' not in row:
            raise Exception('Model not saved to database, no db_id present')
        else:
            return row['db_id']

    def save_to_db(self, session):
        Model = self.__model__
        force_create = False
        if not self.__unique__:
            clear_models(session, Model)
            force_create = True
        df = self.get_dataframe()
        csv_pk_column = df[self.__primary_key__]
        db_ids = []
        for csv_pk_id in csv_pk_column:
            row = df.loc[csv_pk_id]
            model_dict = exclude_keys(row, [self.__primary_key__])
            model_dict = make_dict(model_dict, model_dict.keys())
            model = get_or_create(session, Model, model_dict,
                                  self.__unique__, self._parsers,
                                  force_create=force_create)
            db_pk_id = getattr(model, self.__primary_key__)
            db_ids.append(db_pk_id)
        df['db_id'] = db_ids
        self._dataframe = df


class CSVBase(object):
    __metaclass__ = CSVBaseMeta
    __primary_key__ = None
    __unique__ = False
