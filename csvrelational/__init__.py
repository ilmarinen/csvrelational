import pandas
import numpy as np
import sqlalchemy


def make_search_expression(Model, model_dict, search_keys):
    search_expressions = []
    for (key, value) in model_dict.iteritems():
        if key in search_keys:
            search_expression = (getattr(Model, key) == value)
            search_expressions.append(search_expression)

    return sqlalchemy._and(*search_expressions)


def make_dict(raw_dict, keys):
    new_dict = {}
    for key in keys:
        if key in raw_dict:
            value = raw_dict[key]
            if isinstance(value, np.bool_):
                if value:
                    value = True
                else:
                    value = False
            if value is not None:
                new_dict[key] = value

    return new_dict


def exclude_keys(dict_obj, keys=[]):
    for key in keys:
        if key in dict_obj:
            del(dict_obj[key])
    return dict_obj


def set_attributes(model, model_dict):
    for (key, value) in model_dict.iteritems():
        setattr(model, key, value)

    return model


def get_or_create(session, Model, object_dict, unique_keys, parsers, force_create=False):
    parents = []
    new_object_dict = {}
    for (key, value) in object_dict.iteritems():
        parser_tuple = parsers[key]
        if len(parser_tuple) > 1:
            ParentCSVModel = parser_tuple[1]
            backref = parser_tuple[2]
            parent_csv_pk_id = value
            parent_db_id = ParentCSVModel.get_db_id(parent_csv_pk_id)
            new_object_dict[key] = parent_db_id

            if backref is not None:
                parent = ParentCSVModel.get_from_db(parent_csv_pk_id)
                parents.append((parent, backref))
        else:
            new_object_dict[key] = value

    if force_create:
        model = Model(**new_object_dict)
    else:
        search_expression = make_search_expression(Model, new_object_dict, unique_keys)
        model = Model.query.filter(search_expression).first()
        if not model:
            model = Model(**new_object_dict)
        else:
            set_attributes(model, new_object_dict)

    session.add(model)

    for (parent, backref) in parents:
        backref_obj = getattr(parent, backref)
        if isinstance(backref_obj, list):
            backref_obj.append(model)
        elif backref_obj is None:
            backref_obj = model

        setattr(parent, backref, backref_obj)
        session.add(parent)

    session.flush()

    return model


def clear_models(session, Model):
    for model in Model.query.all():
        session.delete(model)
        session.flush()


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
