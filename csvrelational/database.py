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
