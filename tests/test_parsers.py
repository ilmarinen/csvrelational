import numpy as np
from csvrelational import parsers


def test_make_boolean():
    assert parsers.make_boolean(True) == True
    assert parsers.make_boolean(False) == False

    assert parsers.make_boolean('True') == True
    assert parsers.make_boolean('False') == False

    assert parsers.make_boolean('y') == True
    assert parsers.make_boolean('Y') == True
    assert parsers.make_boolean('n') == False
    assert parsers.make_boolean('N') == False

    assert parsers.make_boolean(np.bool(True)) == True
    assert parsers.make_boolean(np.bool(False)) == False


def test_parse_int():
    assert parsers.parse_int('1') == 1
    assert parsers.parse_int('10') == 10
    assert parsers.parse_int(10) == 10
