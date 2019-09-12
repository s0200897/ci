"""
Unit tests for the calculator library
"""

import calculator


def test_addition():
    assert 4 == calculator.add(2, 2)


def test_substraction():
    assert 2 == calculator.substract(4, 2)
