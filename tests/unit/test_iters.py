import pytest

from maggulake.utils.iters import (
    batched,
    limita_iterable_a_x_elementos,
    remove_duplicatas_sem_perder_ordem,
)


def test_batched_normal_case():
    assert list(batched('ABCDEFG', 3)) == [('A', 'B', 'C'), ('D', 'E', 'F'), ('G',)]


def test_batched_single_element_batches():
    assert list(batched('ABCDEFG', 1)) == [
        ('A',),
        ('B',),
        ('C',),
        ('D',),
        ('E',),
        ('F',),
        ('G',),
    ]


def test_batched_large_batch_size():
    assert list(batched('ABCDEFG', 10)) == [('A', 'B', 'C', 'D', 'E', 'F', 'G')]


def test_batched_invalid_batch_size():
    with pytest.raises(ValueError):
        list(batched('ABCDEFG', 0))


def test_batched_empty_iterable():
    assert not list(batched('', 3))


def test_remove_duplicatas_sem_perder_ordem():
    assert remove_duplicatas_sem_perder_ordem([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]) == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert remove_duplicatas_sem_perder_ordem([1, 2, 3, 4, 5]) == [1, 2, 3, 4, 5]
    assert remove_duplicatas_sem_perder_ordem([1, 2, 3, 4, 5, 5, 4, 3, 2, 1]) == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert not remove_duplicatas_sem_perder_ordem([])
    assert remove_duplicatas_sem_perder_ordem(['a', 'b', 'a', 'c', 'b']) == [
        'a',
        'b',
        'c',
    ]
    assert remove_duplicatas_sem_perder_ordem([1, 'a', 1, 'b', 'a']) == [1, 'a', 'b']


def test_limita_iterable_a_x_elementos():
    assert list(limita_iterable_a_x_elementos([1, 2, 3, 4, 5], 3)) == [1, 2, 3]
    assert list(limita_iterable_a_x_elementos([1, 2, 3, 4, 5], 10)) == [1, 2, 3, 4, 5]
    assert not list(limita_iterable_a_x_elementos([1, 2, 3, 4, 5], 0))
    assert not list(limita_iterable_a_x_elementos([], 10))
    assert list(limita_iterable_a_x_elementos("this_is_a_string", 5)) == [
        "t",
        "h",
        "i",
        "s",
        "_",
    ]
