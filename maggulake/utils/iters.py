from itertools import islice
from typing import Generator


def batched(iterable, n: int) -> Generator:
    """Divide um iterable em batches de tamanho n."""
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


def create_batches(iterable, n: int):
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


def remove_duplicatas_sem_perder_ordem(lista: list) -> list:
    """De vez em quando pode ser importante não perder a ordem dos elementos da
    lista, o que geralmente acontece quando usamos set() para remover duplicatas.
    """
    return list(dict.fromkeys(lista))


def limita_iterable_a_x_elementos(iterable, x):
    """Limita o iterable a x elementos"""
    return iterable[: min(x, len(iterable))]
