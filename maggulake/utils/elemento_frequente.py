from typing import Any, List


def most_frequent(x: List[Any]) -> Any:
    if len(x) == 0:
        return 'nao_informado'
    else:
        return max(set(x), key=x.count)
