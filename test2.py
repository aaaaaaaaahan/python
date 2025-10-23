TypeError: unsupported operand type(s) for |: 'type' and '_GenericAlias'

from typing import List, Union

def get_hive_parquet(dataset_name: str, generations: Union[int, List[int], str] = 1, debug: bool = False) -> List[str]:
