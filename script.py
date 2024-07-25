import click
import logging
import pandas as pd


def description(row: pd.Series) -> str:
    """
    Генерирует строку описания на основе данных из строки DataFrame.

    Args:
        row (pd.Series): Строка данных из DataFrame, содержащая столбцы 'CARE_INDEX', 'PLAN_INDEX' и 'PREV_INDEX'.

    Returns:
        str: Строка с описанием с информацией об индексе клиентского менеджера.
    """
    compare_with: str
    if row.CARE_INDEX < row.PLAN_INDEX:
        compare_with = 'меньше'
    else:
        compare_with = 'больше'

    growth: str
    if row.CARE_INDEX < row.PREV_INDEX:
        growth = 'снижение'
    else:
        growth = 'рост'

    return (f'У клиентского менеджера индекс {row.CARE_INDEX:.2f} '
            f'{compare_with} среднего по системе ({row.PLAN_INDEX:.2f}) и '
            f'наблюдается {growth} показателя по сравнению с предыдущим '
            f'периодом ({row.PREV_INDEX:.2f}).')


def calc_manager_index(filepath: str) -> pd.DataFrame:
    """
    Рассчитывает показатели клиентских менеджеров на основе данных из файла.

    Args:
        filepath (str): Путь к файлу Excel с данными.

    Returns:
        pd.DataFrame: Итоговый DataFrame с рассчитанными показателями.
    """
    logging.debug(f'Рассчитываем показатели из файла {filepath}')
    clients: pd.DataFrame = pd.read_excel(filepath, sheet_name='data',
                                          skiprows=1)

    relations: pd.DataFrame = pd.read_excel(filepath, sheet_name='clients',
                                            skiprows=1)

    client_results: pd.DataFrame = pd.merge(relations, clients,
                                            on='CLIENT_ID')
    client_results['CARE_INDEX'] = (client_results.INDEX_1 +
                                    client_results.INDEX_2 +
                                    client_results.INDEX_3)

    manager_results: pd.DataFrame = \
        client_results.groupby('MANAGER_ID') \
                      .agg(CLIENTS=('CARE_INDEX', 'count'),
                           CARE_INDEX=('CARE_INDEX', 'mean'))

    managers: pd.DataFrame = pd.read_excel(filepath, sheet_name='prev_data',
                                           skiprows=1)

    final_df: pd.DataFrame = pd.merge(managers, manager_results,
                                      left_on='USER_ID', right_on='MANAGER_ID')
    final_df = final_df[['USER_ID', 'USER_FIO', 'CLIENTS', 'PREV_INDEX',
                         'CARE_INDEX']]
    plan_index: float = final_df.CARE_INDEX.mean()
    final_df['PLAN_INDEX'] = plan_index
    final_df['DESCRIPTION'] = final_df.apply(description, axis=1)
    logging.debug(f'Результат: {final_df.columns} {final_df.shape}')
    return final_df


@click.command()
@click.argument('filepath')
@click.option('-v', default='INFO', help='Уровень логгирования (verbosity)')
def main(filepath: str, v: str) -> None:
    logging.basicConfig(
        format='%(levelname)s:%(filename)s:[%(asctime)s] %(message)s',
        level=getattr(logging, v.upper()),
    )
    df: pd.DataFrame = calc_manager_index(filepath)
    print(df)


if __name__ == '__main__':
    main()
