'''
Ручной запуск на реальных данных
uv run python src/elt/run.py [yyyy-mm-dd]
sqlite3 dev_data.db "SELECT ..."
'''
import logging
import sys
from datetime import date, datetime

from .loader import count_records, load_date

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def main():
    if len(sys.argv) > 1:
        target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target_date = date.today()

    print(f'____Загрузка курсов за {target_date}____')

    result = load_date(target_date)
    print(f'Результат: {result}, всего записей {count_records()}')


if __name__ == '__main__':
    main()
