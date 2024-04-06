from classes import DBManager, HH_api_db


def main():
    response = HH_api_db()

    employers_dict = response.employers_dict
    employers_all_vacancies = response.get_vacancies()

    # Create database
    database = DBManager()
    database.create_database('vacancies')

    # Create tables
    database.create_tables('vacancies')

    # Save info to database
    database.save_info_to_database('vacancies', employers_dict, employers_all_vacancies)

    # User interface
    while True:
        command = input('''
1 - Вывести список компаний и количество вакансий
2 - Вывести список всех вакансий с указанием названия компании
3 - Получить среднюю з/п по вакансиям
4 - Вывести список вакансий с з/п выше средней
5 - Найти вакансии по ключевому слову
0 - Завершение работы программы
Введите номер команды: ''')
        if command == '0':
            break
        elif command == '1':
            print("\nСписок компаний и количество вакансий в компаниях:")
            for row in database.get_companies_and_vacancies_count('vacancies'):
                print(f"{row[0]} - {row[1]}")
        elif command == '2':
            print("\nСписок всех вакансий с указанием названия компании:")
            for row in database.get_all_vacancies('vacancies'):
                print(f"{row[0]} - {row[1]}")
        elif command == '3':
            print("\nСредняя зарплата по вакансиям:")
            for row in database.get_avg_salary('vacancies'):
                print(f"{round(row[0])} руб.")
        elif command == '4':
            print("\nСписок всех вакансий, у которых зарплата выше средней по всем вакансиям:")
            for row in database.get_vacancies_with_higher_salary('vacancies'):
                print(f"{row[0]}")
        elif command == '5':
            keyword = input('Введите ключевое слово: ')
            try:
                print("\nСписок всех вакансий, в названии которых содержатся переданные в метод слова:")
                for row in database.get_vacancies_with_keyword('vacancies', keyword):
                    print(f"{row[0]} ")
            except Exception as e:
                print('Произошла ошибка. Попробуйте другое ключевое слово')
        else:
            print('Введите корректную команду\n')


if __name__ == '__main__':
    main()
