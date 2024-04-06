import requests
import psycopg2
from config import config


class HH_api_db:
    """Класс для работы с API HH.ru и заполнение таблиц в базу данных"""

    employers_dict = {'Яндекс': '1740',
                      'SkyEng': '1122462',
                      'Softline': '2381',
                      'СБЕР': '3529',
                      'Aviasales.ru': '588914',
                      'Тинькофф': '78638',
                      'VK': '15478',
                      '2ГИС': '64174',
                      'Wildberries': '87021',
                      'Ozon': '2180'}

    @staticmethod
    def get_request(employer_id) -> dict:
        """Запрос списка вакансий по работодателям из списка"""
        params = {
            "page": 1,
            "per_page": 100,
            "employer_id": employer_id,
            "only_with_salary": True,
            "only_with_vacancies": True
        }
        return requests.get("https://api.hh.ru/vacancies/", params=params).json()['items']

    def get_vacancies(self):
        """Получение списка вакансий от работодателей"""
        vacancies_list = []
        for employer in self.employers_dict:
            emp_vacancies = self.get_request(self.employers_dict[employer])
            for vacancy in emp_vacancies:
                if vacancy['salary']['from'] is None:
                    salary = 0
                else:
                    salary = vacancy['salary']['from']
                vacancies_list.append(
                    {'url': vacancy['alternate_url'], 'salary': salary,
                     'vacancy_name': vacancy['name'], 'employer': employer})
        return vacancies_list


class DBManager:
    def __init__(self):
        self.params_db = config()

    def create_database(self, database_name):
        """Создание базы данных."""

        conn = psycopg2.connect(dbname='postgres', **self.params_db)
        conn.autocommit = True

        cur = conn.cursor()

        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')

        cur.close()
        conn.close()

    def create_tables(self, database_name):
        """Создание таблиц companies и vacancies в созданной базе данных"""

        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE companies (
                    company_id SERIAL PRIMARY KEY,
                    company_name VARCHAR UNIQUE
                    )
                    """)

                cur.execute("""
                        CREATE TABLE vacancies (
                        vacancy_id serial,
                        vacancy_name text not null,
                        salary int,
                        company_name text REFERENCES companies(company_name) NOT NULL,
                        vacancy_url varchar not null,
                        foreign key(company_name) references companies(company_name)
                        )
                        """)

        conn.close()

    def save_info_to_database(self, database_name, employers_dict, vacancies_list):
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                for employer in employers_dict:
                    cur.execute(
                        f"INSERT INTO companies(company_name) values ('{employer}')")
                for vacancy in vacancies_list:
                    cur.execute(
                        f"INSERT INTO vacancies(vacancy_name, salary, company_name, vacancy_url) values "
                        f"('{vacancy['vacancy_name']}', '{int(vacancy['salary'])}', "
                        f"'{vacancy['employer']}', '{vacancy['url']}')")

        conn.close()

    def get_companies_and_vacancies_count(self, database_name):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT company_name, COUNT(vacancy_name) FROM vacancies GROUP BY company_name')
                answer = cur.fetchall()
        conn.close()
        return answer

    def get_all_vacancies(self, database_name):
        """Получает список всех вакансий"""
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM vacancies')
                answer = cur.fetchall()
        conn.close()
        return answer

    def get_avg_salary(self, database_name):
        """Получает среднюю зарплату по вакансиям"""
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT AVG(salary) FROM vacancies')
                answer = cur.fetchall()
        conn.close()
        return answer

    def get_vacancies_with_higher_salary(self, database_name):
        """Получает список всех вакансий, у которых зарплата выше средней"""
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT vacancy_name FROM vacancies WHERE salary > (SELECT AVG(salary) FROM vacancies)')
                answer = cur.fetchall()
        conn.close()
        return answer

    def get_vacancies_with_keyword(self, database_name, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with psycopg2.connect(dbname=database_name, **self.params_db) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT vacancy_name FROM vacancies WHERE vacancy_name LIKE '%{keyword}%'")
                answer = cur.fetchall()
        conn.close()
        return answer
