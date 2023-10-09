import json

import psycopg2

from config import config

def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None
    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")
    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db_name};")

    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при создании базы данных: {error}")

    finally:
        if conn is not None:
            conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    try:
        with open(script_file, 'r') as script:
            sql_script = script.read()

        cur.execute(sql_script)

    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при выполнении SQL-скрипта: {error}")




def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                contact VARCHAR(255),
                address VARCHAR(255),
                phone VARCHAR(20),
                fax VARCHAR(20),
                homepage VARCHAR(255)
            );
        """)

    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при создании таблицы suppliers: {error}")

def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    try:
        with open(json_file, 'r', encoding='utf-8') as file:
            suppliers_data = json.load(file)

        return suppliers_data

    except (FileNotFoundError, json.JSONDecodeError) as error:
        print(f"Ошибка при чтении JSON-файла: {error}")
        return []

def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    try:
        for supplier in suppliers:
            cur.execute("""
                INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING supplier_id;
            """, (
                supplier.get("company_name", ""),
                supplier.get("contact", ""),
                supplier.get("address", ""),
                supplier.get("phone", ""),
                supplier.get("fax", ""),
                supplier.get("homepage", "")
            ))

            supplier_id = cur.fetchone()[0]

    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при добавлении данных в таблицу suppliers: {error}")



def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    try:
        cur.execute("""
            ALTER TABLE products
            ADD COLUMN supplier_id INTEGER REFERENCES suppliers(supplier_id);
        """)

        # Создаем индекс на столбец product_name в таблице products
        cur.execute("""
            CREATE INDEX products_product_name_idx ON products(product_name);
        """)

        # Обновляем supplier_id в products на основе product_name
        cur.execute("""
            UPDATE products
            SET supplier_id = suppliers.supplier_id
            FROM suppliers
            WHERE products.product_name = suppliers.company_name;
        """)
    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при добавлении foreign key в таблицу products: {error}")




if __name__ == '__main__':
    main()
