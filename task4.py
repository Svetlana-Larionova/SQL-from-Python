import pandas as pd
import psycopg2


def main_task():
    """
    Задание 4: Подключение к PostgreSQL и выполнение SQL запроса
    """
    print("Задание 4: Подключение к PostgreSQL")
    print("=" * 40)

    try:
        print("1. Подключаемся к базе 'obucheniye'...")

        connection = psycopg2.connect(
            host='localhost',
            port=5432,
            database='obucheniye',
            user='postgres',
            password='12345'
        )

        print("2. Подключение успешно!")

        # SQL запрос из задания 2
        query = """
        WITH ProductsWithCategories AS (
            SELECT 
                p."ProductID",
                p."ProductName", 
                p."UnitPrice",
                p."SupplierID",
                c."CategoryName"
            FROM products p
            LEFT JOIN categories c ON p."CategoryID" = c."CategoryID"
            WHERE c."CategoryName" LIKE 'C%'
        )
        SELECT 
            pwc.*,
            s."CompanyName" AS SupplierName,
            AVG(pwc."UnitPrice") OVER (PARTITION BY pwc."SupplierID") AS AvgPricePerSupplier
        FROM ProductsWithCategories pwc
        LEFT JOIN suppliers s ON pwc."SupplierID" = s."SupplierID";
        """

        print("3. Выполняем SQL запрос...")

        df = pd.read_sql_query(query, connection)

        print(f"4. Успешно! Получено {len(df)} строк")

        # Показываем результат
        print("\n" + "=" * 50)
        print("Результат запроса:")
        print("=" * 50)
        print(df)

        print("\nИнформация о данных:")
        print(f"Колонки: {list(df.columns)}")
        print(f"Размер данных: {df.shape}")

        # Сводная информация
        print("\nСводная информация:")
        print(f"Уникальных поставщиков: {df['suppliername'].nunique()}")
        print(f"Уникальных категорий: {df['CategoryName'].nunique()}")
        print(f"Средняя цена товаров: {df['UnitPrice'].mean():.2f}")

        # Средние цены по поставщикам
        print("\nСредние цены по поставщикам:")
        avg_prices = df[['suppliername', 'avgpricepersupplier']].drop_duplicates()
        for _, row in avg_prices.iterrows():
            print(f"   {row['suppliername']}: {row['avgpricepersupplier']:.2f}")

        # Сохраняем в CSV
        csv_filename = 'task4_result.csv'
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"\nДанные сохранены в '{csv_filename}'")

        # Закрываем подключение
        connection.close()
        print("Подключение закрыто")

        return df

    except Exception as e:
        print(f"Ошибка: {e}")
        return None


def display_summary(df):
    """
    Краткий отчет о выполнении задания
    """
    print("\n" + "=" * 50)
    print("ОТЧЕТ О ВЫПОЛНЕНИИ ЗАДАНИЯ 4")
    print("=" * 50)

    print("Выполненные задачи:")
    print("• Установлено подключение к PostgreSQL")
    print("• Выполнен SQL запрос из задания 2")
    print("• Данные загружены в pandas DataFrame")
    print("• Результат сохранен в CSV файл")

    print(f"\nРезультаты:")
    print(f"• Обработано товаров: {len(df)}")
    print(f"• Категории: {list(df['CategoryName'].unique())}")
    print(f"• Поставщиков: {df['suppliername'].nunique()}")
    print(f"• Средняя цена: {df['UnitPrice'].mean():.2f}")
    print(f"• Файл с результатом: task4_result.csv")

    print("\nЗадание 4 выполнено успешно!")


if __name__ == "__main__":
    # Выполняем основной запрос
    result_df = main_task()

    if result_df is not None:
        display_summary(result_df)