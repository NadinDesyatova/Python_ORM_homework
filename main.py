import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Book, Shop, Stock, Sale


def read_data_file(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    return data    

def insert_data(session, data):
    for item in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[item.get('model')]
        session.add(model(id=item.get('pk'), **item.get('fields')))
    session.commit()

def get_book_sales_information(session, current_publisher):
    query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).join(Shop).join(Sale)
    if current_publisher.isdigit():
        query = query.filter(Publisher.id == current_publisher).all()
    else:
        query = query.filter(Publisher.name == current_publisher).all()

    for title, name, price, date_sale in query:
        print(f"{title:<40} | {name:<10} | {price:<8} | {date_sale}")


if __name__ == '__main__':
    connection_driver = 'postgresql'
    user='postgres'
    password=''
    server_name = 'localhost'
    server_port = '5432'
    db_name = 'book_sales_db'

    DSN = f"{connection_driver}://{user}:{password}@{server_name}:{server_port}/{db_name}"
    engine = sqlalchemy.create_engine(DSN)

    Session = sessionmaker(bind=engine)

    create_tables(engine)

    file = "tests_data.json"
    data_to_insert = read_data_file(file)

    current_publisher = input("Введите имя или идентификатор издателя: ")
    
    current_session = Session()
    
    insert_data(current_session, data_to_insert)

    get_book_sales_information(current_session, current_publisher)

    current_session.close()
