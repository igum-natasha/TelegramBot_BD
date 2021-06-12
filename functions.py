from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, text, create_engine, inspect, insert
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, column
from sqlalchemy.orm import mapper


class Books:
    def __init__(self, id_book, name_book, cost, address, count):
        self.id_books = id_book
        self.name_book = name_book
        self.cost = cost
        self.address = address
        self.count = count

    def __repr__(self):
        return self.id_books


class Buy:
    def __init__(self, id_buy, date, id_shop, id_customer, id_book, count, cost):
        self.id_buy = id_buy
        self.date = date
        self.id_shop = id_shop
        self.id_customer = id_customer
        self.id_book = id_book
        self.count = count
        self.cost = cost

    def __repr__(self):
        return self.id_buy


class Customer:
    def __init__(self, id_customer, lastname, address, sale):
        self.id_customer = id_customer
        self.lastname = lastname
        self.address = address
        self.sale = sale

    def __repr__(self):
        return self.id_customer


class Shop:
    def __init__(self, id_shop, name_shop, address, percent):
        self.id_shop = id_shop
        self.name_shop = name_shop
        self.address = address
        self.percenr = percent

    def __repr__(self):
        return self.id_shop



def create_db(name, login, passw):
    engine = create_engine('postgresql://{}:{}@localhost/{}'.format(login, passw, name))
    if not database_exists(engine.url):
        create_database(engine.url)
        return 0
    return 1


def full_clear(metadata, engine, tables):
    for table in tables:
        del_stmt = Table(table, metadata).delete()
        engine.execute(del_stmt)


def clear_one_table(metadata, engine, table):
    del_stmt = Table(table, metadata).delete()
    engine.execute(del_stmt)


def find_value_bd(engine, table, field, value):
    s = select(['*']).select_from(table).where(column(field) == value)
    print(s)
    return engine.execute(s)


def DeleteID(name_table, name_column, value, metadata, session):
    for table in metadata.sorted_tables:
        if table.name == name_table:
            if list(inspect(table).primary_key)[0].name != name_column:
                return "Ошибка!!! Удаление не по ключевому полю"
            session.query(table).filter(text('{} = {}'.format(name_column, value))).delete()
            session.commit()
    return "OK"


def OutputDB(metadata, session):
    message = dict()
    for table in metadata.sorted_tables:
        queryy = session.query(table)
        message[table.name] = []
        for query in queryy:
            message[table.name].append(query)
    return message


def AddClassTeble(Class, name_table, count, metadata, newRecord):
    for i in range(count - len(newRecord)):
        newRecord.append(None)
    print(newRecord)
    mapper(Class, Table(name_table, metadata, autoload=True))
    return Class(*newRecord)


def DeleteALL(metadata, engine):
    Base = declarative_base()
    Base.metadata.drop_all(bind=engine, tables=metadata.sorted_tables)
    return "OK"


def AddNewRecord(name_table, record, session, metadata):
    newRecord = record.split(';')
    print(newRecord)
    recordDB = ''
    if name_table == 'books':
        recordDB = AddClassTeble(Books, name_table, 5, metadata, newRecord)
    elif name_table == "buy":
        recordDB = AddClassTeble(Buy, name_table, 7, metadata, newRecord)
    elif name_table == "customer":
        recordDB = AddClassTeble(Customer, name_table, 4, metadata, newRecord)
    elif name_table == "shop":
        recordDB = AddClassTeble(Shop, name_table, 4, metadata, newRecord)
    session.add(recordDB)
    session.commit()


def DeleteParam(name_table, name_column, value, metadata, session):
    for table in metadata.sorted_tables:
        if table.name == name_table:
            if list(inspect(table).primary_key)[0].name == name_column:
                return "Ошибка!!! Удаление по ключевому полю"
            session.query(table).filter(text('{} = {}'.format(name_column, value))).delete()
            session.commit()
    return "OK"
