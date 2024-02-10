from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

try:
    engine = create_engine('postgresql://finapp_user:12345@localhost:5432/FinanceDB')
    print('Connection to the database established successfully.')
except SQLAlchemyError as e:
    print('Failed to connect to the database:', str(e))