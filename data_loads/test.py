from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Establish a connection to the PostgreSQL database
engine = create_engine('postgresql://finapp_user:12345@localhost:5432/FinanceDB')

Session = sessionmaker(bind=engine)
session = Session()

print(f'engine = {engine}')