from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, insert, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Establish a connection to the PostgreSQL database
engine = create_engine('postgresql://finapp_user:12345@localhost:5432/FinanceDB')
Session = sessionmaker(bind=engine)
session = Session()

# Define the table structure
metadata = MetaData()

Base = declarative_base()
class TABLE_MODEL_EQUITY_LIST(Base):
    __tablename__ = 'equity_list'
    __table_args__ = {'schema': 'sm'}

    isin_number = Column('isin_number', String, primary_key=True)
    bse = Column('bse', String)
    security_code_bse = Column(String)
    nse = Column('nse', String)
    security_code_nse = Column(String)

    issuer_name = Column(String)
    security_id = Column('security_id', String)
    security_name = Column('security_name', Integer)
    status = Column('status', String)
    security_group = Column('security_group', Integer)
    face_value = Column('face_value', Integer)
    industry = Column('industry', Integer)
    market_capitalisation_in_crore = Column('market_capitalisation_in_crore', String)
    audit_create_date = Column('audit_create_date', String)
    audit_update_date = Column('audit_update_date', String)

    def __repr__(self):
        return f"<YourTable(id={self.id}, security_code='{self.security_code}', column_name='{self.column_name}')>"

class TABLE_MODEL_AUDIT_EQUITY_HISTORICAL_LOAD_STATUS(Base):
    __tablename__ = 'audit_equity_historical_load_status'
    __table_args__ = {'schema': 'sm'}

    exchange = Column(String)
    isin_number = Column(String, primary_key=True)

class TABLE_MODEL_EQUITY_HISTORICAL_DATA(Base):
    __tablename__ = 'equity_historical_data'
    __table_args__ = {'schema': 'sm'}

    exchange = Column(String)
    trade_date = Column(String)
    isin_number = Column(String, primary_key=True)
    security_name = Column(String)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)
    open_interest = Column(Integer)
    net_change = Column(Integer)
    total_buy_quantity = Column(Integer)
    lower_circuit_limit = Column(Integer)
    upper_circuit_limit = Column(Integer)
    total_sell_quantity = Column(Integer)
    average_price = Column(Integer)
    audit_creation_date = Column(String)


    def __repr__(self):
        return f"<YourTable(id={self.id}, security_code='{self.security_code}', column_name='{self.column_name}')>"


class TABLE_MODEL_EQUITY_MARKET_AS_ON_DATE_DATA(Base):
    __tablename__ = 'equity_market_as_on_date_data'
    __table_args__ = {'schema': 'sm'}

    security_code = Column(String, primary_key=True)
    as_on_date = Column(String)
    previous_close = Column(Integer)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    last_traded_price_ltp = Column(Integer)
    volume_weighted_average_price_vwap = Column(Integer)

    fifty2wkhigh_adj = Column(Integer)
    fifty2wkhigh_adjdt = Column(String)
    fifty2wklow_adj = Column(Integer)
    fifty2wklow_adjdt = Column(String)
    fifty2wkhigh_unadj = Column(Integer)
    fifty2wkhigh_unadjdt = Column(String)
    fifty2wklow_unadj = Column(Integer)
    fifty2wklow_unadjdt = Column(String)
    monthhighlow = Column(String)
    weekhighlow = Column(String)

    weighted_average_price_wap = Column(Integer)
    mktcapfull = Column(Integer)
    mktcapff = Column(Integer)
    turnoverin = Column(String)
    turnover = Column(Integer)

    faceval = Column(Integer)
    earnings_per_share_eps = Column(Integer)
    cash_earnings_per_share_ceps = Column(Integer)
    price_to_earnings_ratio_pe = Column(Integer)
    price_to_book_ratio_pb = Column(Integer)
    return_on_equity_ratio_roe = Column(Integer)
    setltype = Column(String)
    index = Column(String)

    audit_create_date = Column(String)


    def __repr__(self):
        return f"<YourTable(id={self.id}, security_code='{self.security_code}', column_name='{self.column_name}')>"
