from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
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
# TABLE_EQUITY_LIST = Table('EQUITY_LIST', metadata, schema='sm',
#                         Column('security_code', Integer, primary_key=True),
#                         Column('issuer_name', String),
#                         Column('security_id', String),
#                         Column('security_name', Integer),
#                         Column('status', String),
#                         Column('security_group', Integer),
#                         Column('face_value', Integer),
#                         Column('isin_no', String),
#                         Column('industry', Integer),
#                         Column('sector_name', Integer),
#                         Column('industry_new_name', String),
#                         Column('igroup_name', Integer),
#                         Column('audit_create_date', Integer),
#                         Column('audit_update_date', String)
#                     )


# Create a table object with the specified schema and table name
#TABLE_EQUITY_LIST = Table('equity_list', metadata, schema='sm', autoload_with=engine)


Base = declarative_base()
class TABLE_MODEL_EQUITY_LIST(Base):
    __tablename__ = 'equity_list'
    __table_args__ = {'schema': 'sm'}

    security_code = Column(String, primary_key=True)
    issuer_name = Column(String)
    security_id = Column('security_id', String)
    security_name = Column('security_name', Integer)
    status = Column('status', String)
    security_group = Column('security_group', Integer)
    face_value = Column('face_value', Integer)
    isin_no = Column('isin_no', String)
    industry = Column('industry', Integer)
    sector_name = Column('sector_name', Integer)
    industry_new_name = Column('industry_new_name', String)
    igroup_name = Column('igroup_name', Integer)
    audit_create_date = Column('audit_create_date', Integer)
    audit_update_date = Column('audit_update_date', String)

    def __repr__(self):
        return f"<YourTable(id={self.id}, security_code='{self.security_code}', column_name='{self.column_name}')>"


class TABLE_MODEL_EQUITY_MARKET_HISTORICAL_DATA(Base):
    __tablename__ = 'equity_market_historical_data'
    __table_args__ = {'schema': 'sm'}

    security_code = Column(String, primary_key=True)
    trade_date = Column(String)
    previous_close = Column(Integer)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    weighted_average_price_wap = Column(Integer)
    number_of_shares = Column(Integer)
    number_of_trades = Column(Integer)
    total_turnover = Column(Integer)
    deliverable_quantity = Column(Integer)
    percentage_deliverable_quantity_to_traded_quantity = Column(Integer)
    spread_high_low = Column(Integer)
    spread_close_open = Column(Integer)
    audit_create_date = Column(String)


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
