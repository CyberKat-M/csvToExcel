import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

def initialize_database(database_uri):
    engine = create_engine(database_uri)
    metadata = MetaData()
    
    clients = Table('clients', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String, unique=True, nullable=False)
    )
    
    assets = Table('assets', metadata,
        Column('id', Integer, primary_key=True),
        Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
        Column('upload_date', DateTime, default=datetime.utcnow),
        Column('HostID', String),
        Column('AssetID', String),
        Column('AssetName', String)
    )
    
    scans = Table('scans', metadata,
        Column('id', Integer, primary_key=True),
        Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
        Column('upload_date', DateTime, default=datetime.utcnow),
        Column('IP', String),
        Column('Network', String),
        Column('DNS', String)
    )
    
    tickets = Table('tickets', metadata,
        Column('id', Integer, primary_key=True),
        Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
        Column('upload_date', DateTime, default=datetime.utcnow),
        Column('TicketID', String),
        Column('AutoTaskTicketNumber', String)
    )
    
    metadata.create_all(engine)

def get_clients(database_uri):
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    result = session.execute(text("SELECT id, name FROM clients")).fetchall()
    session.close()
    return result

def add_client(name, database_uri):
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    result = session.execute(text("INSERT INTO clients (name) VALUES (:name) RETURNING id"), {"name": name}).fetchone()
    session.commit()
    session.close()
    return result[0]

def store_dataframe(table_name, dataframe, client_id, database_uri):
    engine = create_engine(database_uri)
    dataframe['client_id'] = client_id
    dataframe['upload_date'] = datetime.utcnow()
    dataframe.to_sql(table_name, engine, index=False, if_exists='append')
