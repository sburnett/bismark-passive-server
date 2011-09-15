from sqlalchemy import Column, create_engine, ForeignKey, MetaData
from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship, sessionmaker

Base = declarative_base()

class Session(Base):
    __tablename__ = 'sessions'
    id = Column(Integer, primary_key=True)
    signature = Column(String(100), unique=True)

class MacAddress(Base):
    __tablename__ = 'mac_addresses'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    data = Column(BigInteger)

    session = relationship(Session, backref='mac_addresses')

class IpAddress(Base):
    __tablename__ = 'ip_addresses'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    data = Column(Integer)

    session = relationship(Session, backref='ip_addresses')

class LocalAddress(Base):
    __tablename__ = 'local_addresses'
    id = Column(Integer, primary_key=True)
    mac_address_id = Column(Integer, ForeignKey('mac_addresses.id'))
    ip_address_id = Column(Integer, ForeignKey('ip_addresses.id'))

    mac_address = relationship(MacAddress, backref='local_addresses')
    ip_address = relationship(IpAddress, backref='local_addresses')

class DomainName(Base):
    __tablename__ = 'domain_names'
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    data = Column(String(256))

    session = relationship(Session, backref='domain_names')

class DnsMapping(Base):
    __tablename__ = 'dns_mappings'
    id = Column(Integer, primary_key=True)
    local_address_id = Column(Integer, ForeignKey('local_addresses.id'))
    domain_name_id = Column(Integer, ForeignKey('domain_names.id'))
    ip_address_id = Column(Integer, ForeignKey('ip_addresses.id'))

    local_address = relationship(LocalAddress, backref='dns_mappings')
    domain_name = relationship(DomainName, backref='dns_mappings')
    ip_address = relationship(IpAddress, backref='dns_mappings')

class Flow(Base):
    __tablename__ = 'flows'
    id = Column(Integer, primary_key=True)
    source_ip_id = Column(Integer, ForeignKey('ip_addresses.id'))
    destination_ip_id = Column(Integer, ForeignKey('ip_addresses.id'))
    transport = Column(Integer)
    source_port = Column(Integer)
    destination_port = Column(Integer)

    source_ip = relationship(IpAddress,
                             backref='source_flows',
                             primaryjoin=source_ip_id == IpAddress.id)
    destination_ip = relationship(IpAddress,
                                  backref='destination_flows',
                                  primaryjoin=destination_ip_id == IpAddress.id)

class Packet(Base):
    __tablename__ = 'packets'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    size = Column(Integer)
    flow_id = Column(Integer, ForeignKey('flows.id'))

    flow = relationship(Flow, backref='packets')

def initialize_database():
    engine = create_engine('sqlite:///:memory:', echo=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
