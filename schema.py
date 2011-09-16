from sqlalchemy import Column, create_engine, ForeignKey, MetaData
from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship, sessionmaker
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()

class Node(Base):
    __tablename__ = 'nodes'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)

class AnonymizationContext(Base):
    __tablename__ = 'anonymization_contexts'
    __table_args__ = (UniqueConstraint('node_id', 'signature'), )

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'), nullable=False)
    signature = Column(String(100), unique=True, nullable=False)

    node = relationship(Node, backref='anonymization_contexts')

class Session(Base):
    __tablename__ = 'sessions'
    __table_args__ = (UniqueConstraint('anonymization_context_id', 'key'), )

    id = Column(Integer, primary_key=True)
    anonymization_context_id = Column(Integer,
                                      ForeignKey('anonymization_contexts.id'),
                                      nullable=False)
    key = Column(BigInteger, nullable=False)

    anonymization_context = relationship(AnonymizationContext,
                                         backref='sessions')

class Update(Base):
    __tablename__ = 'updates'
    __table_args__ = (UniqueConstraint('session_id', 'sequence_number'), )

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'), nullable=False)
    sequence_number = Column(Integer, nullable=False)
    pcap_received = Column(Integer)
    pcap_dropped = Column(Integer)
    iface_dropped = Column(Integer)

    session = relationship(Session, backref='updates')

class MacAddress(Base):
    __tablename__ = 'mac_addresses'
    __table_args__ = (UniqueConstraint('session_id', 'data'), )

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'), nullable=False)
    data = Column(BigInteger, nullable=False)

    session = relationship(Session, backref='mac_addresses')

class IpAddress(Base):
    __tablename__ = 'ip_addresses'
    __table_args__ = (UniqueConstraint('session_id', 'data'), )

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'), nullable=False)
    data = Column(Integer, nullable=False)

    session = relationship(Session, backref='ip_addresses')

class LocalAddress(Base):
    __tablename__ = 'local_addresses'
    __table_args__ = (UniqueConstraint('mac_address_id',
                                       'ip_address_id',
                                       'update_id'), )

    id = Column(Integer, primary_key=True)
    update_id = Column(Integer, ForeignKey('updates.id'), nullable=False)
    remote_address_id = Column(Integer, nullable=False)
    mac_address_id = Column(Integer, ForeignKey('mac_addresses.id'), nullable=False)
    ip_address_id = Column(Integer, ForeignKey('ip_addresses.id'), nullable=False)

    update = relationship(Update, backref='local_addresses')
    mac_address = relationship(MacAddress, backref='local_addresses')
    ip_address = relationship(IpAddress, backref='local_addresses')

class DomainName(Base):
    __tablename__ = 'domain_names'
    __table_args__ = (UniqueConstraint('session_id', 'data'), )

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'), nullable=False)
    data = Column(String(256), nullable=False)

    session = relationship(Session, backref='domain_names')

class DnsMapping(Base):
    __tablename__ = 'dns_mappings'
    __table_args__ = (UniqueConstraint('update_id',
                                       'local_address_id',
                                       'domain_name_id',
                                       'ip_address_id'), )

    id = Column(Integer, primary_key=True)
    update_id = Column(Integer, ForeignKey('updates.id'), nullable=False)
    local_address_id = Column(Integer, ForeignKey('local_addresses.id'), nullable=False)
    domain_name_id = Column(Integer, ForeignKey('domain_names.id'), nullable=False)
    ip_address_id = Column(Integer, ForeignKey('ip_addresses.id'), nullable=False)

    update = relationship(Update, backref='dns_mappings')
    local_address = relationship(LocalAddress, backref='dns_mappings')
    domain_name = relationship(DomainName, backref='dns_mappings')
    ip_address = relationship(IpAddress, backref='dns_mappings')

class Flow(Base):
    __tablename__ = 'flows'
    id = Column(Integer, primary_key=True)
    remote_flow_id = Column(Integer, nullable=False)
    update_id = Column(Integer, ForeignKey('updates.id'), nullable=False)
    source_ip_id = Column(Integer, ForeignKey('ip_addresses.id'), nullable=False)
    destination_ip_id = Column(Integer, ForeignKey('ip_addresses.id'), nullable=False)
    transport = Column(Integer, nullable=False)
    source_port = Column(Integer, nullable=False)
    destination_port = Column(Integer, nullable=False)

    update = relationship(Update, backref='flows')
    source_ip = relationship(IpAddress,
                             backref='source_flows',
                             primaryjoin=source_ip_id == IpAddress.id)
    destination_ip = relationship(IpAddress,
                                  backref='destination_flows',
                                  primaryjoin=destination_ip_id == IpAddress.id)

class Packet(Base):
    __tablename__ = 'packets'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    size = Column(Integer, nullable=False)
    flow_id = Column(Integer, ForeignKey('flows.id'), nullable=False)

    flow = relationship(Flow, backref='packets')

def initialize_database():
    engine = create_engine('sqlite:///test.sqlite')
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

def init_session():
    return initialize_database()()
