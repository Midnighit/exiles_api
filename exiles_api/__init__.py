import os, json, warnings
from config import *
from math import floor, ceil, sqrt
from struct import pack, unpack
from datetime import datetime
from sqlalchemy import create_engine, literal, desc, MetaData, exc as sa_exc
from sqlalchemy.orm import sessionmaker, Session, relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, func, distinct, Text, Integer, String, Float, DateTime, Boolean

GameBase = declarative_base()
UsersBase = declarative_base()

engines = {"gamedb": create_engine(GAME_DB_URI, echo=ECHO), "usersdb": create_engine(USERS_DB_URI, echo=ECHO)}
metadata = MetaData(bind=engines["gamedb"])

# override Session.get_bind
class RoutingSession(Session):
    def get_bind(self, mapper=None, clause=None):
        if mapper and issubclass(mapper.class_, GameBase):
            return engines["gamedb"]
        else:
            return engines["usersdb"]

Session = sessionmaker(class_=RoutingSession)
session = Session()
GameBase.metadata = metadata

RANKS = (
    'Recruit',
    'Member',
    'Officer',
    'Guildmaster'
)

def db_date():
    now = datetime.utcnow()
    for c in session.query(Characters).order_by(desc(Characters._last_login)).all():
        if c.last_login < now:
            return c.last_login
    return None

# non-db classes
class ChatLogs:
    files = []

    def __init__(self, path, after_date=None):
        self.path = path
        filelist = os.listdir(path)
        filelist.sort()
        for filename in filelist:
            if filename.startswith('ConanSandbox'):
                if filename.startswith('ConanSandbox-backup'):
                    date = datetime.strptime(filename[20:39], '%Y.%m.%d-%H.%M.%S')
                    if not after_date or date > after_date:
                        self.files.append({'name': filename, 'date': date})
                else:
                    self.files.insert(0, {'name': filename, 'date': None})

    def lines(self, index):
        if type(index) is int and index <= len(self.files) - 1:
            filename = self.files[index]['name']
        elif type(index) is str and index.startswith('ConanSandbox'):
            for i in range(len(self.files)):
                if files[i]['name'] == index:
                    filename = index
                    index = i
                    break
        else:
            print("file not found.")
            return False
        file = os.path.join(self.path, filename)
        with open(file, 'r') as f:
            lines = f.readlines()
        self.files[index]['lines'] = lines
        return True

    def chat_lines(self, file=None):
        pass

class Owner:
    @staticmethod
    def exists(owner_id):
        ids = session.query(Characters.id).filter_by(id=owner_id).union(
              session.query(Guilds.id).filter_by(id=owner_id)).all()
        if len(ids) > 0:
            return True
        return False

    @staticmethod
    def get(owner_id):
        owner = session.query(Guilds).get(owner_id)
        if not owner:
            owner = session.query(Characters).get(owner_id)
        return owner

    @property
    def buildings(self):
        return session.query(Buildings).filter_by(owner_id=self.id).all()

    def is_inactive(self, td):
        if self.last_login:
            return self.last_login < datetime.utcnow() - td
        else:
            return True

    def has_tiles(self):
        return True if session.query(Buildings.object_id).filter_by(owner_id=self.id).first() else False

    def tiles(self, bMult=1, pMult=1):
        if not self.has_tiles():
            return tuple()
        bTiles = tuple(
            BuildingTiles(owner_id=self.id,
                          object_id=res[0],
                          amount=res[1]*bMult)
            for res in session.query(Buildings.object_id, func.count(Buildings.object_id))
                              .filter(Buildings.owner_id==self.id, Buildings.object_id==BuildingInstances.object_id)
                              .group_by(Buildings.object_id).all())
        root = tuple(res[0] for res in session.query(distinct(Buildings.object_id))
                          .filter(Buildings.owner_id==self.id, Buildings.object_id==BuildingInstances.object_id).all())
        pTiles = tuple(
            Placeables(owner_id=self.id,
                       object_id=res[0],
                       amount=pMult)
            for res in session.query(Buildings.object_id)
                              .filter(Buildings.owner_id==self.id, Buildings.object_id.notin_(root)).all())
        return bTiles + pTiles

    def num_tiles(self, bMult=1, pMult=1, r=True):
        tiles = self.tiles(bMult, pMult)
        sum = 0
        for t in tiles:
            sum += t.amount
        return int(round(sum, 0)) if r else sum

class Tiles:
    def __init__(self, *args, **kwargs):
        self.owner_id = kwargs.get("owner_id")
        self.object_id = kwargs.get("object_id")
        self.amount = kwargs.get("amount")
        self.type = 'Tile'

    @property
    def owner(self):
        if self.owner_id:
            return session.query(Characters).filter(Characters.id==self.owner_id).first() or \
                   session.query(Guilds).filter(Guilds.id==self.owner_id).first()
        elif self.object_id:
            return session.query(Characters).filter(Buildings.object_id==self.object_id,
                                                    Buildings.owner_id==Characters.id).first()
        return None

    @staticmethod
    def remove(object_ids, autocommit=True):
        if not isinstance(object_ids, (list, set, tuple)):
            object_ids = (object_ids,)
        session.query(BuildableHealth).filter(BuildableHealth.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(BuildingInstances).filter(BuildingInstances.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(Buildings).filter(Buildings.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(DestructionHistory).filter(DestructionHistory.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemInventory).filter(ItemInventory.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemProperties).filter(ItemProperties.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(Properties).filter(Properties.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ActorPosition).filter(ActorPosition.id.in_(object_ids)).delete(synchronize_session='fetch')
        if autocommit:
            session.commit()

    def __repr__(self):
        return f"<Tiles(owner_id={self.owner_id}, object_id={self.object_id}, amount={self.amount}, type='{self.type}')>"

class Thralls:
    def __init__(self, *args, **kwargs):
        self.owner_id = kwargs.get("owner_id")
        self.object_id = kwargs.get("object_id")
        self.type = 'Thrall'

    @property
    def owner(self):
        if self.owner_id:
            return session.query(Characters).filter(Characters.id==self.owner_id).first() or \
                   session.query(Guilds).filter(Guilds.id==self.owner_id).first()
        elif self.object_id:
            property = session.query(Properties).filter_by(object_id=self.object_id).first()
            if property:
                return property.owner
        return None

    @staticmethod
    def remove(object_ids, autocommit=True):
        if not isinstance(object_ids, (list, set, tuple)):
            object_ids = (object_ids,)
        session.query(CharacterStats).filter(CharacterStats.char_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(Properties).filter(Properties.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemInventory).filter(ItemInventory.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemProperties).filter(ItemProperties.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ActorPosition).filter(ActorPosition.id.in_(object_ids)).delete(synchronize_session='fetch')
        if autocommit:
            session.commit()

    def __repr__(self):
        return f"<Thrall(owner_id={self.owner_id}, object_id={self.object_id}, type='{self.type}')>"

class BuildingTiles(Tiles):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'Building'

class Placeables(Tiles):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'Placeable'

class CharList(tuple):
    @property
    def last_to_login(self):
        last = datetime(year=1970, month=1, day=1)
        res = None
        for c in self:
            if c.last_login > last:
                res = c
                last = c.last_login
        return res

    def active(self, td):
        return CharList(c for c in self if c.last_login >= datetime.utcnow() - td)

    def inactive(self, td):
        return CharList(c for c in self if c.last_login < datetime.utcnow() - td)

class PropertiesList(tuple):
    @property
    def is_thrall(self):
        for p in self:
            if "OwnerUniqueID" in p.name:
                return True
        return False

    @property
    def owner_id(self):
        for p in self:
            if "OwnerUniqueID" in p.name:
                return unpack("<Q", p.value[-8:])[0]
        return None

    @property
    def name(self):
        for p in self:
            if "PetName" in p.name or "ThrallName" in p.name:
                return Properties._get_name(p)
        for p in self:
            if "ThrallInfo" in p.name:
                return Properties._get_name(p)
        return None

    @property
    def owner(self):
        id = self.owner_id
        if id is None:
            return None
        return Owner.get(id)

class TilesManager:
    @classmethod
    def get_tiles_by_owner(cls, bMult=1, pMult=1, do_round=True):
        # stores all tiles indexed by their respective owners
        tiles = {}
        # tiles that have an object_id in both Buildings and BuildingInstances are root object building tiles
        root = set()
        # res has format (object_id, owner_id, count(object_id)) contains only building tiles and their aggregated objects
        for res in session.query(Buildings.object_id, Buildings.owner_id, func.count(Buildings.object_id)) \
                          .filter(Buildings.object_id==BuildingInstances.object_id) \
                          .group_by(Buildings.object_id).all():
            # create a new dict entry if owner does not have one yet
            if not res[1] in tiles:
                tiles[res[1]] = res[2] * bMult
            # add aggregated tiles if one already exists
            else:
                tiles[res[1]] += res[2] * bMult
            # remember the object_id as root object in either case
            root.add(res[0])

        # res has format: (object_id, owner_id) contains all building tiles and placeables
        for res in session.query(Buildings.object_id, Buildings.owner_id).filter(Buildings.object_id==ActorPosition.id).all():
            # if object is not a root object, it is a placeable and needs to be added now
            if not res[0] in root:
                # if owner is not in tiles (i.e. owner has no building tiles) create a new dict entry
                if not res[1] in tiles:
                    tiles[res[1]] = pMult
                # otherwise add it to the count
                else:
                    tiles[res[1]] += pMult
        if do_round:
            for owner_id, value in tiles.items():
                tiles[owner_id] = int(round(value, 0))
        return tiles

    @staticmethod
    def get_tiles_consolidated(bMult=1, pMult=1, min_dist=50000, do_round=True):
        AP = ActorPosition
        B = Buildings
        BI = BuildingInstances
        tiles_to_consolidate = {}
        owner_index = {}
        query = (AP.x, AP.y, AP.z, AP.class_, B.object_id, B.owner_id, func.count(B.object_id))
        filter = (AP.id == B.object_id) & (B.object_id == BI.object_id)
        # get all Building pieces and their associated coordiantes and owners excluding pure placeables
        for x, y, z, class_, object_id, owner_id, tiles in session.query(*query).filter(filter).group_by(B.object_id).all():
            _, _, c = class_.partition('.')
            tiles_to_consolidate[object_id] = {'x': x, 'y': y, 'z': z, 'class': c, 'owner_id': owner_id, 'tiles': tiles * bMult}
            # keep a second dict to allow us to find all the objects belonging to an object_id
            if owner_id in owner_index:
                owner_index[owner_id] += [object_id]
            else:
                owner_index[owner_id] = [object_id]

        query = (AP.x, AP.y, AP.z, AP.class_, B.object_id, B.owner_id)
        filter = (AP.id == B.object_id)
        # get all placeables and their associated attributes
        for x, y, z, class_, object_id, owner_id in session.query(*query).filter(filter).all():
            # disregard building pieces that have already been added in the first loop
            if object_id in tiles_to_consolidate:
                continue
            _, _, c = class_.partition('.')
            tiles_to_consolidate[object_id] = {'x': x, 'y': y, 'z': z, 'class': c, 'owner_id': owner_id, 'tiles': pMult}
            # update the owner_index with the placeables
            if owner_id in owner_index:
                owner_index[owner_id] += [object_id]
            else:
                owner_index[owner_id] = [object_id]

        tiles_consolidated = {}
        tiles_per_owner = {}
        # do the consolidating
        for owner_id, object_ids in owner_index.items():
            # remember which objects were within min_dist of another
            remove = set()
            # go through all objects belonging to a single owner
            for i in range(len(object_ids)):
                # if object is within min_dist skip to the next
                if object_ids[i] in remove:
                    continue
                # if object wasn't removed yet add it to the final list
                tiles_consolidated[object_ids[i]] = tiles_to_consolidate[object_ids[i]]
                # go through all the remaining objects belonging to the same owner
                for j in range(i + 1, len(object_ids)):
                    # calculate distance to comparison object
                    dist = sqrt((tiles_to_consolidate[object_ids[i]]['x'] - tiles_to_consolidate[object_ids[j]]['x'])**2 +
                                (tiles_to_consolidate[object_ids[i]]['y'] - tiles_to_consolidate[object_ids[j]]['y'])**2 +
                                (tiles_to_consolidate[object_ids[i]]['z'] - tiles_to_consolidate[object_ids[j]]['z'])**2)
                    # if distance is shorter put it on the remove list and add tiles to current object
                    if dist <= min_dist:
                        remove.add(object_ids[j])
                        tiles_consolidated[object_ids[i]]['tiles'] += tiles_to_consolidate[object_ids[j]]['tiles']
                # for each owner store the absolute number of tiles to tiles_per_owner
                if owner_id in tiles_per_owner:
                    tiles_per_owner[owner_id] += tiles_consolidated[object_ids[i]]['tiles']
                else:
                    tiles_per_owner[owner_id] = tiles_consolidated[object_ids[i]]['tiles']

        # go through all consolidated objects and add the absolute number of tiles for that owner
        for object_id, ctd in tiles_consolidated.items():
            ctd['sum_tiles'] = tiles_per_owner[ctd['owner_id']]

        return tiles_consolidated

class MembersManager:
    @staticmethod
    def _get_guilds_query(threshold):
        C = Characters
        G = Guilds
        subquery1 = session.query(C.guild_id).filter(C.guild_id != None)
        empty_guilds = session.query(G.id, G.name, literal(0).label("members"), literal(0).label("last_login")).filter(G.id.notin_(subquery1))
        subquery2 = session.query(Buildings.owner_id).subquery()
        query = C.guild_id, G.name, func.count(C.guild_id), C._last_login
        filter = C.guild_id!=None, C._last_login>=threshold, G.id==C.guild_id, C.guild_id.in_(subquery2)
        populated_guilds = session.query(*query).filter(*filter).group_by(C.guild_id)
        return empty_guilds.union(populated_guilds)

    @staticmethod
    def _get_chars_query():
        subquery = session.query(Buildings.owner_id).subquery()
        return session.query(Characters.id, Characters.name, Characters._last_login) \
                      .filter(Characters.id.in_(subquery))

    @classmethod
    def get_members(cls, td=None):
        members = {}
        threshold = int((datetime.utcnow() - td).timestamp()) if td is not None else 0
        owners = set()
        for g in cls._get_guilds_query(0).all():
            owners.add(g[0])
            members[g[0]] = {'name': g[1], 'numMembers': g[2], 'numActiveMembers': g[2]}
        for c in cls._get_chars_query().all():
            numActiveMembers = 1 if c[2] >= threshold else 0
            members[c[0]] = {'name': c[1], 'numMembers': 1, 'numActiveMembers': numActiveMembers}
        if td is None:
            return members
        for g in cls._get_guilds_query(threshold):
            owners.remove(g[0])
            members[g[0]]['numActiveMembers'] = g[2]
        for g in owners:
            members[g]['numActiveMembers'] = 0
        return members

# game.db
class Account(GameBase):
    __tablename__ = 'account'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'
    player_id = Column('id', Text, ForeignKey('characters.playerId'), primary_key=True, nullable=False)
    funcom_id = Column('user', Text, nullable=False)
    # relationship
    _character = relationship('Characters', back_populates='_account')

    @property
    def characters(self):
        query = session.query(Characters) \
                       .filter(Characters.player_id.like((str(self._character.player_id) + '#_')) |
                              (Characters.player_id==self._character.player_id)) \
                       .order_by(Characters.player_id)
        return tuple(c for c in query.all())

    @property
    def user(self):
        return session.query(Users).filter_by(funcom_id==self.funcom_id).first()

    def __repr__(self):
        return f"<Account(player_id='{self.player_id}', funcom_id='{self.funcom_id}')>"

class ActorPosition(GameBase):
    __tablename__ = 'actor_position'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    class_ = Column('class', Text)
    id = Column(Integer, primary_key=True, nullable=False)
    # relationship
    building = relationship("Buildings", uselist=False, back_populates="position")
    _properties = relationship("Properties", back_populates="position")

    @property
    def tp(self):
        return f"TeleportPlayer {int(round(self.x, 0))} {int(round(self.y, 0))} {ceil(self.z)}"

    def distance_to(self, pos):
        return int(round(sqrt((self.x - pos.x)**2 + (self.y - pos.y)**2 + (self.z - pos.z)**2), 0))

    @staticmethod
    def distance_between(pos1, pos2):
        return int(round(sqrt((pos1.x - pos2.x)**2 + (pos1.y - pos2.y)**2 + (pos1.z - pos2.z)**2), 0))

    @property
    def properties(self):
        return PropertiesList(self._properties)

    def __repr__(self):
        return f"<ActorPosition(id={self.id}, class='{self.class_}')>"

class BuildableHealth(GameBase):
    __tablename__ = 'buildable_health'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    object_id = Column(Integer, ForeignKey('actor_position.id'), primary_key=True, nullable=False)
    instance_id = Column(Integer, ForeignKey('buildable_health.instance_id'), primary_key=True, nullable=False)
    template_id = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<BuildableHealth(object_id={self.object_id}, instance_id={self.instance_id}, template_id={self.template_id})>"

class BuildingInstances(GameBase):
    __tablename__ = 'building_instances'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    object_id = Column(Integer, ForeignKey('actor_position.id'), primary_key=True, nullable=False)
    instance_id = Column(Integer, primary_key=True)
    class_ = Column('class', Text)

    def __repr__(self):
        return f"<BuildingInstances(object_id={self.object_id}, instance_id={self.instance_id}, class='{self.class_}')>"

class Buildings(GameBase):
    __tablename__ = 'buildings'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    object_id = Column(Integer, ForeignKey('actor_position.id'), primary_key=True, nullable=False)
    # relationship
    position = relationship("ActorPosition", uselist=False, back_populates="building")

    @property
    def owner(self):
        char = session.query(Characters).filter_by(id=self.owner_id).first()
        if char:
            return char
        guild = session.query(Guilds).filter_by(id=self.owner_id).first()
        return guild

    @staticmethod
    def give_to_owner(old_owner_id, new_owner_id, loc=None, autocommit=True):
        if loc is not None:
            try:
                if not isinstance(loc, (list, set, tuple)):
                    print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                    print("loc:", loc)
                    return None
                elif len(loc) == 2:
                    x, y = loc
                    filter = ((ActorPosition.id == Buildings.object_id) &
                              (Buildings.owner_id == old_owner_id) &
                              (ActorPosition.x.between(x[0], x[1])) &
                              (ActorPosition.y.between(y[0], y[1])))
                elif len(loc) == 3:
                    x, y, z = loc
                    filter = ((ActorPosition.id == Buildings.object_id) &
                              (Buildings.owner_id == old_owner_id) &
                              (ActorPosition.x.between(x[0], x[1])) &
                              (ActorPosition.y.between(y[0], y[1])) &
                              (ActorPosition.z.between(z[0], z[1])))
                else:
                    return None
                object_ids = session.query(Buildings.object_id).filter(filter)
                session.query(Buildings).filter(Buildings.object_id.in_(object_ids)) \
                       .update({Buildings.owner_id: new_owner_id}, synchronize_session='fetch')
            except:
                print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                print("loc:", loc)
                return None
        else:
            filter = (Buildings.owner_id == old_owner_id)
            session.query(Buildings).filter(filter) \
                   .update({Buildings.owner_id: new_owner_id}, synchronize_session='fetch')
        if autocommit:
            session.commit()

    @staticmethod
    def remove_by_owner(owner_id, loc=None, autocommit=True):
        if loc is not None:
            try:
                if not isinstance(loc, (list, set, tuple)):
                    print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                    print("loc:", loc)
                    return None
                elif len(loc) == 2:
                    x, y = loc
                    filter = ((ActorPosition.id == Buildings.object_id) &
                              (Buildings.owner_id == owner_id) &
                              (ActorPosition.x.between(x[0], x[1])) &
                              (ActorPosition.y.between(y[0], y[1])))
                elif len(loc) == 3:
                    x, y, z = loc
                    filter = ((ActorPosition.id == Buildings.object_id) &
                              (Buildings.owner_id == owner_id) &
                              (ActorPosition.x.between(x[0], x[1])) &
                              (ActorPosition.y.between(y[0], y[1])) &
                              (ActorPosition.z.between(z[0], z[1])))
                else:
                    return None
            except:
                print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                print("loc:", loc)
                return None
        else:
            filter = (Buildings.owner_id == owner_id)

        object_ids = session.query(Buildings.object_id).filter(filter)
        if loc:
            object_ids_temp = [o for (o,) in object_ids.all()]
        session.query(BuildableHealth).filter(BuildableHealth.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(BuildingInstances).filter(BuildingInstances.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(DestructionHistory).filter(DestructionHistory.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemInventory).filter(ItemInventory.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ItemProperties).filter(ItemProperties.owner_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(Properties).filter(Properties.object_id.in_(object_ids)).delete(synchronize_session='fetch')
        session.query(ActorPosition).filter(ActorPosition.id.in_(object_ids)).delete(synchronize_session='fetch')
        if loc:
            for object_id in object_ids_temp:
                session.query(Buildings).filter_by(object_id=object_id).delete()
        else:
            session.query(Buildings).filter_by(owner_id=owner_id).delete(synchronize_session='fetch')
        if autocommit:
            session.commit()

    @staticmethod
    def restore_from_backup(owner_id, loc=None, remove=True, autocommit=True):
        def get_kwargs(table, values):
            kwargs = {}
            idx = 0
            for col in table.__table__.columns:
                n = col.name if col.name != 'class' else 'class_'
                kwargs[n] = values[idx]
                idx += 1
            return kwargs

        try:
            engine = create_engine(BACKUP_DB_URI, echo=ECHO)
        except:
            print(f"Couldn't open backup.db at {BACKUP_DB_URI}.")
            return None

        if loc is not None:
            try:
                stmt = ( "SELECT object_id "
                         "FROM actor_position, buildings "
                        f"WHERE owner_id = {owner_id} "
                         "AND id = object_id ")
                if not isinstance(loc, (list, set, tuple)):
                    print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                    print("loc:", loc)
                    return None
                elif len(loc) == 2:
                    x, y = loc
                    stmt += (f"AND (x BETWEEN {x[0]} AND {x[1]}) "
                             f"AND (y BETWEEN {y[0]} AND {y[1]})")
                elif len(loc) == 3:
                    x, y, z = loc
                    stmt += (f"AND (x BETWEEN {x[0]} AND {x[1]}) "
                             f"AND (y BETWEEN {y[0]} AND {y[1]}) "
                             f"AND (z BETWEEN {z[0]} AND {z[1]})")
                else:
                    return None
            except:
                print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
                print("loc:", loc)
                return None
        else:
            stmt = f"SELECT object_id FROM buildings WHERE owner_id = {owner_id}"

        with engine.connect() as conn:
            object_ids = tuple(id for (id,) in conn.execute(stmt))
            filter = str(object_ids)
            buildable_health = tuple(conn.execute(f"SELECT * FROM buildable_health WHERE object_id IN {filter}"))
            building_instances = tuple(conn.execute(f"SELECT * FROM building_instances WHERE object_id IN {filter}"))
            destruction_history = tuple(conn.execute(f"SELECT * FROM destruction_history WHERE object_id IN {filter}"))
            item_inventory = tuple(conn.execute(f"SELECT * FROM item_inventory WHERE owner_id IN {filter}"))
            item_properties = tuple(conn.execute(f"SELECT * FROM item_properties WHERE owner_id IN {filter}"))
            properties = tuple(conn.execute(f"SELECT * FROM properties WHERE object_id IN {filter}"))
            actor_position = tuple(conn.execute(f"SELECT * FROM actor_position WHERE id IN {filter}"))
        engine.dispose()

        if remove:
            Buildings.remove_by_owner(owner_id, loc, autocommit)

        for c in buildable_health:
            session.add(BuildableHealth(**get_kwargs(BuildableHealth, c)))
        for c in building_instances:
            session.add(BuildingInstances(**get_kwargs(BuildingInstances, c)))
        for c in destruction_history:
            session.add(DestructionHistory(**get_kwargs(DestructionHistory, c)))
        for c in item_inventory:
            session.add(ItemInventory(**get_kwargs(ItemInventory, c)))
        for c in item_properties:
            session.add(ItemProperties(**get_kwargs(ItemProperties, c)))
        for c in properties:
            session.add(Properties(**get_kwargs(Properties, c)))
        for c in actor_position:
            session.add(ActorPosition(**get_kwargs(ActorPosition, c)))
        for id in object_ids:
            session.add(Buildings(object_id=id, owner_id=owner_id))
        if autocommit:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)
                session.commit()

    def __repr__(self):
        return f"<Buildings(object_id={self.object_id}, owner_id={self.owner_id})>"

class CharacterStats(GameBase):
    __tablename__ = 'character_stats'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    char_id = Column(Integer, ForeignKey('characters.id'), primary_key=True, nullable=False)
    stat_id = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<CharacterStats(char_id={self.char_id}, stat_id={self.stat_id})>"

class Guilds(GameBase, Owner):
    __tablename__ = 'guilds'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    id = Column('guildId', Integer, primary_key=True, nullable=False)
    message_of_the_day = Column('messageOfTheDay', Text, default='')
    owner_id = Column('owner', Integer, ForeignKey('characters.id'), default=0)
    # relationships
    owner = relationship('Characters', foreign_keys=[owner_id])

    @property
    def members(self):
        return CharList(self._members)

    @property
    def last_login(self):
        if len(self._members) > 0:
            return CharList(self._members).last_to_login.last_login
        else:
            return None

    def active_members(self, td):
        return CharList(member for member in self.members if not member.is_inactive(td))

    def inactive_members(self, td):
        return CharList(member for member in self.members if member.is_inactive(td))

    @property
    def is_guild(self):
        return True

    @property
    def is_character(self):
        return False
    def __repr__(self):
        return f"<Guilds(id={self.id}, name='{self.name}')>"

class Characters(GameBase, Owner):
    __tablename__ = 'characters'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    id = Column(Integer, primary_key=True, nullable=False)
    player_id = Column('playerId', Text, nullable=False)
    guild_id = Column('guild', Integer, ForeignKey('guilds.guildId'))
    name = Column('char_name', Text, nullable=False)
    _last_login = Column('lastTimeOnline', Integer)
    # relationship
    guild = relationship('Guilds', backref="_members", foreign_keys=[guild_id])
    _account = relationship("Account", uselist=False, back_populates="_character")

    @staticmethod
    def get_users(value):
        results = session.query(Characters).filter(Characters.name.like('%' + str(value) + '%')).all()
        users = []
        for char in results:
            user = session.query(Users).filter_by(funcom_id=char.account.funcom_id).first()
            users += [user] if user and not user in users else []
        return users

    @staticmethod
    def remove(character_ids, autocommit=True):
        if not isinstance(character_ids, (list, set, tuple)):
            character_ids = (character_ids,)
        for id in character_ids:
            char = session.query(Characters).get(id)
            player_id = char.pure_player_id
            # if char is the last character in its guild also remove the guild
            if char.guild and len(char.guild.members) == 1:
                session.delete(char.guild)
            filter = Characters.player_id.like(player_id + '#_') | (Characters.player_id==player_id)
            # if char is the last character with the given player_id also remove it from the account table
            num = session.query(func.count(Characters.id)).filter(filter).order_by(Characters.id).scalar()
            if num == 1:
                acc = session.query(Account).filter_by(player_id=player_id).first()
                if acc:
                    session.delete(acc)

        session.query(ActorPosition).filter(ActorPosition.id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(CharacterStats).filter(CharacterStats.char_id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(ItemInventory).filter(ItemInventory.owner_id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(ItemProperties).filter(ItemProperties.owner_id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(Properties).filter(Properties.object_id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(Purgescores).filter(Purgescores.purge_id.in_(character_ids)).delete(synchronize_session='fetch')
        session.query(Characters).filter(Characters.id.in_(characters)).delete(synchronize_session='fetch')
        if autocommit:
            session.commit()

    @staticmethod
    def move_to_guild(char_id, guild_id, autocommit=True):
        char = session.query(Characters).get(char_id)
        guild = session.query(Guilds).get(guild_id)
        if char and guild:
            char.guild = guild
            if autocommit:
                session.commit()

    @staticmethod
    def set_last_login(char_id, date=None, autocommit=True):
        if not date:
            ts = floor(datetime.utcnow().timestamp())
        else:
            ts = floor(date.timestamp())
        char = session.query(Characters).get(char_id)
        if char and ts:
            char.last_login = ts
            if autocommit:
                session.commit()

    @property
    def user(self):
        return session.query(Users).filter_by(funcom_id=self.account.funcom_id).first()

    @property
    def pure_player_id(self):
        return self.player_id if len(self.player_id) <= 2 or self.player_id[-2] != '#' else self.player_id[:-2]

    @property
    def account(self):
        if self.slot == 'active':
            return self._account
        else:
            return session.query(Account).filter_by(player_id=self.pure_player_id).one()

    @property
    def slot(self):
        if self.player_id is None:
            return 'undetermined'
        return 'active' if len(self.player_id) <= 2 or self.player_id[-2] != '#' else self.player_id[-1]

    @property
    def last_login(self):
        return datetime.utcfromtimestamp(self._last_login)

    @last_login.setter
    def last_login(self, value):
        self._last_login = value

    @property
    def has_guild(self):
        return not self.guild_id is None

    @property
    def rank_name(self):
        if not type(self.rank) is int:
            return None
        # some entries seem to be buggy and outside of the normal 0-3 range
        elif not self.rank in (0, 1, 2, 3):
            return RANKS[3]
        return RANKS[self.rank]

    @property
    def is_guild(self):
        return False

    @property
    def is_character(self):
        return True

    def __repr__(self):
        return f"<Characters(id={self.id}, name='{self.name}')>"

class DestructionHistory(GameBase):
    __tablename__ = 'destruction_history'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    owner_id = Column(Integer, primary_key=True)
    destroyed_by = Column(Text, primary_key=True)
    object_type = Column(Integer, primary_key=True)
    object_id = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<FollowerMarkers(owner_id={self.owner_id}, follower_id={self.follower_id})>"

class FollowerMarkers(GameBase):
    __tablename__ = 'follower_markers'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    owner_id = Column(Integer, primary_key=True)
    follower_id = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<FollowerMarkers(owner_id={self.owner_id}, follower_id={self.follower_id})>"

class GameEvents(GameBase):
    __tablename__ = 'game_events'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    world_time = Column('worldTime', Integer, primary_key=True)
    event_type = Column('eventType', Integer, primary_key=True)
    object_id = Column(Integer, ForeignKey('actor_position.id'), primary_key=True, nullable=False)

    def __repr__(self):
        return f"<GameEvents(world_time={self.world_time}, event_type={self.event_type}, object_id={self.object_id})>"

class ItemInventory(GameBase):
    __tablename__ = 'item_inventory'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    item_id = Column(Integer, primary_key=True, nullable=False)
    owner_id = Column(Integer, primary_key=True, nullable=False)

    @staticmethod
    def remove(template_ids=None, autocommit=True):
        if not isinstance(template_ids, (list, set, tuple)):
            template_ids = (template_ids,)
        session.query(ItemInventory).filter(ItemInventory.id.in_(template_ids)).delete(synchronize_session='fetch')
        if autocommit:
            session.commit()

    @staticmethod
    def copy_stats(template_id, owner_id, autocommit=True):
        data = session.query(ItemInventory.data).filter_by(template_id=template_id, owner_id=owner_id).first()
        if not data or not Owner.exists(owner_id):
            return None
        data = data[0]
        filter = ItemInventory.template_id == template_id
        session.query(ItemInventory).filter(filter).update({ItemInventory.data: data}, synchronize_session='fetch')
        if autocommit:
            session.commit()

    def __repr__(self):
        return f"<ItemInventory(item_id={self.item_id}, owner_id={self.owner_id})>"

class ItemProperties(GameBase):
    __tablename__ = 'item_properties'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    item_id = Column(Integer, primary_key=True, nullable=False)
    owner_id = Column(Integer, primary_key=True, nullable=False)
    inv_type = Column(Integer, primary_key=True, nullable=False)

    def __repr__(self):
        return f"<ItemInventory(item_id={self.item_id}, owner_id={self.owner_id}, inv_type={self.inv_type})>"

class Properties(GameBase):
    __tablename__ = 'properties'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    object_id = Column(Integer, ForeignKey('actor_position.id'), primary_key=True, nullable=False)
    name = Column(Text, primary_key=True, nullable=False)
    # relationship
    position = relationship("ActorPosition", uselist=False, back_populates="_properties")

    @staticmethod
    def _get_name(p, names=None):
        # if thrall name has been changed by the player it's stored in a row with PetName or ThrallName as name
        if "PetName" in p.name or "ThrallName" in p.name:
            # the codec type (negative = utf-16, positive = utf-8) and string length are in bytes 17-21
            type = unpack("l", p.value[17:21])[0]
            res = (21 + type - 1, "utf-8") if type > 0 else (21 + (abs(type) - 1) * 2, "utf-16")
            return p.value[21:res[0]].decode(res[1])
        # if thrall still has default game name it has to be derived from the TemplateTableSpawn.json
        elif "ThrallInfo" in p.name and (names or os.path.isfile('TemplateTableSpawn.json')):
            end = 12 + unpack("l", p.value[8:12])[0] - 1
            thrall_class = p.value[12:end].decode("utf-8")
            if not names:
                with open('TemplateTableSpawn.json') as json_file:
                    spawns = json.load(json_file)
                for template in spawns:
                    if template['RowName'] == thrall_class:
                        return eval(template['Name'][9:])[2]
            else:
                return names.get(thrall_class)
        return None

    @staticmethod
    def get_thrall_object_ids(name=None, owner_id=None, strict=False):
        objects = []
        if name:
            names = {}
            # read class <=> name link from json if available
            if os.path.isfile('TemplateTableSpawn.json'):
                with open('TemplateTableSpawn.json') as json_file:
                    spawns = json.load(json_file)
                for template in spawns:
                    names[template['RowName']] = eval(template['Name'][9:])[2]

            # thralls with custom names
            name_filter = (Properties.name.like("%PetName")) | (Properties.name.like("%ThrallName"))
            custom_name_ids_query = session.query(Properties.object_id).filter(name_filter)
            # thralls with no custom names
            info_filter = (Properties.name.like("%ThrallInfo")) & (Properties.object_id.notin_(custom_name_ids_query))

            # get object_ids for thralls with custom names
            for p in session.query(Properties).filter(name_filter).all():
                nam = Properties._get_name(p)
                if (strict and name.lower() == nam.lower()) or (not strict and name.lower() in nam.lower()):
                    objects.append(p.object_id)

            for p in session.query(Properties).filter(info_filter).all():
                nam = Properties._get_name(p, names)
                if nam and ((strict and name.lower() == nam.lower()) or (not strict and name.lower() in nam.lower())):
                    objects.append(p.object_id)

        elif owner_id:
            for p in session.query(Properties).filter(Properties.name.like("%OwnerUniqueID")).all():
                own_id = unpack("<Q", p.value[-8:])[0]
                if owner_id == own_id:
                    objects.append(p.object_id)
        return objects

    @staticmethod
    def get_thrall_owners(name=None, object_id=None, owner_id=None, strict=False):
        owners = {}
        if name:
            names = {}
            # read class <=> name link from json if available
            if os.path.isfile('TemplateTableSpawn.json'):
                with open('TemplateTableSpawn.json') as json_file:
                    spawns = json.load(json_file)
                    for template in spawns:
                        names[template['RowName']] = eval(template['Name'][9:])[2]

            # thralls with custom names
            name_filter = (Properties.name.like("%PetName")) | (Properties.name.like("%ThrallName"))
            custom_name_ids_query = session.query(Properties.object_id).filter(name_filter)
            # thralls with no custom names
            info_filter = (Properties.name.like("%ThrallInfo")) & (Properties.object_id.notin_(custom_name_ids_query))

            # get owners for thralls with custom names
            for p in session.query(Properties).filter(name_filter).all():
                nam = Properties._get_name(p)
                if (strict and name.lower() == nam.lower()) or (not strict and name.lower() in nam.lower()):
                    owner_filter = (Properties.object_id==p.object_id) & (Properties.name.like("%OwnerUniqueID"))
                    po = session.query(Properties).filter(owner_filter).first()
                    if po:
                        owners[nam] = {"owner": po.owner, "object_id": p.object_id}

            # get owners for thralls with default names
            for p in session.query(Properties).filter(info_filter).all():
                nam = Properties._get_name(p, names)
                n = nam if nam else "None"
                if nam and ((strict and name.lower() == nam.lower()) or (not strict and name.lower() in nam.lower())):
                    owner_filter = (Properties.object_id==p.object_id) & (Properties.name.like("%OwnerUniqueID"))
                    po = session.query(Properties).filter(owner_filter).first()
                    if po:
                        owners[nam] = {"owner": po.owner, "object_id": p.object_id}

        elif owner_id:
            owner = Owner.get(owner_id)
            for p in session.query(Properties).filter(Properties.name.like("%OwnerUniqueID")).all():
                own_id = unpack("<Q", p.value[-8:])[0]
                if owner_id == own_id:
                    pl = PropertiesList(session.query(Properties).filter_by(object_id=p.object_id).all())
                    nam = pl.name
                    if nam:
                        owners[nam] = {"owner": owner, "object_id": p.object_id}

        elif object_id:
            pl = PropertiesList(session.query(Properties).filter_by(object_id=object_id).all())
            nam = pl.name
            if nam:
                owners[nam] = {"owner": pl.owner, "object_id": object_id}

        return owners

    @staticmethod
    def give_thrall(object_id, owner_id, autocommit=True):
        if object_id is None:
            return None
        filter = (Properties.object_id==object_id) & (Properties.name.like('%OwnerUniqueId'))
        p = session.query(Properties).filter(filter).first()
        o = Owner.exists(owner_id)
        if not (p and o):
            return None
        p.owner_id = owner_id
        if autocommit:
            session.commit()

    @property
    def is_thrall(self):
        if "OwnerUniqueID" in self.name or "PetName" in self.name or "ThrallName" in self.name:
            return True
        return False

    @property
    def thrall_name(self, names=None):
        return self._get_name(self, names)

    @property
    def owner_id(self):
        if "OwnerUniqueID" in self.name:
            return unpack("<Q", self.value[-8:])[0]
        return None

    @owner_id.setter
    def owner_id(self, value):
        if "OwnerUniqueID" in self.name:
            self.value = self.value[:-8] + pack("<Q", value)

    @property
    def owner(self):
        id = self.owner_id
        if id is None:
            return None
        return Owner.get(id)

    @owner.setter
    def owner(self, value):
        if not isinstance(value, (Guilds, Characters)):
            return None
        self.owner_id = value.id

    def __repr__(self):
        return f"<Properties(object_id={self.object_id}, name='{self.name}')>"

class Purgescores(GameBase):
    __tablename__ = 'purgescores'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    purge_id = Column('purgeid', Integer, primary_key=True, nullable=False)

    def __repr__(self):
        return f"<Purgescores(purge_id={self.purge_id})>"

class ServerPopulationRecordings(GameBase):
    __tablename__ = 'serverPopulationRecordings'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'

    time_of_recording = Column('timeOfRecording', Integer, primary_key=True, nullable=False)

    def __repr__(self):
        return f"<ServerPopulationRecordings(time_of_recording={self.time_of_recording}, population={self.population})>"

# supplemental.db
class Users(UsersBase):
    __tablename__ = 'users'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    disc_user = Column(String, unique=True, nullable=False)
    disc_id = Column(String(18), unique=True)
    funcom_id = Column(String(16), unique=True)

    def __repr__(self):
        disc_user = f"'{str(self.disc_user)}'" if self.disc_user else "None"
        disc_id = f"'{str(self.disc_id)}'" if self.disc_id else "None"
        funcom_id = f"'{str(self.funcom_id)}'" if self.funcom_id else "None"
        return f"<Users(id={self.id}, disc_user={disc_user}, disc_id={disc_id}, funcom_id={funcom_id})>"

    @property
    def characters(self):
        player_id = str(self.get_player_id(self.funcom_id))
        characters = CharList(c for c in session.query(Characters)
                                                .filter(Characters.player_id.like(player_id + '#_') |
                                                       (Characters.player_id==player_id))
                                                .order_by(Characters.player_id).all())
        return characters

    @staticmethod
    def get_player_id(value):
        result = session.query(Account.player_id).filter_by(funcom_id=value).first()
        return result[0] if result else None

    @staticmethod
    def get_users(value):
        if len(str(value)) >= 17 and str(value).isnumeric():
            result = session.query(Users).filter_by(disc_id=value).first()
            if result:
                return [result]
            return []
        elif len(value) > 5 and value[-5] == '#':
            result = session.query(Users).filter(Users.disc_user.collate('NOCASE')==value).first()
            if result:
                return [result]
        result = session.query(Users).filter(Users.disc_user.like((value + '#____'))).first()
        if result:
            return [result]
        results = [u for u in session.query(Users).filter(Users.disc_user.like(('%' + value + '%#____'))).all()]
        if results:
            return results
        return []

    @staticmethod
    def get_disc_users(value):
        if len(value) > 5 and value[-5] == '#':
            result = session.query(Users.disc_user).filter(Users.disc_user.collate('NOCASE')==value).first()
            if result:
                return result[0]
        result = session.query(Users.disc_user).filter(Users.disc_user.like((value + '#____'))).first()
        if result:
            return result[0]
        results = tuple(u[0] for u in session.query(Users.disc_user).filter(Users.disc_user.like(('%' + value + '%#____'))).all())
        if results:
            return results[0] if len(results) == 1 else results
        return None

class OwnersCache(UsersBase, Owner):
    __tablename__ = 'owners_cache'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)

    def __init__(self, *args, **kwargs):
        if kwargs.get('id') == None:
            raise ValueError("Missing argument 'id' to initialize OwnersCache")
        super().__init__(*args, **kwargs)

    @staticmethod
    def update(ruins_clan_id=11, autocommit=True):
        owners = cache = {}
        results = session.query(Guilds.id, Guilds.name).filter(Guilds.name!='Ruins').all()
        owners = {owner[0]: owner[1] for owner in results}
        results = session.query(Characters.id, Characters.name).filter(Characters.name!='Ruins').all()
        owners.update({owner[0]: owner[1] for owner in results})
        cache = {owner.id: owner.name for owner in session.query(OwnersCache).all()}
        if 0 not in owners:
            owners[0] = 'Game Assets'
        if not ruins_clan_id in owners:
            owners[ruins_clan_id] = 'Ruins'
        for id, name in owners.items():
            if not id in cache:
                new_owner = OwnersCache(id=id, name=name)
                session.add(new_owner)
            elif cache[id] != owners[id]:
                changed_owner = session.query(OwnersCache).get(id)
                changed_owner.name = name
        if autocommit:
            session.commit()

    def __repr__(self):
        return f"<OwnersCache(id={self.id}, name='{self.name}')>"

class ObjectsCache(UsersBase):
    __tablename__ = 'objects_cache'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    _timestamp = Column('timestamp', Integer, nullable=False)

    def __init__(self, *args, **kwargs):
        if kwargs.get('id') == None:
            raise ValueError("Missing argument 'id' to initialize ObjectsCache")
        if ous := kwargs.get('owner_unknown_since'):
            self.owner_unknown_since = ous
            del kwargs['owner_unknown_since']
        super().__init__(*args, **kwargs)

    @staticmethod
    def update(ruins_clan_id=11, autocommit=True):
        objects = cache = {}
        sqGuilds = session.query(Guilds.id)
        sqChars = session.query(Characters.id)
        objects = {obj[0] for obj in session.query(Buildings.object_id).filter(
            Buildings.owner_id.notin_(sqChars) &
            Buildings.owner_id.notin_(sqGuilds) &
            (Buildings.owner_id != 0) |
            (Buildings.owner_id == ruins_clan_id)).all()}
        cache = {obj.id: obj.owner_unknown_since for obj in session.query(ObjectsCache).all()}
        for id, dt in cache.items():
            if not id in objects:
                del_obj = session.query(ObjectsCache).get(id)
                session.delete(del_obj)
        now = int(datetime.utcnow().timestamp())
        for id in objects:
            if not id in cache:
                new_obj = ObjectsCache(id=id, _timestamp=now)
                session.add(new_obj)
        if autocommit:
            session.commit()

    @property
    def owner_unknown_since(self):
        return datetime.utcfromtimestamp(self._timestamp)

    @owner_unknown_since.setter
    def owner_unknown_since(self, value):
        if type(value) is datetime:
            self._timestamp = int(value.timestamp())
        elif type(value) is int:
            self._timestamp = value

    def __repr__(self):
        return f"<ObjectsCache(id={self.id}, owner_unknown_since='{self.owner_unknown_since}')>"

class DeleteChars(UsersBase):
    __tablename__ = 'delete_chars'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    player_id = Column(Text, unique=True, nullable=False)
    name = Column(Text, nullable=False)

    @staticmethod
    def add(chars, autocommit=True):
        for player_id, name in chars.items():
            dc = session.query(DeleteChars).filter_by(player_id=player_id).first()
            if dc:
                dc.name = name
            else:
                session.add(DeleteChars(player_id=player_id, name=name))
        if autocommit:
            session.commit()

    def __repr__(self):
        return f"<DeleteChars(id={self.id}, player_id='{self.player_id}', name='{self.name}')>"

class GlobalVars(UsersBase):
    __tablename__ = 'global_vars'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    value = Column(Text)

    @staticmethod
    def get_value(name):
        value = session.query(GlobalVars.value).filter_by(name=name).first()
        return value[0] if value else None

    @staticmethod
    def set_value(name, value, autocommit=True):
        gv = session.query(GlobalVars).filter_by(name=name).first()
        if gv:
            gv.value = value
        else:
            gv = GlobalVars(name=name, value=value)
            session.add(gv)
        if autocommit:
            session.commit()
        return gv

    def __repr__(self):
        return f"<GlobalVars(id={self.id}, name='{self.name}', value='{self.value}')>"

class Applications(UsersBase):
    __tablename__ = 'applications'
    __bind_key__ = 'usersdb'

    id               = Column(Integer, primary_key=True, nullable=False)
    disc_id          = Column(String(18), unique=True, nullable=False)
    status           = Column(String, nullable=False, default='open')
    funcom_id_row    = Column(Integer, default=None)
    current_question = Column(Integer, default=1)
    open_date        = Column(DateTime, default=datetime.utcnow())

    def __init__(self, disc_id, *args, **kwargs):
        kwargs['disc_id'] = disc_id
        for q in session.query(BaseQuestions).all():
            if q.has_funcom_id:
                self.funcom_id_row=q.id
            session.add(Questions(qnum=q.id, question=q.txt, answer='', application=self))
        super().__init__(*args, **kwargs)

    def can_edit_questions(self):
        return self.status in ('open', 'finished', 'review')

    @property
    def first_unanswered(self):
        if self.questions:
            for q in self.questions:
                if q.answer == '':
                    return q.qnum
        return -1

    def __repr__(self):
        return f"<Applications(id={self.id}, disc_id='{self.disc_id}', status='{self.status}')>"

class Questions(UsersBase):
    __tablename__ = 'questions'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    application_id = Column(Integer, ForeignKey(Applications.id, ondelete='CASCADE'))
    qnum = Column(Integer, nullable=False)
    question = Column(String)
    answer = Column(String)
    # relationships
    application = relationship('Applications', backref=backref("questions", cascade="all, delete"))

    def __repr__(self):
        return f"<Qustions(id={self.id}, qnum={self.qnum})>"

class BaseQuestions(UsersBase):
    __tablename__ = 'base_questions'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    txt = Column(String)
    has_funcom_id = Column('has_funcomID', Boolean, default=False)

    def __repr__(self):
        return f"<BaseQuestions(id={self.id})>"

class TextBlocks(UsersBase):
    __tablename__ = 'text_blocks'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    content = Column(String)

    @staticmethod
    def get(name=None, id=None, obj=False):
        if not name and not id:
            return None
        if not obj:
            if id:
                result = session.query(TextBlocks.content).filter_by(id=id).first()
            else:
                result = session.query(TextBlocks.content).filter_by(name=name).first()
            if result:
                return result[0]
            return None
        if id:
            return session.query(TextBlocks).get(id)
        else:
            return session.query(TextBlocks).filter_by(name=name).first()

    def __repr__(self):
        return f"<TextBlocks(id={self.id}, name='{self.name}', content='{self.content}')>"

class MagicChars(UsersBase):
    __tablename__ = 'magic_chars'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    mana = Column(Integer, default=0)
    total_uses = Column(Integer, default=0)
    total_spent = Column(Integer, default=0)
    last_use = Column(Integer, default=None)
    active = Column(Boolean, default=True)

    def __repr__(self):
        return f"<MagicUsers(id={self.id}, name='{self.name}', mana={self.mana}, active='{self.active}')>"

GameBase.metadata.create_all(engines['gamedb'])
UsersBase.metadata.create_all(engines['usersdb'])
