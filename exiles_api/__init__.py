from config import *
from datetime import datetime
from sqlalchemy import create_engine, desc, MetaData
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
class Owner:
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

    def __repr__(self):
        return f"<Tiles(owner_id={self.owner_id}, object_id={self.object_id}, amount={self.amount}, type='{self.type}')>"

class BuildingTiles(Tiles):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'Building'

class Placeables(Tiles):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'Placeable'

class CharList(tuple):
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

class TilesManager:
    def __init__(self):
        # stores all tiles indexed by their respective owners
        self.tiles = {}
        # tiles that have an object_id in both Buildings and BuildingInstances are root object building tiles
        self.root = set()

    def get_tiles_by_owner(self, bMult=1, pMult=1, do_round=True):
        # res has format (object_id, owner_id, count(object_id)) contains only building tiles and their aggregated objects
        for res in session.query(Buildings.object_id, Buildings.owner_id, func.count(Buildings.object_id)) \
                          .filter(Buildings.object_id==BuildingInstances.object_id) \
                          .group_by(Buildings.object_id).all():
            # create a new dict entry if owner does not have one yet
            if not res[1] in self.tiles:
                self.tiles[res[1]] = res[2] * bMult
            # add aggregated tiles if one already exists
            else:
                self.tiles[res[1]] += res[2] * bMult
            # remember the object_id as root object in either case
            self.root.add(res[0])

        # res has format: (object_id, owner_id) contains all building tiles and placeables
        for res in session.query(Buildings.object_id, Buildings.owner_id).filter(Buildings.object_id==ActorPosition.id).all():
            # if object is not a root object, it is a placeable and needs to be added now
            if not res[0] in self.root:
                # if owner is not in tiles (i.e. owner has no building tiles) create a new dict entry
                if not res[1] in self.tiles:
                    self.tiles[res[1]] = pMult
                # otherwise add it to the count
                else:
                    self.tiles[res[1]] += pMult
        if do_round:
            for owner_id, value in self.tiles.items():
                self.tiles[owner_id] = int(round(value, 0))
        return self.tiles

class MembersManager:
    def _get_guilds_query(self, threshold):
        subquery = session.query(Buildings.owner_id).subquery()
        return session.query(Characters.guild_id, Guilds.name, func.count(Characters.guild_id), Characters._last_login) \
                      .filter(Characters.guild_id!=None,
                              Characters._last_login>=threshold,
                              Guilds.id==Characters.guild_id,
                              Characters.guild_id.in_(subquery)) \
                      .group_by(Characters.guild_id)

    def _get_chars_query(self):
        subquery = session.query(Buildings.owner_id).subquery()
        return session.query(Characters.id, Characters.name, Characters._last_login) \
                      .filter(Characters.guild_id==None, Characters.id.in_(subquery))

    def get_members(self, td=None):
        self.members = {}
        threshold = int((datetime.utcnow() - td).timestamp()) if td is not None else 0
        owners = set()
        for g in self._get_guilds_query(0).all():
            owners.add(g[0])
            self.members[g[0]] = {'name': g[1], 'numMembers': g[2], 'numActiveMembers': g[2]}
        for c in self._get_chars_query().all():
            numActiveMembers = 1 if c[2] >= threshold else 0
            self.members[c[0]] = {'name': c[1], 'numMembers': 1, 'numActiveMembers': numActiveMembers}
        if td is None:
            return self.members
        for g in self._get_guilds_query(threshold):
            owners.remove(g[0])
            self.members[g[0]]['numActiveMembers'] = g[2]
        for g in owners:
            self.members[g]['numActiveMembers'] = 0
        return self.members

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

    @property
    def owner(self):
        char = session.query(Characters).filter_by(id=self.owner_id).first()
        if char:
            return char
        guild = session.query(Guilds).filter_by(id=self.owner_id).first()
        return guild

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
    owner_id = Column('owner', Integer, ForeignKey('characters.id'), default='')
    # relationships
    owner = relationship('Characters', foreign_keys=[owner_id])

    @property
    def members(self):
        return CharList(self._members)

    @property
    def last_login(self):
        if len(self._members) > 0:
            return CharList(self._members).last_to_login().last_login
        else:
            return None

    def active_members(self, td):
        return CharList(member for member in self.members if not member.is_inactive(td))

    def inactive_members(self, td):
        return CharList(member for member in self.members if member.is_inactive(td))

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

    world_time = Column('wordlTime', Integer, primary_key=True)
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
        if len(str(value)) == 18 and str(value).isnumeric():
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

GameBase.metadata.create_all(engines['gamedb'])
UsersBase.metadata.create_all(engines['usersdb'])
