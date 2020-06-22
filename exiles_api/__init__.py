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
class Player:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        if self.id:
            self.id = str(self.id)
            self.real_id = self.id if len(self.id) <= 2 or self.id[-2] != '#' else self.id[:-2]
        else:
            self.real_id = None
        self.funcom_id = kwargs.get("funcom_id")
        self.steam_id = kwargs.get("steam_id")
        self.disc_user = kwargs.get("disc_user")
        if self.disc_user and (len(self.disc_user) <= 5 or self.disc_user[-5] != '#'):
            self._get_disc_user_by_disc_short()
        # print(f"attributes after assigning kwargs: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
        found_attributes = num_attributes = self._get_num_attributes()
        while(found_attributes > 0 and not self._has_all_attributes()):
            if not self.id:
                if self.funcom_id:
                    self._get_player_id_by_funcom_id()
                    # print(f"attributes after fcid =>   id: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.id and self.steam_id:
                    self._get_player_id_by_steam_id()
                    # print(f"attributes after stid =>   id: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.id and self.disc_user:
                    self._get_player_id_by_disc_user()
                    # print(f"attributes after   du =>   id: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
            if not self.funcom_id:
                if self.id:
                    self._get_funcom_id_by_player_id()
                    # print(f"attributes after   id => fcid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.funcom_id and self.steam_id:
                    self._get_funcom_id_by_steam_id()
                    # print(f"attributes after stid => fcid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.funcom_id and self.disc_user:
                    self._get_funcom_id_by_disc_user()
                    # print(f"attributes after   du => fcid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
            if not self.steam_id:
                if self.id:
                    self._get_steam_id_by_player_id()
                    # print(f"attributes after   id => stid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.steam_id and self.funcom_id:
                    self._get_steam_id_by_funcom_id()
                    # print(f"attributes after fcid => stid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.steam_id and self.disc_user:
                    self._get_steam_id_by_disc_user()
                    # print(f"attributes after   du => stid: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
            if not self.disc_user:
                if self.id:
                    self._get_disc_user_by_player_id()
                    # print(f"attributes after   id =>   du: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.disc_user and self.funcom_id:
                    self._get_disc_user_by_funcom_id()
                    # print(f"attributes after fcid =>   du: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
                if not self.disc_user and self.steam_id:
                    self._get_disc_user_by_steam_id()
                    # print(f"attributes after stid =>   du: id == {self.id}, real_id == {self.real_id}, funcom_id == {self.funcom_id}, steam_id == {self.steam_id}, disc_user == {self.disc_user}")
                    if self._has_all_attributes():
                        break
            new_num_attributes = self._get_num_attributes()
            found_attributes = new_num_attributes - num_attributes
            num_attributes = new_num_attributes

        if self.real_id:
            self.characters = [c for c in session.query(Characters)
                                                 .filter(Characters.player_id.like(self.real_id + '#_') |
                                                        (Characters.player_id==self.real_id)).all()]
        else:
            self.characters = []

    def _has_all_attributes(self):
        return self.id is not None and \
               self.funcom_id is not None and \
               self.steam_id is not None and \
               self.disc_user is not None

    def _get_num_attributes(self):
        num = 0
        if self.id:
            num += 1
        if self.funcom_id:
            num += 1
        if self.steam_id:
            num += 1
        if self.disc_user:
            num += 1
        return num

    def _get_disc_user_by_disc_short(self):
        result = session.query(Users.disc_user).filter(Users.disc_user.like((self.disc_user + '#____'))).first()
        if result:
            self.disc_user = result[0]
            return
        result = session.query(Users.disc_user).filter(Users.disc_user.like(('%' + self.disc_user + '%#____'))).first()
        if result:
            self.disc_user = result[0]

    def _get_player_id_by_funcom_id(self):
        result = session.query(Account.player_id).filter_by(funcom_id=self.funcom_id).first()
        if result:
            self.real_id = self.id = str(result[0])

    def _get_player_id_by_steam_id(self):
        self._get_funcom_id_by_steam_id()
        if self.funcom_id and not self.id:
            self._get_player_id_by_funcom_id()
        # characters that have not logged in since the patch still have their steam_id in place of the funcom_id
        result = session.query(Account.player_id).filter_by(funcom_id=self.steam_id).first()
        if result:
            self.real_id = self.id = str(result[0])

    def _get_player_id_by_disc_user(self):
        user = session.query(Users).filter_by(disc_user=self.disc_user).first()
        if user:
            self.steam_id = user.steam_id or self.steam_id
            self.funcom_id = user.funcom_id or self.funcom_id
            if user.player_id:
                self.real_id = self.id = str(user.player_id)
            # characters that have not logged in since the patch still have their steam_id in place of the funcom_id
            if not self.id:
                result = session.query(Account.player_id).filter_by(funcom_id=self.steam_id).first()
                if result:
                    self.real_id = self.id = str(result[0])

    def _get_funcom_id_by_player_id(self):
        result = session.query(Account.funcom_id).filter_by(player_id=self.real_id).first()
        if result and len(result[0]) == 16:
            self.funcom_id = result[0]
        elif result and len(result[0]) == 17:
            self.steam_id = result[0]

    def _get_funcom_id_by_steam_id(self):
        result = session.query(Steam64.funcom_id).filter_by(id=self.steam_id).first()
        if result and len(result[0]) == 16:
            self.funcom_id = result[0]
        else:
            user = session.query(Users).filter_by(steam_id=self.steam_id).first()
            if user:
                self.disc_user = user.disc_user or self.disc_user
                self.funcom_id = user.funcom_id
                if user.player_id:
                    self.real_id = str(user.player_id)
                self.id = self.id or self.real_id

    def _get_funcom_id_by_disc_user(self):
        self._get_player_id_by_disc_user()

    def _get_steam_id_by_player_id(self):
        self._get_funcom_id_by_player_id()
        self._get_steam_id_by_funcom_id()

    def _get_steam_id_by_funcom_id(self):
        result = session.query(Steam64.id).filter_by(funcom_id=self.funcom_id).first()
        if result:
            self.steam_id = result[0]
        else:
            user = session.query(Users).filter_by(funcom_id=self.funcom_id).first()
            if user:
                self.steam_id = user.steam_id
                self.disc_user = user.disc_user or self.disc_user
                if user.player_id:
                    self.real_id = str(user.player_id)
                self.id = self.id or self.real_id

    def _get_steam_id_by_disc_user(self):
        result = session.query(Users.steam_id).filter_by(disc_user=self.disc_user).first()
        if result:
            self.steam_id = result[0]

    def _get_disc_user_by_player_id(self):
        self._get_funcom_id_by_player_id()
        self._get_disc_user_by_funcom_id()

    def _get_disc_user_by_funcom_id(self):
        user = session.query(Users).filter_by(funcom_id=self.funcom_id).first()
        if user:
            self.steam_id = user.steam_id or self.steam_id
            self.disc_user = user.disc_user
            if user.player_id:
                self.real_id = str(user.player_id)
            self.id = self.id or self.real_id

    def _get_disc_user_by_steam_id(self):
        user = session.query(Users).filter_by(steam_id=self.steam_id).first()
        if user:
            self.steam_id = user.steam_id or self.steam_id
            self.disc_user = user.disc_user
            if user.player_id:
                self.real_id = str(user.player_id)
            self.id = self.id or self.real_id

    def __repr__(self):
        id = f"'{str(self.id)}'" if self.id else "None"
        funcom_id = f"'{str(self.funcom_id)}'" if self.funcom_id else "None"
        steam_id = f"'{str(self.steam_id)}'" if self.steam_id else "None"
        disc_user = f"'{str(self.disc_user)}'" if self.disc_user else "None"
        return f"<Player(id={id}, funcom_id={funcom_id}, steam_id={steam_id}, disc_user={disc_user})>"

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def funcom_id(self):
        return self._funcom_id

    @funcom_id.setter
    def funcom_id(self, value):
        self._funcom_id = value

    @property
    def steam_id(self):
        return self._steam_id

    @steam_id.setter
    def steam_id(self, value):
        if value is None:
            self._steam_id = None
            return
        if not value is str:
            try:
                value = str(value)
            except:
                raise ValueError("steam_id must be a string.")
        if not value:
            raise ValueError("Missing argument steam_id")
        if not value.isnumeric():
            raise ValueError("steam_id may only contain numeric characterrs.")
        if not len(value) == 17:
            raise ValueError("steam_id must have 17 digits.")
        self._steam_id = value
        return

    @property
    def disc_user(self):
        return self._disc_user

    @disc_user.setter
    def disc_user(self, value):
        self._disc_user = value

    @property
    def characters(self):
        return self._characters

    @characters.setter
    def characters(self, characters):
        self._characters = characters

class Owner:
    def is_inactive(self, td):
        return self.last_login < datetime.utcnow() - td

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

# db-classes
class Account(GameBase):
    __tablename__ = 'account'
    __table_args__ = {'autoload': True}
    __bind_key__ = 'gamedb'
    player_id = Column('id', Text, ForeignKey('characters.playerId'), primary_key=True, nullable=False)
    funcom_id = Column('user', Text, ForeignKey('steam64.id'), nullable=False)
    # relationship
    _character = relationship('Characters', back_populates='_account')
    # relationship('Characters', backref="account")

    @property
    def characters(self):
        query = session.query(Characters) \
                       .filter(Characters.player_id.like((str(self._character.player_id) + '#_')) |
                              (Characters.player_id==self._character.player_id)) \
                       .order_by(Characters.player_id)
        return tuple(c for c in query.all())

    @property
    def user(self):
        query = session.query(Users).filter((Users.funcom_id==self.funcom_id) | (Users.player_id==self.player_id))
        return query.first()

    @property
    def player(self):
        return Player(id=self.player_id, funcom_id=self.funcom_id)

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
    owner_id = Column('owner', Integer, ForeignKey('characters.id'))
    # relationships
    owner = relationship('Characters', foreign_keys=[owner_id])

    @property
    def members(self):
        return CharList(self._members)

    @property
    def last_login(self):
        return CharList(self._members).last_to_login().last_login

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

    @property
    def player(self):
        return Player(id=self.player_id)

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

class Steam64(GameBase):
    __tablename__ = 'steam64'
    __bind_key__ = 'gamedb'

    id = Column(Text, primary_key=True, nullable=False)
    funcom_id = Column('user_id', Text, nullable=False)
    # relationship
    account = relationship('Account', backref="steam64", uselist=False)

    def __repr__(self):
        return f"<Steam64(id='{self.id}', funcom_id='{self.funcom_id}')>"

class Users(UsersBase):
    __tablename__ = 'users'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    steam_id = Column('SteamID64', String(17), unique=True, nullable=False)
    disc_user = Column(String, unique=True, nullable=False)
    disc_id = Column(String(18), unique=True)
    funcom_id = Column(String(16), unique=True)
    player_id = Column(String, unique=True)

    def __repr__(self):
        steam_id = f"'{str(self.steam_id)}'" if self.steam_id else "None"
        disc_user = f"'{str(self.disc_user)}'" if self.disc_user else "None"
        disc_id = f"'{str(self.disc_id)}'" if self.disc_id else "None"
        funcom_id = f"'{str(self.funcom_id)}'" if self.funcom_id else "None"
        player_id = f"'{str(self.player_id)}'" if self.player_id else "None"
        return f"<Users(id={self.id}, steam_id={steam_id}, disc_user={disc_user}, disc_id={disc_id}, funcom_id={funcom_id}, player_id={player_id})>"

    @property
    def characters(self):
        if self.funcom_id or self.player_id:
            return Player(id=self.player_id, funcom_id=self.funcom_id).characters
        # characters that have not logged in since the patch still have their steam_id in place of the funcom_id
        account = session.query(Account).filter_by(funcom_id=self.steam_id).first()
        if account:
            return account.characters

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
    def update():
        owners = cache = {}
        results = session.query(Guilds.id, Guilds.name).filter(Guilds.name!='Ruins').all()
        owners = {owner[0]: owner[1] for owner in results}
        results = session.query(Characters.id, Characters.name).filter(Characters.name!='Ruins').all()
        owners.update({owner[0]: owner[1] for owner in results})
        cache = {owner.id: owner.name for owner in session.query(OwnersCache).all()}
        if not 0 in owners:
            owners[0] = 'Game Assets'
        if not 11 in owners:
            owners[RUINS_CLAN_ID] = 'Ruins'
        for id, name in owners.items():
            if not id in cache:
                new_owner = OwnersCache(id=id, name=name)
                session.add(new_owner)
            elif cache[id] != owners[id]:
                changed_owner = session.query(OwnersCache).get(id)
                changed_owner.name = name
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
    def update():
        objects = cache = {}
        sqGuilds = session.query(Guilds.id)
        sqChars = session.query(Characters.id)
        objects = {obj[0] for obj in session.query(Buildings.object_id).filter(
            Buildings.owner_id.notin_(sqChars) &
            Buildings.owner_id.notin_(sqGuilds) &
            (Buildings.owner_id != 0) |
            (Buildings.owner_id == RUINS_CLAN_ID)).all()}
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

    id = Column(Integer, primary_key=True, nullable=False)
    applicant = Column(String, unique=True, nullable=False)
    status = Column(String, nullable=False)
    steam_id_row = Column(Integer)
    current_question = Column(Integer)
    open_date = Column(DateTime)

    def __repr__(self):
        return f"<Applications(id={self.id}, applicant='{self.applicant}', status='{self.status}')>"

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
    has_steam_id = Column('has_steamID', Boolean, default=False)

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
