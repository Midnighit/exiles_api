import os, json, warnings
from statistics import median, mean
from config import *
from math import floor, ceil, sqrt
from struct import pack, unpack
from time import sleep
from datetime import datetime, date, timedelta, time
from sqlalchemy.orm import sessionmaker, Session, relationship, backref
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, literal, desc, MetaData, Table, exc as sa_exc
from sqlalchemy import Column, ForeignKey, func, distinct, Text, Integer, String, Float, DateTime, Boolean, Interval

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

RANKS = ('Recruit', 'Member', 'Officer', 'Guildmaster')
ITER = (list, tuple, set)
NUMBER = (int, float)

def db_date():
    now = datetime.utcnow()
    for c in session.query(Characters).order_by(desc(Characters._last_login)).all():
        if c.last_login < now:
            return c.last_login
    return None

def make_instance_db(source_db="game.db", dest_db="dest.db", owner_ids=None, inverse_owners=False, loc=None, with_chars=True, with_alts=True, inverse_mods=False, mod_names=None):
    if owner_ids:
        guild_ids = []
        char_ids = []
        for owner_id in owner_ids:
            if session.query(Characters).get(owner_id):
                char_ids.append(owner_id)
            elif session.query(Guilds).get(owner_id):
                guild_ids.append(owner_id)

    print("Copying mods...")
    Mods.copy(source_db, dest_db, mod_names, inverse_mods)
    print("Copying buildings...")
    Buildings.copy(source_db, dest_db, owner_ids, loc, inverse_owners)
    print("Copying guilds...")
    Guilds.copy(source_db, dest_db, guild_ids, with_chars, with_alts, inverse_owners)
    print("Copying characters...")
    Characters.copy(source_db, dest_db, char_ids, with_alts, inverse_owners)

def next_time(value):
    weekdays = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
    now = datetime.utcnow()
    d, t = value.split()
    hour, minute = t.split(':')
    due_time = time(hour=int(hour), minute=int(minute))
    today = datetime.combine(now, due_time)
    days_ahead = (weekdays[d] - today.weekday()) % 7
    if days_ahead == 0 and now.time() > due_time:
        days_ahead = 7
    return today + timedelta(days=days_ahead)

def iter2str(value):
    if not isinstance(value, ITER):
        return value
    if len(value) == 1 and isinstance(value, tuple):
        return str(value)[1:-2]
    else:
        return str(value)[1:-1]

# non-db classes
class ChatLogs:
    def __init__(self, path, after_date=None):
        self.path = path
        self.files, self.pippi = [], []
        # add one second to account for lost split seconds in the log
        self.after_date = after_date + timedelta(seconds=1) if after_date else None
        filelist = os.listdir(path)
        filelist.sort(reverse=True)
        for filename in filelist:
            if filename.startswith('ConanSandbox'):
                if filename.startswith('ConanSandbox-backup'):
                    date = datetime.strptime(filename[20:39], '%Y.%m.%d-%H.%M.%S')
                    if not after_date or date > after_date:
                        self.files.append({'name': filename, 'date': date})
                else:
                    date = datetime.utcfromtimestamp(os.path.getmtime(os.path.join(path, filename)))
                    if not after_date or date > after_date:
                        self.files.insert(0, {'name': filename, 'date': date})
            if filename.startswith('PippiCommands'):
                if filename.startswith('PippiCommands-backup'):
                    date = datetime.strptime(filename[21:40], '%Y.%m.%d-%H.%M.%S')
                    if not after_date or date > after_date:
                        self.pippi.append({'name': filename, 'date': date})
                else:
                    date = datetime.utcfromtimestamp(os.path.getmtime(os.path.join(path, filename)))
                    if not after_date or date > after_date:
                        self.pippi.insert(0, {'name': filename, 'date': date})

    def get_lines(self, after_date=None):
        self.chat_lines, normal_lines, special_lines = [], [], []
        after_date = after_date + timedelta(seconds=1) if after_date else self.after_date
        # iterate through files from oldest to newest
        for file in sorted(self.files, key=lambda item: item['date']):
            if not after_date or file['date'] > after_date:
                filename = os.path.join(self.path, file['name'])
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines()
                for line in lines[:-1]:
                    if line[0] == '[':
                        date = self.get_date(line)
                        if (not after_date or date > after_date) and '[Pippi]PippiChat: ' in line:
                            normal_lines.append(line)

        for file in sorted(self.pippi, key=lambda item: item['date']):
            if not after_date or file['date'] > after_date:
                filename = os.path.join(self.path, file['name'])
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines()
                for line in lines[:-1]:
                    if line[0] == '[':
                        date = self.get_date(line)
                        if (not after_date or date > after_date) and '[Pippi]PippiChat: ' in line:
                            special_lines.append(line)

        # merge normal and special lines keeping the temporal order intact
        normal_date = self.get_date(normal_lines[0]) if len(normal_lines) > 0 else None
        special_date = self.get_date(special_lines[0]) if len(special_lines) > 0 else None
        while True:
            # if either of the two list are empty append the other to the result and break out of the loop
            if len(normal_lines) == 0:
                self.chat_lines += special_lines
                break
            elif len(special_lines) == 0:
                self.chat_lines += normal_lines
                break
            # compare both dates, append the older one to the merge list and update it for the next comparison
            if normal_date <= special_date:
                self.chat_lines.append(normal_lines.pop(0))
                normal_date = self.get_date(normal_lines[0]) if len(normal_lines) > 0 else None
            else:
                self.chat_lines.append(special_lines.pop(0))
                special_date = self.get_date(special_lines[0]) if len(special_lines) > 0 else None

    def cycle_pippi(self, keep_files=3):
        while len(self.pippi) >= keep_files:
            oldest_file = self.pippi[-1]
            path = os.path.join(self.path, oldest_file['name'])
            counter = 1
            while True:
                try:
                    os.remove(path)
                    del self.pippi[-1]
                    break
                except Exception as exc:
                    print(f"Failed attempt {counter} to delete {path}.\n{str(exc)}\nTrying again in 1 second.")
                    if counter < 5:
                        counter += 1
                        sleep(1)
                    else:
                        return False

        # if the script reaches this point there should be only keep_files - 1 files left
        # rename the youngest one (without a date in its filename) to -backup-date
        src_path = os.path.join(self.path, 'PippiCommands.log')
        last_edit = datetime.utcfromtimestamp(os.path.getmtime(os.path.join(src_path))).strftime('%Y.%m.%d-%H.%M.%S')
        dst_path = src_path[:-4] + '-backup-' + last_edit + '.log'
        while True:
            try:
                os.rename(src_path, dst_path)
                self.pippi[0]['name'] = 'PippiCommands-backup-' + last_edit + '.log'
                break
            except Exception as exc:
                print(f"Failed attempt {counter} to rename {src_path} to {dst_path}.\n{str(exc)}\nTrying again in 1 second.")
                if counter < 5:
                    counter += 1
                    sleep(1)
                else:
                    return False
        # append a log file closed line to the newly backuped log
        now = datetime.utcnow()
        log_date_fmt1 = now.strftime('%m/%d/%y %H:%M:%S')
        log_date_fmt2 = now.strftime('%Y.%m.%d-%H.%M.%S:%f')[:-3]
        try:
            with open(dst_path, 'a', encoding='utf-8-sig') as f:
                f.write(f"[{log_date_fmt2}]Log file closed, {log_date_fmt1}\n")
        except Exception as exc:
            print(f"Failed to edit PippiCommands.log-backup-{last_edit}.log'.\n{str(exc)}")
            return False

        try:
            with open(src_path, 'a', encoding='utf-8-sig') as f:
                f.write(f"Log file open, {log_date_fmt1}\n")
        except Exception as exc:
            print(f"Failed to create PippiCommands.log.\n{str(exc)}")
            return False

        return True

    @staticmethod
    def get_date(line):
        try:
            return datetime.strptime(line[1:24], '%Y.%m.%d-%H.%M.%S:%f')
        except:
            return None

    @staticmethod
    def get_chat_info(line, date_format=None):
        """
        Example Chat
        [2000.01.01-12.00.00:000][Pippi]PippiChat: Alice said in channel [Alice:Bob]: Hello Bob!
                                                   ^div_1                          ^div_2
        """
        if not '[Pippi]PippiChat: ' in line:
            return (None, None, None, None, None)
        div_1 = 43
        div_2 = line.find(']', div_1)
        date = ChatLogs.get_date(line)
        sender, channel = line[div_1:div_2].split(" said in channel [")
        if ':' in channel:
            sender, recipient = channel.split(':')
            channel = 'Whisper'
        elif channel not in ('Global', 'Local', 'Emote', 'Shout', 'Mumble'):
            recipient = channel
            channel = 'Guild'
        else:
            recipient = ''
        chat = line[div_2+3:-1]
        if '"' in chat:
            chat = chat.replace('"', "'")
        for c in ';\n\r':
            if c in chat:
                chat = chat.replace(c, '')
        if date_format:
            return (date.strftime(date_format), sender, recipient, channel, chat)
        else:
            return (date, sender, recipient, channel, chat)

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

    @staticmethod
    def get_by_name(owner_name, strict=True, nocase=False, include_chars=True, include_guilds=True):
        chars, guilds = [], []
        if include_guilds:
            if strict and not nocase:
                guilds = session.query(Guilds).filter_by(name=owner_name).all()
            elif strict and nocase:
                guilds = session.query(Guilds).filter(Guilds.name.collate('NOCASE') == owner_name).all()
            else:
                guilds = session.query(Guilds).filter(Guilds.name.like('%' + owner_name + '%')).all()
        if include_chars:
            if strict and not nocase:
                chars = session.query(Characters).filter_by(name=owner_name).all()
            elif strict and nocase:
                chars = session.query(Characters).filter(Characters.name.collate('NOCASE') == owner_name).all()
            else:
                chars = session.query(Characters).filter(Characters.name.like('%' + owner_name + '%')).all()
        return chars + guilds

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
        if not isinstance(object_ids, ITER):
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
        if not isinstance(object_ids, ITER):
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
    @staticmethod
    def get_tiles_by_owner(bMult=1, pMult=1, do_round=True):
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
    def _get_guilds_query(threshold, only_with_buildings=True):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            C = Characters
            G = Guilds
            subquery1 = session.query(C.guild_id).filter(C.guild_id != None).subquery()
            subquery2 = session.query(Buildings.owner_id).subquery()
            query = G.id, G.name, literal(0).label("members"), literal(0).label("last_login")
            filter = G.id.notin_(subquery1), G.id.in_(subquery2) if only_with_buildings else G.id.notin_(subquery1),
            empty_guilds = session.query(*query).filter(*filter)
            query = C.guild_id, G.name, func.count(C.guild_id), C._last_login
            if only_with_buildings:
                filter = C.guild_id!=None, C._last_login>=threshold, G.id==C.guild_id, C.guild_id.in_(subquery2)
            else:
                filter = C.guild_id!=None, C._last_login>=threshold, G.id==C.guild_id
            populated_guilds = session.query(*query).filter(*filter).group_by(C.guild_id)
            return empty_guilds.union(populated_guilds)

    @staticmethod
    def _get_chars_query(only_with_buildings=True):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            if only_with_buildings:
                subquery = session.query(Buildings.owner_id).subquery()
                return session.query(Characters.id, Characters.name, Characters._last_login). \
                               filter(Characters.id.in_(subquery))
            else:
                return session.query(Characters.id, Characters.name, Characters._last_login).filter_by(guild_id=None)

    @classmethod
    def get_members(cls, td=None, d=datetime.utcnow(), buildings=True):
        members = {}
        threshold = int((d - td).timestamp()) if td is not None else 0
        owners = set()
        for g in cls._get_guilds_query(0, buildings).all():
            owners.add(g[0])
            members[g[0]] = {'name': g[1], 'numMembers': g[2], 'numActiveMembers': g[2]}
        for c in cls._get_chars_query(buildings).all():
            numActiveMembers = 1 if c[2] >= threshold else 0
            members[c[0]] = {'name': c[1], 'numMembers': 1, 'numActiveMembers': numActiveMembers}
        if td is None:
            return members
        for g in cls._get_guilds_query(threshold, buildings):
            owners.remove(g[0])
            members[g[0]]['numActiveMembers'] = g[2]
        for g in owners:
            members[g]['numActiveMembers'] = 0
        return members

class Mods:
    @staticmethod
    def copy(source_db="game.db", dest_db="dest.db", mod_names=None, inverse=False):
        # confirm that source and destination files exist
        if not (os.path.isfile(SAVED_DIR_PATH + '/' + source_db) and os.path.isfile(SAVED_DIR_PATH + '/' + dest_db)):
            print("Either source or destination DB file don't exist in saved folder.")
            return None

        # Try to get engine for the destination db
        try:
            dest_db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + dest_db
            engine = create_engine(dest_db_uri, echo=ECHO)
        except:
            print(f"Couldn't open destination DB at {dest_db_uri}.")
            return None

        # if no mod_names were given, all mods are selected
        if not mod_names or (not isinstance(mod_names, str) and not isinstance(mod_names, ITER)):
            # if inverse is False, all mods are copied
            if not inverse:
                obj_ids = (
                    "WHERE (((class LIKE '/Game/Mods/%' OR class LIKE '/Game/DLC/%') "
                    "AND x=0 AND y=0 AND z=0 AND rx=0 AND ry=0 AND rz=0 AND rw=1) "
                    "OR id IN (SELECT id FROM static_buildables)"
                )
            # if inverse is True, no mods are copied
            else:
                return None
        # if mod_names were given, those mods are selected
        else:
            # if inverse is False, exactly the given mods are copied
            if not inverse:
                mod_expr = f"= '{mod_names}'" if isinstance(mod_names, str) else f" IN ({iter2str(mod_names)})"
            # if inverse is True, all mods except the ones selected are copied
            else:
                mod_expr = f"!= '{mod_names}'" if isinstance(mod_names, str) else f" NOT IN ({iter2str(mod_names)})"
            obj_ids = (
                f"WHERE (((class LIKE '/Game/Mods/%' AND SUBSTR(class, 12, INSTR(SUBSTR(class, 12), '/')-1) {mod_expr}) "
                 "OR class LIKE '/Game/DLC/%') AND x=0 AND y=0 AND z=0 AND rx=0 AND ry=0 AND rz=0 AND rw=1) "
                 "OR id IN (SELECT id FROM static_buildables)"
            )

        slf, wobi, wii, sifa = "SELECT * FROM", "WHERE object_id IN", "WHERE id IN", "SELECT id FROM src.actor_position"
        source_db_path = SAVED_DIR_PATH + '/' + source_db
        with engine.begin() as conn:
            conn.execute(f"ATTACH DATABASE '{source_db_path}' AS 'src'")
            # Delete conflicting objects in the destination db if they exist
            conn.execute(f"DELETE FROM actor_position {obj_ids}")
            conn.execute(f"DELETE FROM mod_controllers {wii} ({sifa} {obj_ids})")
            conn.execute(f"DELETE FROM properties {wobi} ({sifa} {obj_ids})")
            # copy the objects from the source db into the destination db
            conn.execute(f"REPLACE INTO actor_position {slf} src.actor_position {obj_ids}")
            conn.execute(f"REPLACE INTO mod_controllers {slf} src.mod_controllers {wii} ({sifa} {obj_ids})")
            conn.execute(f"REPLACE INTO properties {slf} src.properties {wobi} ({sifa} {obj_ids})")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

    @staticmethod
    def delete(db="game.db", mod_names=None, inverse=False):
        # confirm that source and destination files exist
        if not (os.path.isfile(SAVED_DIR_PATH + '/' + db)):
            print("Either source or destination DB file don't exist in saved folder.")
            return None

        # Try to get engine for the destination db
        try:
            db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + db
            engine = create_engine(db_uri, echo=ECHO)
        except:
            print(f"Couldn't open destination DB at {db_uri}.")
            return None

        # if no mod_names were given, all mods are selected
        if not mod_names or (not isinstance(mod_names, str) and not isinstance(mod_names, ITER)):
            # if inverse is False, all mods are copied
            if not inverse:
                obj_ids = "WHERE class LIKE '/Game/Mods/%' AND x=0 AND y=0 AND z=0 AND rz=0 AND rw=1"
            # if inverse is True, no mods are copied
            else:
                return None
        # if mod_names were given, those mods are selected
        else:
            # if inverse is False, exactly the given mods are copied
            if not inverse:
                mod_expr = f"= '{mod_names}'" if isinstance(mod_names, str) else f" IN ({iter2str(mod_names)})"
            # if inverse is True, all mods except the ones selected are copied
            else:
                mod_expr = f"!= '{mod_names}'" if isinstance(mod_names, str) else f" NOT IN ({iter2str(mod_names)})"
            obj_ids = (
                 "WHERE class LIKE '/Game/Mods/%' AND x=0 AND y=0 AND z=0 AND rz=0 AND rw=1 "
                f"AND SUBSTR(class, 12, INSTR(SUBSTR(class, 12), '/') - 1) {mod_expr}"
            )

        with engine.begin() as conn:
            conn.execute(f"DELETE FROM properties WHERE object_id IN (SELECT id FROM actor_position {obj_ids})")
            conn.execute(f"DELETE FROM mod_controllers WHERE id IN  (SELECT id FROM actor_position {obj_ids})")
            conn.execute(f"DELETE FROM actor_position {obj_ids}")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

class Stats:
    @staticmethod
    def get_tile_statistics(td=timedelta(days=0), d = None, full=False):
        # timestamp threshold is set based on the the timedelta given and the age of the db
        if not d:
            d = db_date()
        threshold = 0 if not td else int((d - td).timestamp())
        C = Characters
        G = Guilds
        members = MembersManager.get_members(td, d, False)
        # stores all tiles indexed by their respective owners
        building_tiles, placeables, tiles = {}, {}, {}
        # tiles that have an object_id in both Buildings and BuildingInstances are root object building tiles
        root = set()
        # res has format (object_id, owner_id, count(object_id)) contains only building tiles and their aggregated objects
        for res in session.query(Buildings.object_id, Buildings.owner_id, func.count(Buildings.object_id)) \
                          .filter(Buildings.object_id==BuildingInstances.object_id) \
                          .group_by(Buildings.object_id).all():
            # create a new dict entry if owner does not have one yet
            if not res[1] in tiles:
                tiles[res[1]] = res[2]
                building_tiles[res[1]] = res[2]
            # add aggregated tiles if one already exists
            else:
                tiles[res[1]] += res[2]
                building_tiles[res[1]] += res[2]
            # remember the object_id as root object in either case
            root.add(res[0])

        # res has format: (object_id, owner_id) contains all building tiles and placeables
        for res in session.query(Buildings.object_id, Buildings.owner_id).filter(Buildings.object_id==ActorPosition.id).all():
            # if object is not a root object, it is a placeable and needs to be added now
            if not res[0] in root:
                # create a new dict entry if owner does not have one yet
                if not res[1] in placeables:
                    placeables[res[1]] = 1
                # otherwise add it to the count
                else:
                    placeables[res[1]] += 1
                # same thing for the general tiles dict
                if not res[1] in tiles:
                    tiles[res[1]] = 1
                else:
                    tiles[res[1]] += 1

        nm, nam = 'numMembers', 'numActiveMembers'

        # active guilds with more than one member
        active_guilds = dict(filter(lambda m: m[1][nm] > 1 and m[1][nam] >= 1, members.items()))
        tiles_active_guilds = {id: tiles[id] if id in tiles else 0 for id in active_guilds}

        # inactive guilds with more than one member
        inactive_guilds = dict(filter(lambda m: m[1][nm] > 1 and m[1][nam] == 0, members.items()))
        tiles_inactive_guilds = {id: tiles[id] if id in tiles else 0 for id in inactive_guilds}

        # active characters without guilds or in guilds with one member only
        active_chars_no_guild = dict(filter(lambda m: m[1][nm] == 1 and m[1][nam] == 1, members.items()))
        tiles_active_chars_no_guild = {id: tiles[id] if id in tiles else 0 for id in active_chars_no_guild}

        # inactive characters without guilds or in guilds with one member only
        inactive_chars_no_guild = dict(filter(lambda m: m[1][nm] == 1 and m[1][nam] == 0, members.items()))
        tiles_inactive_chars_no_guild = {id: tiles[id] if id in tiles else 0 for id in inactive_chars_no_guild}

        # active characters regardless of guild status
        active_chars = dict(filter(lambda m: m[1][nam] >= 1, members.items()))
        tiles_active_chars = {id: tiles[id] if id in tiles else 0 for id in active_chars}

        # inactive characters regardless of guild status
        inactive_chars = {id: 1 for id, in session.query(C.id).filter(C._last_login<=threshold).all()}
        tiles_inactive_chars = {id: tiles[id] if id in tiles else 0 for id in inactive_chars}

        # single characters or guilds with one member named ruins
        ruin_chars_no_guild = dict(filter(lambda m: m[1][nm] == 1 and m[1]['name'] == "Ruins", members.items()))
        tiles_ruin_chars_no_guild = {id: tiles[id] if id in tiles else 0 for id in ruin_chars_no_guild}

        # guilds with more than one member named ruins
        ruin_chars_guild = dict(filter(lambda m: m[1][nm] > 1 and m[1]['name'] == "Ruins", members.items()))
        tiles_ruin_chars_guild = {id: tiles[id] if id in tiles else 0 for id in ruin_chars_guild}

        # ruins regardless of whether they're owned by a guild or a character
        ruins = dict(filter(lambda m: m[1]['name'] == "Ruins", members.items()))
        tiles_ruins = {id: tiles[id] if id in tiles else 0 for id in ruins}

        # tiles that have no owner
        tiles_no_owner = {id: tiles[id] for id in tiles if id not in members}
        threshold24h = (d - timedelta(hours=24)).timestamp()

        # set statistics list
        s = {}
        # iterables
        if full:
            s['tiles'] = tiles
            s['buildingTiles'] = building_tiles
            s['placeables'] = placeables
            s['allChars'] = members
            s['activeGuilds'] = active_guilds
            s['tilesActiveGuilds'] = tiles_active_guilds
            s['inactiveGuilds'] = inactive_guilds
            s['tilesInactiveGuilds'] = tiles_inactive_guilds
            s['activeCharsNoGuild'] = active_chars_no_guild
            s['tilesActiveCharsNoGuild'] = tiles_active_chars_no_guild
            s['inactiveCharsNoGuilds'] = inactive_chars_no_guild
            s['tilesInactiveCharsNoGuild'] = tiles_inactive_chars_no_guild
            s['ruinCharsNoGuild'] = ruin_chars_no_guild
            s['tilesRuinCharsNoGuild'] = tiles_ruin_chars_no_guild
            s['ruinCharsGuild'] = ruin_chars_guild
            s['tilesRuinCharsGuild'] = tiles_ruin_chars_guild
            s['ruins'] = ruins
            s['tilesRuins'] = tiles_ruins
            s['activeChars'] = active_chars
            s['tilesActiveChars'] = tiles_active_chars
            s['inactiveChars'] = inactive_chars
            s['tilesInactiveChars'] = tiles_inactive_chars
            s['tilesNoOwner'] = tiles_no_owner
            s['logins'] = session.query(C).filter(C._last_login>=threshold24h).all()
        # numbers
        s['dbDate'] = d
        s['numTiles'] = sum(building_tiles.values()) + sum(placeables.values())
        s['numBuildingTiles'] = sum(building_tiles.values())
        s['numPlaceables'] = sum(placeables.values())
        s['numActiveGuilds'] = len(active_guilds)
        s['numActiveCharsInActiveGuilds'] = sum([d[nam] for d in active_guilds.values()])
        s['numInactiveCharsInActiveGuilds'] = sum([d[nm] - d[nam] for d in active_guilds.values()])
        s['numCharsInActiveGuilds'] = s['numActiveCharsInActiveGuilds'] + s['numInactiveCharsInActiveGuilds']
        s['numTilesActiveGuilds'] = sum(tiles_active_guilds.values())
        s['numInactiveGuilds'] = len(inactive_guilds)
        s['numCharsInInactiveGuilds'] = sum([d[nm] for d in inactive_guilds.values()])
        s['numTilesInactiveGuilds'] = sum(tiles_inactive_guilds.values())
        s['numGuilds'] = s['numActiveGuilds'] + s['numInactiveGuilds']
        s['numTilesGuilds'] = s['numTilesActiveGuilds'] + s['numTilesInactiveGuilds']
        s['numActiveCharsNoGuild'] = len(active_chars_no_guild)
        s['numTilesActiveCharsNoGuild'] = sum(tiles_active_chars_no_guild.values())
        s['numInactiveCharsNoGuild'] = len(inactive_chars_no_guild)
        s['numTilesInactiveCharsNoGuild'] = sum(tiles_inactive_chars_no_guild.values())
        s['numCharsNoGuild'] = s['numActiveCharsNoGuild'] + s['numInactiveCharsNoGuild']
        s['numTilesCharsNoGuild'] = s['numTilesActiveCharsNoGuild'] + s['numTilesInactiveCharsNoGuild']
        s['numActiveChars'] = sum([d[nam] for d in active_chars.values()])
        s['numTilesActiveChars'] = sum(tiles_active_chars.values())
        s['numInactiveChars'] = len(inactive_chars)
        s['numTilesInactiveChars'] = sum(tiles_inactive_chars.values())
        s['numChars'] = s['numActiveChars'] + s['numInactiveChars']
        s['numTilesNoOwner'] = sum(tiles_no_owner.values())
        s['numLogins'] = session.query(func.count(C.id)).filter(C._last_login>=threshold24h).scalar()
        s['numRuinCharsNoGuild'] = len(ruin_chars_no_guild)
        s['numTilesRuinCharsNoGuild'] = sum(tiles_ruin_chars_no_guild.values())
        s['numRuinCharsGuild'] = len(ruin_chars_guild)
        s['numTilesRuinCharsGuild'] = sum(tiles_ruin_chars_guild.values())
        s['numRuins'] = len(ruins)
        s['numTilesRuins'] = sum(tiles_ruins.values())
        s['meanTilesActiveGuilds'] = mean(tiles_active_guilds.values()) if tiles_active_guilds else None
        s['medianTilesActiveGuilds'] = median(tiles_active_guilds.values()) if tiles_active_guilds else None
        s['meanTilesInactiveGuilds'] = mean(tiles_inactive_guilds.values()) if tiles_inactive_guilds else None
        s['medianTilesInactiveGuilds'] = median(tiles_inactive_guilds.values()) if tiles_inactive_guilds else None
        s['meanTilesActiveCharsNoGuild'] = mean(tiles_active_chars_no_guild.values()) if tiles_active_chars_no_guild else None
        s['medianTilesActiveCharsNoGuild'] = median(tiles_active_chars_no_guild.values()) if tiles_active_chars_no_guild else None
        s['meanTilesInactiveCharsNoGuild'] = mean(tiles_inactive_chars_no_guild.values()) if tiles_inactive_chars_no_guild else None
        s['medianTilesInactiveCharsNoGuild'] = median(tiles_inactive_chars_no_guild.values()) if tiles_inactive_chars_no_guild else None
        s['meanTilesActiveChars'] = mean(tiles_active_chars.values()) if tiles_active_chars else None
        s['medianTilesActiveChars'] = median(tiles_active_chars.values()) if tiles_active_chars else None
        s['meanTilesInactiveChars'] = mean(tiles_inactive_chars.values()) if tiles_inactive_chars else None
        s['medianTilesInactiveChars'] = median(tiles_inactive_chars.values()) if tiles_inactive_chars else None
        return s

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
    def _verify_loc(loc):
        # no location info is correct in that it means no filter is applied
        if loc is None:
            return True
        # checks if loc is a list or tuple and has either two (x and y) or three (x, y and z) elements
        if not isinstance(loc, ITER) or len(loc) < 2 or len(loc) > 3:
            return False
        # check if each element contained in loc has exactly two (min and max) numbers
        for c in loc:
            if not isinstance(loc, ITER) or len(c) != 2 or not isinstance(c[0], NUMBER) or not isinstance(c[1], NUMBER):
                return False
            if  c[0] > c[1]:
                return False
        return True

    @staticmethod
    def _get_objects_query(owner_ids=None, loc=None, inverse=False, attach=None):
        attach = attach + '.' if attach else ''
        # The SELECT clause is always the same
        query_list = ["SELECT object_id"]
        # The FROM clause only has to include actor_position if a location was given
        if loc:
            query_list.append(f"FROM {attach}actor_position, {attach}buildings")
        else:
            query_list.append(f"FROM {attach}buildings")
        # Create the WHERE clause depending on loc, owner_id and inverse
        where_list = ["id = object_id"] if loc else []
        if loc and len(loc) == 2:
            x, y = loc
            if not inverse:
                where_list.append(
                    f"x BETWEEN {x[0]} AND {x[1]} AND "
                    f"y BETWEEN {y[0]} AND {y[1]}"
                )
            else:
                where_list.append(
                    f"NOT (x BETWEEN {x[0]} AND {x[1]} AND "
                    f"     y BETWEEN {y[0]} AND {y[1]})"
                )

        elif loc and len(loc) == 3:
            x, y, z = loc
            if not inverse:
                where_list.append(
                    f"x BETWEEN {x[0]} AND {x[1]} AND "
                    f"y BETWEEN {y[0]} AND {y[1]} AND "
                    f"z BETWEEN {z[0]} AND {z[1]}"
                )
            else:
                where_list.append(
                    f"NOT (x BETWEEN {x[0]} AND {x[1]} AND "
                    f"     y BETWEEN {y[0]} AND {y[1]} AND "
                    f"     z BETWEEN {z[0]} AND {z[1]})"
                )
        if owner_ids:
            o_ids = owner_ids if not isinstance(owner_ids, ITER) else iter2str(owner_ids)
            if not inverse:
                where_list.append(f"owner_id IN ({o_ids})")
            else:
                where_list.append(f"owner_id NOT IN ({o_ids}, 0)")
        else:
            # avoid deleting the game internal buildings belonging to owner_id 0
            where_list.append("owner_id != 0")
        if len(where_list) > 0:
            query_list.append("WHERE " + " AND ".join(where_list))
        return ' '.join(query_list)

    @staticmethod
    def _get_objects_filter(owner_ids=None, loc=None, inverse=False):
        owner_filter = None
        if owner_ids:
            if not inverse:
                if isinstance(owner_ids, int):
                    owner_filter = (Buildings.owner_id == owner_ids)
                elif isinstance(owner_ids, ITER):
                    owner_filter = (Buildings.owner_id.in_(owner_ids))
            else:
                if isinstance(owner_ids, int):
                    owner_filter = (Buildings.owner_id != owner_ids)
                elif isinstance(owner_ids, ITER):
                    owner_filter = (Buildings.owner_id.notin_(owner_ids))

        loc_filter = None
        if loc:
            if len(loc) == 2:
                x, y = loc
                loc_filter = (
                    (ActorPosition.id == Buildings.object_id) &
                    (ActorPosition.x.between(x[0], x[1])) &
                    (ActorPosition.y.between(y[0], y[1]))
                )
            elif len(loc) == 3:
                x, y, z = loc
                loc_filter = (
                    (ActorPosition.id == Buildings.object_id) &
                    (ActorPosition.x.between(x[0], x[1])) &
                    (ActorPosition.y.between(y[0], y[1])) &
                    (ActorPosition.z.between(z[0], z[1]))
                )

        if owner_filter is not None and loc_filter is None:
            return owner_filter
        elif owner_filter is None and loc_filter is not None:
            return loc_filter
        elif owner_filter is not None and loc_filter is not None:
            return owner_filter & loc_filter
        else:
            return ()

    @staticmethod
    def copy(source_db="backup.db", dest_db="game.db", owner_ids=None, loc=None, inverse=False):
        # confirm that source and destination files exist
        if not (os.path.isfile(SAVED_DIR_PATH + '/' + source_db) and os.path.isfile(SAVED_DIR_PATH + '/' + dest_db)):
            print("Either source or destination DB file don't exist in saved folder.")
            return None

        # Try to get engine for the destination db
        try:
            dest_db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + dest_db
            engine = create_engine(dest_db_uri, echo=ECHO)
        except:
            print(f"Couldn't open destination DB at {dest_db_uri}.")
            return None

        # Ensure that, if a location is given, it is in the correct format
        if not Buildings._verify_loc(loc):
            print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
            print("loc:", loc)
            return None

        # generate the apropriate query with the information given
        obj_ids = Buildings._get_objects_query(owner_ids, loc, inverse, attach='src')

        if owner_ids:
            if not isinstance(owner_ids, ITER):
                owner_ids = (owner_ids, )
            thrall_id_list = []
            for owner_id in owner_ids:
                thrall_id = Properties.get_thrall_object_ids(owner_id=owner_id, strict=True)
                if thrall_id:
                    thrall_id_list += thrall_id
            thrall_id_list = map(str, thrall_id_list)
            if not inverse:
                thrall_ids = f"IN ({', '.join(thrall_id_list)})"
            else:
                thrall_ids = f"NOT IN ({', '.join(thrall_id_list)})"

        # do the actual copying
        slf, wobi, wowi = "SELECT * FROM", "WHERE object_id IN", "WHERE owner_id IN"
        source_db_path = SAVED_DIR_PATH + '/' + source_db
        with engine.begin() as conn:
            conn.execute(f"ATTACH DATABASE '{source_db_path}' AS 'src'")
            # Delete conflicting objects in the destination db if they exist
            conn.execute(f"DELETE FROM buildable_health {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"DELETE FROM building_instances {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"DELETE FROM destruction_history {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"DELETE FROM item_inventory {wowi} ({obj_ids}) OR owner_id {thrall_ids}")
            conn.execute(f"DELETE FROM item_properties {wowi} ({obj_ids}) OR owner_id {thrall_ids}")
            conn.execute(f"DELETE FROM properties {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"DELETE FROM actor_position WHERE id IN ({obj_ids}) OR id {thrall_ids}")
            conn.execute(f"DELETE FROM buildings {wobi} ({obj_ids})")
            # copy the objects from the source db into the destination db
            conn.execute(f"REPLACE INTO buildable_health {slf} src.buildable_health {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"REPLACE INTO building_instances {slf} src.building_instances {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"REPLACE INTO destruction_history {slf} src.destruction_history {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"REPLACE INTO item_inventory {slf} src.item_inventory {wowi} ({obj_ids}) OR owner_id {thrall_ids}")
            conn.execute(f"REPLACE INTO item_properties {slf} src.item_properties {wowi} ({obj_ids}) OR owner_id {thrall_ids}")
            conn.execute(f"REPLACE INTO properties {slf} src.properties {wobi} ({obj_ids}) OR object_id {thrall_ids}")
            conn.execute(f"REPLACE INTO actor_position {slf} src.actor_position WHERE id IN ({obj_ids}) OR id {thrall_ids}")
            conn.execute(f"REPLACE INTO buildings {slf} src.buildings {wobi} ({obj_ids})")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

    @staticmethod
    def delete(db="game.db", owner_ids=None, loc=None, inverse=False):
        # confirm that source and destination files exist
        if not os.path.isfile(SAVED_DIR_PATH + '/' + db):
            print("DB file doesn't exist in saved folder.")
            return None

        # Try to get engine for the db
        try:
            db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + db
            engine = create_engine(db_uri, echo=ECHO)
        except:
            print(f"Couldn't open DB at {db_uri}.")
            return None

        # Ensure that, if a location is given, it is in the correct format
        if not Buildings._verify_loc(loc):
            print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
            print("loc:", loc)
            return None

        # generate the apropriate query with the information given
        obj_ids = Buildings._get_objects_query(owner_ids, loc, inverse)

        # do the actual deleting
        with engine.begin() as conn:
            obj_ids_tt = "SELECT object_id FROM obj_ids"
            conn.execute(f"CREATE TEMPORARY TABLE obj_ids AS {obj_ids}")
            conn.execute(f"DELETE FROM buildable_health WHERE object_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM building_instances WHERE object_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM destruction_history WHERE object_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM item_inventory WHERE owner_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM item_properties WHERE owner_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM properties WHERE object_id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM actor_position WHERE id IN ({obj_ids_tt})")
            conn.execute(f"DELETE FROM buildings WHERE object_id IN ({obj_ids_tt})")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

    @staticmethod
    def give_to_owner(old_owner_id, new_owner_id, loc=None, autocommit=True):

        # Ensure that, if a location is given, it is in the correct format
        if not Buildings._verify_loc(loc):
            print("loc is in the wrong format. Needs to be ((x_min, x_max), (y_min, y_max), [(z_min, z_max)]).")
            print("loc:", loc)
            return None

        filter = Buildings._get_objects_filter(owner_ids=old_owner_id, loc=loc)
        if loc is not None:
            obj_ids = session.query(Buildings.object_id).filter(filter).scalar_subquery()
            session.query(Buildings).filter(Buildings.object_id.in_(obj_ids)) \
                .update({Buildings.owner_id: new_owner_id}, synchronize_session='fetch')
        else:
            session.query(Buildings).filter(filter) \
                .update({Buildings.owner_id: new_owner_id}, synchronize_session='fetch')

        if autocommit:
            session.commit()

    @staticmethod
    def restore_from_backup(owner_ids, loc=None, remove=True):
        # basically an alias for a specific utilisation of the copy method
        if remove:
            Buildings.delete(owner_ids=owner_ids)
        Buildings.copy(owner_ids=owner_ids, loc=loc)

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

    @staticmethod
    def copy(source_db="backup.db", dest_db="game.db", owner_ids=None, with_chars=False, with_alts=False, inverse=False):
        # copy without owner_ids means copy all guilds the inverse of that is no guilds
        if owner_ids is None and inverse:
            return None

        # generate an appropriate WHERE clause
        def owner_filter(key):
            slid = "SELECT guildId FROM src.guilds"
            if owner_ids:
                if not inverse:
                    if isinstance(owner_ids, int):
                        return f"WHERE {key}={owner_ids}"
                    elif isinstance(owner_ids, ITER):
                        return f"WHERE {key} IN ({iter2str(owner_ids)})"
                else:
                    if isinstance(owner_ids, int):
                        return f"WHERE {key}!={owner_ids} AND {key} IN ({sldid})"
                    elif isinstance(owner_ids, ITER):
                        return f"WHERE {key} NOT IN ({iter2str(owner_ids)}) AND {key} IN ({sldid})"
            else:
                return f"WHERE {key} IN ({sldid})"

        def char_filter(key):
            slcid = "SELECT id FROM src.characters"
            if owner_ids:
                if not inverse:
                    if isinstance(owner_ids, int):
                        return f"WHERE {key} IN ({slcid} WHERE guild={owner_ids}) OR {key} IN ({iter2str(char_ids)})"
                    elif isinstance(owner_ids, ITER):
                        return f"WHERE {key} IN ({slcid} WHERE guild IN ({iter2str(owner_ids)})) OR {key} IN ({iter2str(char_ids)})"
                else:
                    if isinstance(owner_ids, int):
                        return (f"WHERE {key} NOT IN ({slcid} WHERE (guild={owner_ids} OR {key} IN ({iter2str(char_ids)}))) "
                                f"AND {key} IN ({slcid} WHERE guild IS NOT NULL)")
                    elif isinstance(owner_ids, ITER):
                        return (f"WHERE {key} NOT IN ({slcid} WHERE guild IN ({iter2str(owner_ids)}) OR {key} IN ({iter2str(char_ids)})) "
                                f"AND {key} IN ({slcid} WHERE guild IS NOT NULL)")
            else:
                return f"WHERE {key} IN ({slcid} WHERE {key} IN ({iter2str(char_ids)}) AND guild IS NOT NULL)"

        # confirm that source and destination files exist
        if not (os.path.isfile(SAVED_DIR_PATH + '/' + source_db) and os.path.isfile(SAVED_DIR_PATH + '/' + dest_db)):
            print("Either source or destination DB file don't exist in saved folder.")
            return None

        # Try to get engine for the destination db
        try:
            dest_db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + dest_db
            engine = create_engine(dest_db_uri, echo=ECHO)
        except:
            print(f"Couldn't open destination DB at {dest_db_uri}.")
            return None

        # do the actual copying
        slf = "SELECT * FROM"
        acc_id = "CASE WHEN INSTR(playerId, '#') > 0 THEN SUBSTR(playerId, 1, LENGTH(playerId)-2) ELSE playerId END"
        source_db_path = SAVED_DIR_PATH + '/' + source_db
        with engine.begin() as conn:
            conn.execute(f"ATTACH DATABASE '{source_db_path}' AS 'src'")
            # Delete conflicting objects in the destination db if they exist
            conn.execute(f"DELETE FROM purgescores {owner_filter('purgeid')}")
            conn.execute(f"DELETE FROM guilds {owner_filter('guildId')}")
            # copy the objects from the source db into the destination db
            conn.execute(f"REPLACE INTO purgescores {slf} src.purgescores {owner_filter('purgeid')}")
            conn.execute(f"REPLACE INTO guilds {slf} src.guilds {owner_filter('guildId')}")
            # if with_chars is True copy the chars of copied guilds as well
            if with_chars:
                char_ids = []
                # Get the account ids (playerId) for all characters getting copied
                conn.execute("CREATE TEMPORARY TABLE acc AS "
                            f"SELECT DISTINCT {acc_id} FROM src.characters {char_filter('id')}")
                if with_alts:
                    query = conn.execute(f"SELECT id FROM src.characters WHERE {acc_id} IN ({slf} acc)")
                    char_ids = tuple(id for id, in query.all())
                conn.execute(f"DELETE FROM account WHERE id IN ({slf} acc)")
                conn.execute(f"DELETE FROM actor_position {char_filter('id')}")
                conn.execute(f"DELETE FROM character_stats {char_filter('char_id')}")
                conn.execute(f"DELETE FROM item_inventory {char_filter('owner_id')}")
                conn.execute(f"DELETE FROM item_properties {char_filter('owner_id')}")
                conn.execute(f"DELETE FROM properties {char_filter('object_id')}")
                conn.execute(f"DELETE FROM purgescores {char_filter('purgeid')}")
                conn.execute(f"DELETE FROM characters {char_filter('id')}")
                # copy the objects from the source db into the destination db
                conn.execute(f"REPLACE INTO account {slf} src.account WHERE id IN (SELECT * FROM acc)")
                conn.execute(f"REPLACE INTO actor_position {slf} src.actor_position {char_filter('id')}")
                conn.execute(f"REPLACE INTO character_stats {slf} src.character_stats {char_filter('char_id')}")
                conn.execute(f"REPLACE INTO item_inventory {slf} src.item_inventory {char_filter('owner_id')}")
                conn.execute(f"REPLACE INTO item_properties {slf} src.item_properties {char_filter('owner_id')}")
                conn.execute(f"REPLACE INTO properties {slf} src.properties {char_filter('object_id')}")
                conn.execute(f"REPLACE INTO purgescores {slf} src.purgescores {char_filter('purgeid')}")
                conn.execute(f"REPLACE INTO characters {slf} src.characters {char_filter('id')}")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

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
        if not isinstance(character_ids, ITER):
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
        session.query(Characters).filter(Characters.id.in_(character_ids)).delete(synchronize_session='fetch')
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
    def set_last_login(char_ids, date=None, autocommit=True):
        if not date:
            ts = floor(datetime.utcnow().timestamp())
        else:
            ts = floor(date.timestamp())
        if not isinstance(char_ids, ITER):
            char_ids = (char_ids,)
        for char_id in char_ids:
            char = session.query(Characters).get(char_id)
            if char and ts:
                char.last_login = ts
        if autocommit:
            session.commit()

    @staticmethod
    def copy(source_db="backup.db", dest_db="game.db", owner_ids=None, with_alts=False, inverse=False):
        # copy without owner_ids means copy all characters the inverse of that is no characters
        if owner_ids is None and inverse:
            return None

        # generate an appropriate WHERE clause
        def owner_filter(key):
            slid = "SELECT id FROM src.characters"
            if owner_ids:
                if not inverse:
                    if isinstance(owner_ids, int):
                        return f"WHERE {key}={owner_ids}"
                    elif isinstance(owner_ids, ITER):
                        return f"WHERE {key} IN ({iter2str(owner_ids)})"
                else:
                    if isinstance(owner_ids, int):
                        return f"WHERE {key}!={owner_ids} AND {key} IN ({slid})"
                    elif isinstance(owner_ids, ITER):
                        return f"WHERE {key} NOT IN ({iter2str(owner_ids)}) AND {key} IN ({slid})"
            else:
                return f"WHERE {key} IN ({slid})"

        # confirm that source and destination files exist
        if not (os.path.isfile(SAVED_DIR_PATH + '/' + source_db) and os.path.isfile(SAVED_DIR_PATH + '/' + dest_db)):
            print("Either source or destination DB file don't exist in saved folder.")
            return None

        # Try to get engine for the destination db
        try:
            dest_db_uri =  "sqlite:///" + SAVED_DIR_PATH + '/' + dest_db
            engine = create_engine(dest_db_uri, echo=ECHO)
        except:
            print(f"Couldn't open destination DB at {dest_db_uri}.")
            return None

        # do the actual copying
        slf = "SELECT * FROM"
        acc_id = "CASE WHEN INSTR(playerId, '#') > 0 THEN SUBSTR(playerId, 1, LENGTH(playerId)-2) ELSE playerId END"
        source_db_path = SAVED_DIR_PATH + '/' + source_db
        with engine.begin() as conn:
            conn.execute(f"ATTACH DATABASE '{source_db_path}' AS 'src'")
            # Get the account ids (playerId) for all characters getting copied
            conn.execute("CREATE TEMPORARY TABLE acc AS "
                        f"SELECT DISTINCT {acc_id} FROM src.characters {owner_filter('id')}")
            # Extend owner_ids to all chars with a matching account id
            if with_alts:
                query = conn.execute(f"SELECT id FROM src.characters WHERE {acc_id} IN ({slf} acc)")
                owner_ids = tuple(id for id, in query.all())
            # Delete conflicting objects in the destination db if they exist
            conn.execute(f"DELETE FROM account WHERE id IN ({slf} acc)")
            conn.execute(f"DELETE FROM actor_position {owner_filter('id')}")
            conn.execute(f"DELETE FROM character_stats {owner_filter('char_id')}")
            conn.execute(f"DELETE FROM item_inventory {owner_filter('owner_id')}")
            conn.execute(f"DELETE FROM item_properties {owner_filter('owner_id')}")
            conn.execute(f"DELETE FROM properties {owner_filter('object_id')}")
            conn.execute(f"DELETE FROM purgescores {owner_filter('purgeid')}")
            conn.execute(f"DELETE FROM characters {owner_filter('id')}")
            # copy the objects from the source db into the destination db
            conn.execute(f"REPLACE INTO account {slf} src.account WHERE id IN (SELECT * FROM acc)")
            conn.execute(f"REPLACE INTO actor_position {slf} src.actor_position {owner_filter('id')}")
            conn.execute(f"REPLACE INTO character_stats {slf} src.character_stats {owner_filter('char_id')}")
            conn.execute(f"REPLACE INTO item_inventory {slf} src.item_inventory {owner_filter('owner_id')}")
            conn.execute(f"REPLACE INTO item_properties {slf} src.item_properties {owner_filter('owner_id')}")
            conn.execute(f"REPLACE INTO properties {slf} src.properties {owner_filter('object_id')}")
            conn.execute(f"REPLACE INTO purgescores {slf} src.purgescores {owner_filter('purgeid')}")
            conn.execute(f"REPLACE INTO characters {slf} src.characters {owner_filter('id')}")

        with engine.begin() as conn:
            conn.execute("VACUUM")
        engine.dispose()

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
        if not isinstance(template_ids, ITER):
            template_ids = (template_ids,)
        session.query(ItemInventory).filter(ItemInventory.template_id.in_(template_ids)).delete(synchronize_session='fetch')
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
        if ("PetName" in p.name or "ThrallName" in p.name) and len(p.value) >= 21:
            # the codec type (negative = utf-16, positive = utf-8) and string length are in bytes 17-21
            type = unpack("l", p.value[17:21])[0]
            res = (21 + type - 1, "utf-8") if type > 0 else (21 + (abs(type) - 1) * 2, "utf-16")
            return p.value[21:res[0]].decode(res[1])
        # if thrall still has default game name it has to be derived from the TemplateTableSpawn.json
        elif "ThrallInfo" in p.name and (names or os.path.isfile('TemplateTableSpawn.json')) and len(p.value) >= 13:
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
            # add DLC default names manually (as they come up)
            names['Horse_Knight_RoH_Black'] = 'Black Horse'

            # thralls with custom names
            name_filter = (Properties.name.like("%PetName")) | (Properties.name.like("%ThrallName"))
            custom_name_ids_query = session.query(Properties.object_id).filter(name_filter)
            # thralls with no custom names
            info_filter = (Properties.name.like("%ThrallInfo")) & (Properties.object_id.notin_(custom_name_ids_query))

            # get owners for thralls with custom names
            for p in session.query(Properties).filter(name_filter).all():
                nam = Properties._get_name(p)
                if nam and ((strict and name.lower() == nam.lower()) or (not strict and name.lower() in nam.lower())):
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
    def get_pippi_money(name=None, owner_id=None):
        if name:
            # if there are no matching results or the name is ambiguous, return None
            owners = Owner.get_by_name(name, nocase=True, include_guilds=False)
            if len(owners) != 1:
                return None

            owner_id = owners[0].id

        # get the property row with the Pippi wallet belonging to the owner
        p = session.query(Properties).filter_by(object_id=owner_id, name="Pippi_WalletComponent_C.walletAmount").first()
        if not p:
            # if no such owner exists or they don't have a wallet, return None
            return None

        gold = unpack('>Q', p.value[66:74])[0]
        silver = unpack('>Q', p.value[141:149])[0]
        bronze = unpack('>Q', p.value[216:224])[0]
        return (gold, silver, bronze)

    @staticmethod
    def give_thrall(object_ids, owner_id, autocommit=True):
        if object_ids is None:
            return None
        if not isinstance(object_ids, ITER):
            object_ids = (object_ids,)
        for object_id in object_ids:
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

class Categories(UsersBase):
    __tablename__ = 'categories'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    cmd = Column(String, nullable=False)
    frequency = Column(Interval, default=timedelta(days=7))
    start = Column(String, default=datetime.utcnow().strftime('%A') +' 00:00')
    fee = Column(Integer, default=1)
    verbosity = Column(Integer, default=1)
    guild_pay = Column(Boolean, default=False)
    output_channel = Column(String)
    input_channel = Column(String)
    alert_message = Column(String)
    # relationship
    groups = relationship("Groups", back_populates="category")

    @staticmethod
    def _convert_to_daytime(value):
        wd = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
        now = datetime.utcnow()
        try:
            d, t = value.split()
            d = d if d in wd else now.strftime('%A')
            t = t.split(':')
            hour = t[0] if int(t[0]) >= 0 and int(t[0]) < 24 else 0
            minute = t[1] if int(t[1]) >= 0 and int(t[1]) < 60 else 0
        except:
            d = now.strftime('%A')
            hour, minute = '00', '00'
        return d + ' ' + hour.zfill(2) + ':' + minute.zfill(2)

    def __init__(self, *args, **kwargs):
        freq = {'daily': timedelta(days=1), 'weekly': timedelta(weeks=1), 'monthly': timedelta(weeks=4)}
        if kwargs.get('frequency') and type(kwargs['frequency']) is str and kwargs['frequency'] in freq:
            kwargs['frequency'] = freq[kwargs['frequency']]
        kwargs['start'] = self._convert_to_daytime(kwargs.get('start'))
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"<Categories(id={self.id}, name='{self.name}' cmd='{self.cmd}')>"

class CatOwners(UsersBase):
    __tablename__ = 'cat_owners'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('groups.id'), primary_key=True, nullable=False)
    # relationship
    group = relationship("Groups", back_populates="owners")

    def __init__(self, *args, **kwargs):
        if not ('group' in kwargs or 'group_id' in kwargs) and not ('category' in kwargs or 'category_id' in kwargs):
            raise SQLAlchemyError("Initialization requires either a group or a category.")
        elif not ('group' in kwargs or 'group_id' in kwargs):
            if not 'next_due' in kwargs:
                cat = kwargs.get('category', session.query(Categories).get(kwargs.get('category_id', 0)))
                kwargs['next_due'] = next_time(cat.start) if cat and cat.start else datetime.utcnow()
            kwargs['group'] = Groups(name=kwargs.get('name'), next_due=kwargs['next_due'], category=cat)
        _kwargs = {}
        for key, arg in kwargs.items():
            if key in self.__mapper__.attrs.keys():
                _kwargs[key] = arg
        super().__init__(*args, **_kwargs)

    @staticmethod
    def get(name, category_id=None):
        ids = [id for id, in session.query(Characters.id).filter(Characters.name.collate('NOCASE') == name).union(
                             session.query(Guilds.id).filter(Guilds.name.collate('NOCASE') == name)).all()]
        if not ids:
            return None
        cat_owners = []
        if category_id is not None:
            filter = CatOwners.id.in_(ids) & (CatOwners.category_id == category_id)
            cat_owners = [co for co in session.query(CatOwners).filter(filter).all()]
        else:
            cat_owners = [co for co in session.query(CatOwners).filter(CatOwners.id.in_(ids)).all()]
        return cat_owners

    @property
    def category(self):
        return self.group.category

    @property
    def name(self):
        name = session.query(Characters.name).filter_by(id=self.id).union(
               session.query(Guilds.name).filter_by(id=self.id)).first()
        return name[0] if name else None

    @name.setter
    def name(self, value):
        self.group.name = value

    @property
    def balance(self):
        return self.group.balance

    @balance.setter
    def balance(self, value):
        self.group.balance = value

    @property
    def next_due(self):
        return self.group.next_due

    @next_due.setter
    def next_due(self, value):
        self.group.next_due = value

    @property
    def last_payment(self):
        return self.group.last_payment

    @last_payment.setter
    def last_payment(self, value):
        self.group.last_payment = value

    @property
    def is_simple_group(self):
        return False if self.group._name else True

    def __repr__(self):
        return f"<CatOwners(id={self.id}, group_id={self.group_id})>"

class Groups(UsersBase):
    __tablename__ = 'groups'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('categories.id'))
    _name = Column("name", String)
    balance = Column(Integer, default=0)
    last_payment = Column(DateTime)
    next_due = Column(DateTime)
    # relationship
    owners = relationship("CatOwners", back_populates="group")
    category = relationship("Categories", back_populates="groups")

    def __init__(self, *args, **kwargs):
        if not 'next_due' in kwargs:
            cat = kwargs.get('category')
            if not cat:
                cat = session.query(Categories).get(kwargs['category_id'])
            kwargs['next_due'] = next_time(cat.start) if cat and cat.start else datetime.utcnow()
        super().__init__(*args, **kwargs)

    @property
    def name(self):
        if self._name:
            return self._name
        elif len(self.owners) >= 1:
            id = self.owners[0].id
            name = session.query(Characters.name).filter_by(id=id).union(
                   session.query(Guilds.name).filter_by(id=id)).first()
            return name[0] if name else None
        return None

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def is_simple(self):
        return False if self._name else True


    def __repr__(self):
        return f"<Groups(id={self.id}, name='{self.name}')>"

class Boatbucks(UsersBase):
    __tablename__ = 'boatbucks'
    __bind_key__ = 'usersdb'

    id = Column(Integer, primary_key=True)
    bucks = Column(Integer, default=0)

    def __repr__(self):
        return f"<Boatbucks(id={self.id}, bucks={self.bucks})>"

GameBase.metadata.create_all(engines['gamedb'])
UsersBase.metadata.create_all(engines['usersdb'])
