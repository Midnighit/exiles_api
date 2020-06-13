import os
from dotenv import load_dotenv

load_dotenv()

GAME_DB_URI = "sqlite:///" + os.getenv('SAVED_DIR_PATH') + "/game.db"
USERS_DB_URI = "sqlite:///" + os.getenv('SAVED_DIR_PATH') + "/supplemental.db"
ECHO = False
