import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import DictCursor
from pypika import Query, Table
from configparser import ConfigParser
from datetime import datetime

from pprint import pprint

CONFIG_FILE = "database.ini"
CONFIG_SECTION = "postgresql"
MESSAGES_TABLE = "messages"
USERS_TABLE = "users"


class ContextDb:

    connection = None

    def __init__(self):
        self.connection = self.connect()

    def config(self):
        parser = ConfigParser()
        parser.read(CONFIG_FILE)
        db = {}
        if parser.has_section(CONFIG_SECTION):
            params = parser.items(CONFIG_SECTION)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(f"Section {CONFIG_SECTION} not found in {CONFIG_FILE}")
        return db

    def connect(self):
        conn = None
        try:
            print("Fetching db config...")
            params = self.config()
            print("Connecting to db...")
            conn = psycopg2.connect(**params)
        except Exception as e:
            print(f"Exception attempting to connect to database: {e}")
        return conn

    def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def register_user(self, unique_contact_id):
        print("Checking users for unique contact id...")
        t = Table(USERS_TABLE)
        user_already_exists_query = (
            Query.from_(t).select(t.id).where(t.unique_contact_id == unique_contact_id)
        )
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(user_already_exists_query))
            user = cur.fetchone()
            if user:
                return {"error": "User with unique contact id already exists."}

        print("Attempting to insert new user...")
        current_time = datetime.utcnow()
        new_user = {
            "unique_contact_id": unique_contact_id,
            "registered_at": current_time,
            "messages_last_checked": current_time,  # need to change to messages_last_checked_at
        }
        columns = new_user.keys()
        values = [new_user[column] for column in columns]
        insert_statement = f"insert into {USERS_TABLE} (%s) values %s"
        with self.connection.cursor() as cur:
            cur.execute(insert_statement, (AsIs(",".join(columns)), tuple(values)))
            self.connection.commit()

        print("Confirming user registration...")
        confirmation_query = (
            Query.from_(t).select(t.id).where(t.unique_contact_id == unique_contact_id)
        )
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(confirmation_query))
            user = cur.fetchone()
            if user.get("id"):
                return {"id": user["id"]}

        return {"error": "User not registered."}

    def get_user_info(self, id):
        return None

    def send_message(self, sender_info, recipient_info, message):
        sent_at = None
        try:
            sender_id = sender_info["id"]
            recipient_id = recipient_info["id"]
            print(f"sender id: {sender_id}")
            print(f"recipient id: {recipient_id}")
            print("Message:")
            pprint(message)
            sent_at = datetime.utcnow()
            print(f"sent_at: {sent_at}")
        except Exception as e:
            print(f"Exception attempting to save message in database: {e}")
        return sent_at

    def get_messages(self, recipient_info):
        messages = []
        try:
            cur = self.connection.cursor()
            cur.execute("SELECT version()")
            db_version = cur.fetchone()
            recipient_id = recipient_info["id"]
            print(f"recipient id: {recipient_id}")
            print(f"Test retrieval of db version: {db_version}")
            cur.close()
        except Exception as e:
            print(f"Exception attempting to retrieve messages from database: {e}")
        return messages
