from psycopg2 import connect
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
            conn = connect(**params)
        except Exception as e:
            print(f"Exception attempting to connect to database: {e}")
        return conn

    def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def save_record(self, record, table):
        print("Saving record...")
        insert_statement = f"insert into {table} (%s) values %s"
        try:
            with self.connection.cursor() as cur:
                cur.execute(insert_statement, (AsIs(",".join(record.keys())), tuple(record.values())))
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to save record: {e}")
        return False

    def get_user_unique_contact_id_by_id(self, user_id):
        print("Getting user info by id...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.unique_contact_id).where(t.id == user_id)
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(query))
            user = cur.fetchone()
            if user and user.get("unique_contact_id"):
                return user["unique_contact_id"]
        return None

    def get_user_id_by_unique_contact_id(self, unique_contact_id):
        print("Getting user info by unique contact id...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.id).where(t.unique_contact_id == unique_contact_id)
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(query))
            user = cur.fetchone()
            if user and user.get("id"):
                return user["id"]
        return None

    def update_messages_last_checked_at(self, user_id):
        print("Updating user messages last checked at...")
        current_time = str(datetime.utcnow())
        update_statement = f"update {USERS_TABLE} set messages_last_checked_at = '{current_time}' where id = {user_id}"
        try:
            with self.connection.cursor() as cur:
                cur.execute(update_statement)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to update user messages_last_checked_at: {e}")
        return False

    def register_user(self, unique_contact_id):
        print("Checking users for unique contact id...")
        user_id = self.get_user_id_by_unique_contact_id(unique_contact_id)
        if user_id:
            return {"error": "User with unique contact id already exists."}

        print("Attempting to insert new user...")
        current_time = datetime.utcnow()
        new_user = {
            "unique_contact_id": unique_contact_id,
            "registered_at": current_time,
            "messages_last_checked_at": current_time,
        }
        response = self.save_record(new_user, USERS_TABLE)
        if not response:
            return {"error": "Problem saving new user in database."}

        print("Confirming user registration...")
        user_id = self.get_user_id_by_unique_contact_id(unique_contact_id)
        if user_id:
            return {"id": user_id}

        return {"error": "User not registered."}

    def send_message(self, message):
        print("Saving message in database...")
        message["saved_at"] = datetime.utcnow()
        response = self.save_record(message, MESSAGES_TABLE)
        if response:
            return message["saved_at"]
        return None

    def get_messages(self, recipient_id):
        print("Getting messages from database...")
        t = Table(MESSAGES_TABLE)
        query = Query.from_(t).select(t.star).where(t.recipient_id == recipient_id)  # add query for dates?
        try:
            with self.connection.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(str(query))
                messages = []
                while True:
                    message = cur.fetchone()
                    if message:
                        messages.append(dict(message))
                    else:
                        break
                return messages
            return []
        except Exception as e:
            print(f"Exception attempting to retrieve messages from database: {e}")
        return None

    def delete_messages(self, recipient_id, message_ids):
        print("Deleting messages...")
        message_ids = tuple(message_ids)
        delete_statement = f"delete from {MESSAGES_TABLE} where recipient_id = {recipient_id} and id in {message_ids}"
        try:
            with self.connection.cursor() as cur:
                cur.execute(delete_statement)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to delete messages: {e}")
        return False
