from psycopg2 import connect
from psycopg2.extensions import AsIs
from psycopg2.extras import DictCursor
from pypika import Query, Table
from configparser import ConfigParser
from datetime import datetime
from bcrypt import hashpw, checkpw, gensalt

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

    def get_user_messages_last_checked_at_by_id(self, user_id):
        print("Getting messages last checked at by id...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.messages_last_checked_at).where(t.id == user_id)
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(query))
            user = cur.fetchone()
            if user and user.get("messages_last_checked_at"):
                return user["messages_last_checked_at"]
        return None

    def get_user_email_by_id(self, user_id):
        print("Getting user email by id...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.email).where(t.id == user_id)
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(query))
            user = cur.fetchone()
            if user and user.get("email"):
                return user["email"]
        return None

    def get_user_id_by_email(self, email):
        print("Getting user id by email...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.id).where(t.email == email)
        with self.connection.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(str(query))
            user = cur.fetchone()
            if user and user.get("id"):
                return user["id"]
        return None

    def does_unique_id_exist(self, sender_id, unique_id):
        print("Checking unique id...")
        t = Table(MESSAGES_TABLE)
        query = Query.from_(t).select(t.id).where(t.sender_id == sender_id).where(t.unique_id == unique_id)
        try:
            with self.connection.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(str(query))
                message = cur.fetchone()
                if message and message.get("id"):
                    return True
        except Exception as e:
            print(f"Exception attempting to check password: {e}")
            return None
        return False

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

    def check_password(self, user_id, password):
        print("Checking password...")
        t = Table(USERS_TABLE)
        query = Query.from_(t).select(t.password_hash).where(t.id == user_id)
        try:
            with self.connection.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(str(query))
                user = cur.fetchone()
                if (
                    user
                    and user.get("password_hash")
                    and checkpw(password.encode("utf-8"), bytes(user["password_hash"]))
                ):
                    return True
        except Exception as e:
            print(f"Exception attempting to check password: {e}")
            return None
        return False

    def change_password(self, user_id, old_password, new_password):
        if not self.check_password(user_id, old_password):
            return False
        print("Changing password...")
        new_password_hash = hashpw(new_password.encode("utf-8"), gensalt())
        update_statement = f"update {USERS_TABLE} set password_hash = '{new_password_hash}' where id = {user_id}"
        try:
            with self.connection.cursor() as cur:
                cur.execute(update_statement)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to change password: {e}")
        return False

    def delete_user(self, user_id, password):
        if not self.check_password(user_id, password):
            return False
        print("Deleting user...")
        delete_statement = f"delete from {USERS_TABLE} where id = {user_id}"
        try:
            with self.connection.cursor() as cur:
                cur.execute(delete_statement)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to delete user: {e}")
        return False

    def register_user(self, email, password):
        print("Checking users for email...")
        user_id = self.get_user_id_by_email(email)
        if user_id:
            return {"msg": "User with email already exists."}

        print("Attempting to insert new user...")
        current_time = datetime.utcnow()
        new_user = {
            "email": email,
            "password_hash": hashpw(password.encode("utf-8"), gensalt()),
            "registered_at": current_time,
            "messages_last_checked_at": current_time,
        }
        record_saved = self.save_record(new_user, USERS_TABLE)
        if not record_saved:
            return {"msg": "Problem saving new user in database."}

        print("Confirming user registration...")
        user_id = self.get_user_id_by_email(email)
        if user_id:
            return {"id": user_id}

        return {"msg": "User not registered."}

    def send_message(self, message):
        print("Saving message in database...")
        message["saved_at"] = datetime.utcnow()
        record_saved = self.save_record(message, MESSAGES_TABLE)
        if record_saved:
            return message["saved_at"]
        return None

    def get_messages(self, recipient_id, messages_last_checked_at):
        print("Getting messages from database...")
        t = Table(MESSAGES_TABLE)
        query = Query.from_(t).select(t.star).where(t.recipient_id == recipient_id)
        if messages_last_checked_at:
            query = query.where(t.saved_at >= messages_last_checked_at)
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
        delete_statement = f"delete from {MESSAGES_TABLE} where recipient_id = {recipient_id}"
        if message_ids is None:
            message_ids = tuple(message_ids)
            delete_statement = delete_statement + f" and id in {message_ids}"
        try:
            with self.connection.cursor() as cur:
                cur.execute(delete_statement)
                self.connection.commit()
                return True
        except Exception as e:
            print(f"Exception attempting to delete messages: {e}")
        return False
