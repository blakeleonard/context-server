from os import getenv
from re import match
from flask import Flask, request
from helpers.context_db import ContextDb
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity

app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = getenv("JWT_SECRET_KEY", "super-secret")
jwt = JWTManager(app)

EMAIL_REGEX = r"""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
PASSWORD_REGEX = r"""^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*#?&])[A-Za-z\d@$!%*#?&]{8,}$"""


@app.route("/register_user", methods=["POST"])
def register_user():
    if not request.is_json:
        return {"msg": "Missing JSON in request."}, 400
    if not request.json.get("email"):
        return {"msg": "Missing email."}, 400
    if not request.json.get("password"):
        return {"msg": "Missing password."}, 400
    if not match(EMAIL_REGEX, request.json["email"]):
        return {"msg": "Invalid email."}, 400
    if not match(PASSWORD_REGEX, request.json["password"]):
        return {"msg": "Invalid password."}, 400

    db = ContextDb()
    response = db.register_user(request.json["email"], request.json["password"])
    db.close()

    if response.get("id"):
        return {"id": response["id"]}
    if response.get("msg"):
        return {"msg": response["msg"]}, 500
    return {"msg": "User not registered."}, 500


@app.route("/login", methods=["POST"])
def login():
    if not request.is_json:
        return {"msg": "Missing JSON in request"}, 400
    if not request.json.get("email"):
        return {"msg": "Missing email."}, 400
    if not request.json.get("password"):
        return {"msg": "Missing password."}, 400

    db = ContextDb()
    user_id = db.get_user_id_by_email(request.json["email"])
    if not user_id:
        return {"msg": "Email does not belong to a registered user."}, 401
    authenticated = db.check_password(user_id, request.json["password"])
    db.close()
    if authenticated is None:
        return {"msg": "Error while attempting to check password."}, 500
    if not authenticated:
        return {"msg": "Bad username or password."}, 401

    access_token = create_access_token(identity=request.json["email"])
    return {"id": user_id, "access_token": access_token}


@app.route("/change_password/<user_id>", methods=["POST"])
@jwt_required
def change_password(user_id):
    if not user_id:
        return {"msg": "Missing user id."}, 400
    if not request.is_json:
        return {"msg": "Missing JSON in request"}, 400
    if not request.json.get("old_password"):
        return {"msg": "Missing old password."}, 400
    if not request.json.get("new_password"):
        return {"msg": "Missing new password."}, 400
    if not match(PASSWORD_REGEX, request.json["new_password"]):
        return {"msg": "Invalid new password."}, 400

    db = ContextDb()
    email = db.get_user_email_by_id(user_id)
    if not email:
        return {"msg": "User id does not belong to a registered user."}, 401
    if email != get_jwt_identity():
        return {"msg": "User is not authorized."}, 401

    password_changed = db.change_password(user_id, request.json["old_password"], request.json["new_password"],)
    db.close()
    if not password_changed:
        return {"msg": "Password Change failed."}, 500

    return {"password_changed_for_user_id": user_id}


@app.route("/delete_user/<user_id>", methods=["DELETE"])
@jwt_required
def delete_user(user_id):
    if not user_id:
        return {"msg": "Missing user id."}, 400
    if not request.is_json:
        return {"msg": "Missing JSON in request"}, 400
    if not request.json.get("password"):
        return {"msg": "Missing password."}, 400

    db = ContextDb()
    email = db.get_user_email_by_id(user_id)
    if not email:
        return {"msg": "User id does not belong to a registered user."}, 401
    if email != get_jwt_identity():
        return {"msg": "User is not authorized."}, 401

    if not db.check_password(user_id, request.json["password"]):
        return {"msg": "Wrong password."}, 401
    messages_deleted = db.delete_messages(user_id, None)
    if not messages_deleted:
        return {"msg": "User message deletion failed.  User not deleted."}, 500

    user_deleted = db.delete_user(user_id)
    db.close()
    if not user_deleted:
        return {"msg": "User deletion failed."}, 500

    return {"deleted_user_id": user_id}


@app.route("/messages/<user_id>", methods=["GET", "POST", "DELETE"])
@jwt_required
def messages(user_id):
    if not user_id:
        return {"msg": "User id is missing."}, 400

    db = ContextDb()
    email = db.get_user_email_by_id(user_id)
    if not email:
        return {"msg": "User id does not belong to a registered user."}, 401
    if email != get_jwt_identity():
        return {"msg": "User is not authorized."}, 401

    if request.method == "GET":
        return get_messages(user_id, request, db)
    elif request.method == "POST":
        return send_message(user_id, request, db)
    elif request.method == "DELETE":
        return delete_messages(user_id, request, db)


def get_messages(recipient_id, request, db):
    messages_last_checked_at = None
    get_all = request.args.get("all")
    if get_all is None or get_all.lower() != "true":
        messages_last_checked_at = db.get_user_messages_last_checked_at_by_id(recipient_id)
        if not messages_last_checked_at:
            return {"msg": "User missing messages last checked at field."}, 500

    messages = db.get_messages(recipient_id, messages_last_checked_at)
    if messages is None:
        return {"msg": "Problem while attempting to retreive messages from database."}, 500
    db.update_messages_last_checked_at(recipient_id)
    db.close()

    return {"messages": messages}


REQUIRED_MESSAGE_FIELDS = ["unique_id", "recipient_email", "sent_from_sender_at", "encrypted_id", "hmac", "body"]


def send_message(sender_id, request, db):
    if not request.is_json:
        return {"msg": "Missing JSON in request"}, 400
    message = request.json
    for field in REQUIRED_MESSAGE_FIELDS:
        if not message.get(field):
            return {"msg": f"Message data is missing {field}."}, 400
    if len(message) != len(REQUIRED_MESSAGE_FIELDS):
        return {"msg": "Message data contains unknown fields."}, 400

    message["sender_id"] = int(sender_id)

    recipient_id = db.get_user_id_by_email(message["recipient_email"])
    if not recipient_id:
        return {"msg": "Recipient email does not belong to a registered user."}, 401
    message["recipient_id"] = int(recipient_id)
    message.pop("recipient_email")

    if message["sender_id"] == message["recipient_id"]:
        return {"msg": "Sender and Recipient are the same."}, 400

    if db.does_unique_id_exist(sender_id, message["unique_id"]):
        return {"msg": "Message has already been sent."}, 400

    saved_in_db_at = db.send_message(message)
    db.close()

    if saved_in_db_at:
        return {"saved_in_db_at": saved_in_db_at}
    return {"msg": "Message not saved in database."}, 500


def delete_messages(recipient_id, request, db):
    delete_all = request.args.get("all")
    if delete_all is not None and delete_all.lower() == "true":
        messages_deleted = db.delete_messages(recipient_id, None)
    else:
        request_dict = request.json
        if not request_dict:
            return {"msg": "Missing JSON in request"}, 400
        if not request_dict.get("message_ids") or len(request_dict["message_ids"]) < 1:
            return {"msg": "Missing message ids."}, 400

        messages_deleted = db.delete_messages(recipient_id, request_dict["message_ids"])

    db.close()

    return {"messages_deleted": messages_deleted}


if __name__ == "__main__":
    app.run()
