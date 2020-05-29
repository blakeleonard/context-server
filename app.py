from flask import Flask, request, escape, jsonify
from helpers.context_db import ContextDb

app = Flask(__name__)


@app.route("/api/register_user/", methods=["POST"])
def register_user():
    request_dict = request.get_json()
    if not request_dict.get("unique_contact_id"):
        return {"error": "Missing unique contact id."}
    elif len(request_dict["unique_contact_id"]) < 10 or len(request_dict["unique_contact_id"]) > 20:
        return {"error": "Unique contact id must be 10 - 20 characters in length."}

    db = ContextDb()
    response = db.register_user(request_dict["unique_contact_id"])
    db.close()

    if response.get("id"):
        return {"id": response["id"]}
    elif response.get("error"):
        return {"error": response["error"]}
    return {"error": "User not registered."}


REQUIRED_MESSAGE_FIELDS = ["recipient_unique_contact_id", "sent_from_sender_at", "is_encrypted", "body"]


@app.route("/api/send_message/<sender_id>", methods=["POST"])
def send_message(sender_id):
    message = request.get_json()
    if not message:
        return {"error": "Missing message data."}
    for field in REQUIRED_MESSAGE_FIELDS:
        if not message.get(field):
            return {"error": f"Message data is missing {field}."}

    db = ContextDb()
    if not sender_id:
        if not message.get("sender_unique_contact_id"):
            return {"error": "Either sender id or sender unique contact id must be present."}
        sender_id = db.get_user_id_by_unique_contact_id(message["sender_unique_contact_id"])
        if not sender_id:
            return {"error": "Sender unique contact id does not belong to a registered user."}
        message.pop("sender_unique_contact_id")
    else:
        sender_unique_contact_id = db.get_user_unique_contact_id_by_id(sender_id)
        if not sender_unique_contact_id:
            return {"error": "Sender id does not belong to a registered user."}
    message["sender_id"] = int(sender_id)

    recipient_id = db.get_user_id_by_unique_contact_id(message["recipient_unique_contact_id"])
    if not recipient_id:
        return {"error": "Recipient unique contact id does not belong to a registered user."}
    message["recipient_id"] = int(recipient_id)
    message.pop("recipient_unique_contact_id")

    if message["sender_id"] == message["recipient_id"]:
        return {"error": "Sender and Recipient are the same."}

    if len(message) != 5:
        return {"error": "Message data contains unknown fields."}

    saved_in_db_at = db.send_message(message)
    db.close()

    if saved_in_db_at:
        return {"saved_in_db_at": saved_in_db_at}
    return {"error": "Message not saved in database."}


@app.route("/api/get_messages/<recipient_id>", methods=["GET"])
def get_messages(recipient_id):
    if not recipient_id:
        return {"error": "Missing recipient id."}

    db = ContextDb()
    recipient_unique_contact_id = db.get_user_unique_contact_id_by_id(recipient_id)
    if not recipient_unique_contact_id:
        return {"error": "Recipient id does not belong to a registered user."}

    messages = db.get_messages(recipient_id)
    if messages is None:
        return {"error": "Problem while attempting to retreive messages from database."}
    db.update_messages_last_checked_at(recipient_id)
    db.close()

    return {"messages": messages}


@app.route("/api/delete_messages/<recipient_id>", methods=["DELETE"])
def delete_messages(recipient_id):
    if not recipient_id:
        return {"error": "Missing recipient id."}
    request_dict = request.get_json()
    if not request_dict.get("message_ids") or len(request_dict["message_ids"]) < 1:
        return {"error": "Missing message ids."}

    db = ContextDb()
    recipient_unique_contact_id = db.get_user_unique_contact_id_by_id(recipient_id)
    if not recipient_unique_contact_id:
        return {"error": "Recipient id does not belong to a registered user."}

    response = db.delete_messages(recipient_id, request_dict["message_ids"])
    db.close()

    return {"deleted_all": response}


if __name__ == "__main__":
    app.run()
