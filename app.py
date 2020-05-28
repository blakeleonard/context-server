from flask import Flask, escape, request, jsonify
from helpers.context_db import ContextDb

app = Flask(__name__)


@app.route("/")
def hello():
    name = request.args.get("name", "World")
    return f"Hello, {escape(name)}!"


@app.route("/api/register_user/<unique_contact_id>", methods=["POST"])
def register_user(unique_contact_id):
    if not unique_contact_id:
        return {"error": "Missing unique contact id."}

    db = ContextDb()
    response = db.register_user(unique_contact_id)
    db.close()

    if response.get("id"):
        return {"id": response["id"]}
    elif response.get("error"):
        return {"error": response["error"]}
    return {"error": "User not registered."}


@app.route("/api/send_message/<sender_id>", methods=["POST"])
def send_message(sender_id):
    if not sender_id:
        return {"error": "Missing sender id."}
    message = request.get_json()
    if not message:
        return {"error": "Missing message data."}
    if not message.get("recipient_id"):
        return {"error": "Message data is missing recipient id."}
    if not message.get("sent_from_sender_at"):
        return {"error": "Message data is missing sender sent at timestamp."}
    if not message.get("is_encrypted"):
        return {"error": "Message data is missing encrypted flag."}
    if not message.get("body"):
        return {"error": "Message data is missing message body."}

    db = ContextDb()
    sender_info = db.get_user_info(sender_id)
    if not sender_info:
        return {"error": "Sender id does not belong to a registered user."}
    recipient_info = db.get_user_info(message["recipient_id"])
    if not recipient_info:
        return {"error": "Recipient id does not belong to a registered user."}

    saved_in_db_at = db.send_message(sender_info, recipient_info, message)
    db.close()

    if saved_in_db_at:
        return {"saved_in_db_at": saved_in_db_at}
    return {"error": "Message not saved in database."}


@app.route("/api/get_messages/<recipient_id>", methods=["GET"])
def get_messages(recipient_id):
    if not recipient_id:
        return {"error": "Missing recipient id."}

    db = ContextDb()
    recipient_info = db.get_user_info(recipient_id)
    if not recipient_info:
        return {"error": "Recipient id does not belong to a registered user."}

    messages = db.get_messages(recipient_info)
    db.close()

    return {"messages": messages}


if __name__ == "__main__":
    app.run()
