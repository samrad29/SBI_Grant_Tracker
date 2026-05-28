from flask import Blueprint, jsonify, request, session
from werkzeug.security import check_password_hash, generate_password_hash

from app.auth_util import sanitize_email, sanitize_password
from db.db_util import get_db_connection, is_test_mode

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/api/auth/session", methods=["GET"])
def session_status():
    """Return current session user id, or 401 if not logged in."""
    uid = session.get("user_id")
    group_name = session.get("group_name")
    if not uid:
        return jsonify({"message": "Not authenticated"}), 401
    return jsonify({"user_id": uid, "group_name": group_name}), 200

@auth_bp.route("/api/auth/group_session", methods=["GET"])
def group_session():
    """
    Return the current group name, should be used to access the create user page
    """
    group_name = session.get("group_name")
    if not group_name:
        return jsonify({"message": "Not authenticated"}), 401
    return jsonify({"group_name": group_name}), 200

@auth_bp.route("/api/auth/validate_netID")
def validate_netID():
    """
    Validate a netID against the database
    """
    netID = request.args.get("netID")
    if not netID:
        return jsonify({"message": "NetID required"}), 400
    conn = get_db_connection(test_mode=is_test_mode())
    cursor = conn.cursor()
    cursor.execute("SELECT group_name, group_id FROM groups WHERE net_id = %s LIMIT 1", (netID,))
    group = cursor.fetchone()
    if group:
        session.permanent = True
        session["group_id"] = group["group_id"]
        session["group_name"] = group["group_name"]
        return jsonify({"message": "NetID validated successfully", "group_name": group["group_name"], "group_id": group["group_id"]}), 200
    else:
        return jsonify({"message": "NetID not found"}), 404

@auth_bp.route("/api/auth/create_user")
def create_user():
    """
    Create a user in the database
    """
    try:
        email = request.args.get("email")
        group_id = session.get("group_id")
        if not group_id:
            return jsonify({"message": "Group ID not validated, session invalid"}), 401
        password = request.args.get("password")
        password_confirmation = request.args.get("password_confirmation")
        if password != password_confirmation:
            return jsonify({"message": "Passwords do not match"}), 400
        if not email or not password:
            return jsonify({"message": "Email and password required"}), 400
        if not sanitize_email(email):
            return jsonify({"message": "Invalid email"}), 400
        if not sanitize_password(password):
            return jsonify({"message": "Invalid password"}), 400
        password_hash = generate_password_hash(password)
        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE user_email = %s AND group_id = %s LIMIT 1", (email, group_id))
        user = cursor.fetchone()
        if user:
            return jsonify({"message": "User already exists"}), 400
        else:
            cursor.execute("INSERT INTO users (user_email, user_password, group_id) VALUES (%s, %s, %s)", (email, password_hash, group_id))
            conn.commit()
        return jsonify({"message": "User created successfully"}), 200
    except Exception as e:
        return jsonify({"message": "Error creating user: " + str(e)}), 500


@auth_bp.route("/api/auth/login", methods=["POST"])
def login():
    """
    Validate credentials and set ``session['user_id']`` (Flask server-side session cookie).
    """
    conn = None
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip()
        password = data.get("password") or ""
        if not email or not password:
            return jsonify({"message": "Email and password required"}), 400

        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, user_password FROM users WHERE user_email = %s LIMIT 1",
            (email,),
        )
        user = cursor.fetchone()
        if user and check_password_hash(user["user_password"], password):
            session.permanent = True
            session["user_id"] = user["user_id"]
            return jsonify(
                {"message": "User logged in successfully", "user_id": user["user_id"]}
            ), 200
        return jsonify({"message": "Invalid email or password"}), 401
    except Exception as e:
        return jsonify({"message": "Error logging in: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()


@auth_bp.route("/api/auth/logout", methods=["POST"])
def logout():
    """
    Clear the session cookie (drops ``user_id``).
    """
    try:
        session.clear()
        return jsonify({"message": "User logged out successfully"}), 200
    except Exception as e:
        return jsonify({"message": "Error logging out: " + str(e)}), 500