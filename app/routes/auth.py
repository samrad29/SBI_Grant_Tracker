from flask import Blueprint, jsonify, request, session
from werkzeug.security import check_password_hash

from db.db_util import get_db_connection, is_test_mode

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/api/auth/session", methods=["GET"])
def session_status():
    """Return current session user id, or 401 if not logged in."""
    uid = session.get("user_id")
    if not uid:
        return jsonify({"message": "Not authenticated"}), 401
    return jsonify({"user_id": uid}), 200


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