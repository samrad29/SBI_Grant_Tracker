import re

def sanitize_email(email):
    """
    Sanitize an email address and make sure it is a valid email address
    """
    if not email:
        return False
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return False
    return True

def sanitize_password(password):
    """
    Sanitize a password and make sure it is a valid password
    """
    if not password:
        return False
    if len(password) < 8:
        return False
    return True