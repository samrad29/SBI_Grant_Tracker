import time
import random

from db.db_util import get_db_connection

class TokenTracker:
    def __init__(self, job_id: int, conn=None):
        self.usage = {
            "groq": {"prompt": 0, "completion": 0},
            "openai": {"prompt": 0, "completion": 0},
        }
        self.job_id = job_id
        self.conn = conn

    def add(self, provider: str, prompt: int, completion: int):
        if self.conn is not None:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO token_tracker (job_id, provider, prompt_tokens, completion_tokens, total_tokens) VALUES (%s, %s, %s, %s, %s)",
                (self.job_id, provider, prompt, completion, prompt + completion),
            )
            cursor.close()
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO token_tracker (job_id, provider, prompt_tokens, completion_tokens, total_tokens) VALUES (%s, %s, %s, %s, %s)",
                (self.job_id, provider, prompt, completion, prompt + completion),
            )
            conn.commit()
            cursor.close()
            conn.close()
        self.usage[provider]["prompt"] += prompt
        self.usage[provider]["completion"] += completion

    def total(self):
        return self.usage

def with_backoff(fn, max_retries=5, base_delay=1.0):
    def wrapper(*args, **kwargs):
        last = None

        for i in range(max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last = e

                if i == max_retries:
                    raise

                delay = base_delay * (2 ** i)
                delay *= 1 + random.uniform(-0.2, 0.2)

                time.sleep(delay)

        raise last

    return wrapper