"""
This module contains functions to use AI to help with the grant ingestion process.
classify_grant: classify the grant as relevant to tribes or not
describe_changes: describe the changes in the grant since the last time it was ingested
"""
from groq import Groq
from dotenv import load_dotenv
import json
import os

load_dotenv()

# NOTE: when we productionalize this, we will want to get the API key from the environment variables
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL")

def get_groq_client():
    if not GROQ_API_KEY:
        raise RuntimeError(
            "Missing Groq API key. Set GROQ_API_KEY in your environment (or .env)."
        )
    return Groq(api_key=GROQ_API_KEY)

def classify_grant(groq_client, grant):
    prompt = f"""
        You are evaluating whether a federal grant is relevant to a Native American tribal government.

        Respond ONLY with valid JSON.

        Grant Title:
        {grant["title"]}

        Description:
        {grant["description"][:1500] if grant["description"] else "No description available"}

        Eligibility:
        {grant.get("eligibilities", [])}

        Return:
        {{
        "MODEL": "{GROQ_MODEL}",
        "is_relevant": true/false,
        "relevance_score": 0-100,
        "tags": [],
        "reasoning": ""
        }}
        """

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        content = response.choices[0].message.content
        if not content:
            print("No content returned from AI")
            return None
        return json.loads(content)

    except Exception as e:
        print("Error:", e)
        return None

def test_groq():
    groq = get_groq_client()
    response = groq.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "user", "content": "Hello, how are you?"}
        ]
    )
    print(response.choices[0].message.content)

if __name__ == "__main__":
    test_groq()