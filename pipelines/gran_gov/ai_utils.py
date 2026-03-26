"""
This module contains functions to use AI to help with the grant ingestion process.
classify_grant: classify the grant as relevant to tribes or not
describe_changes: describe the changes in the grant since the last time it was ingested
"""
from groq import Groq
from dotenv import load_dotenv
import json
import os
from datetime import datetime
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

def ai_grant_tagging(groq_client, grant):
    prompt = f"""
        You are classifying a government grant into categories.

        You MUST:
        1. Assign relevance scores (0-100) to the predefined categories below
        2. Optionally suggest up to 3 NEW categories if the predefined ones are insufficient

        Predefined categories: Housing, Tribal, Gaming, Cannabis, Environment, Agriculture, Broadband, Technology, Infrastructure, workforce_development, accepting_applications

        Rules:
        - Accepting applications is a special category used to indicate that the grant is currently accepting applications as of {datetime.now().strftime("%Y-%m-%d")}
        - Prefer predefined categories whenever possible
        - Only create a new category if it captures something important not covered above
        - New categories must be concise (1-3 words, snake_case)
        - Do NOT create synonyms of existing categories
        - Avoid overly specific categories (e.g., "solar_panel_installation_grants")

        Respond ONLY with valid JSON in this format:
        {{
        "tags": [
            {{"tag": "energy", "score": 85}},
            {{"tag": "tribal", "score": 60}}
        ],
        "new_tags": [
            {{"tag": "disaster_recovery", "score": 75}}
        ]
        }}

        Grant Title:
        {grant["title"]}

        Grant Description:
        {grant.get("description", "")[:1500]}

        Eligibility Codes:
        {grant.get("eligibilities", [])}

        Eligibility Description:
        {grant.get("eligibility_description", "")}

        deadline date:
        {grant.get("deadline_date", "")}

        deadline description:
        {grant.get("deadline_description", "")}
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
        print("Error in ai_grant_tagging:", e)
        return None

def ai_tribal_eligibility_check(groq_client, grant):
    prompt = f"""
        You are evaluating whether a Native American tribal government is eligible for a federal grant.

        Respond ONLY with valid JSON.

        Grant Title:
        {grant["title"]}

        Grant Description:
        {grant.get("description", "")[:1500]}

        Eligibility Codes:
        {grant.get("eligibilities", [])}

        Eligibility Description:
        {grant.get("eligibility_description", "")}

        Return:
        {{
        "model": "{GROQ_MODEL}",
        "is_tribal_eligible": true/false,
        "eligibility_score": 0-100,
        "eligibility_reasoning": ""
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
        print("Error in ai_tribal_eligibility_check:", e)
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