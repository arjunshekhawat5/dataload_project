import os
import webbrowser
from dotenv import load_dotenv
from fyers_apiv3.fyersModel import SessionModel

# Load credentials from .env file
load_dotenv()

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
REDIRECT_URI = "http://asefno.com/"  # Must match your Fyers App settings
RESPONSE_TYPE = "code"
GRANT_TYPE = "authorization_code"

def generate_token():
    """
    A guided process to get the Fyers v3 access token using the official SDK utility.
    """
    if not all([CLIENT_ID, SECRET_KEY]):
        print("ERROR: FYERS_CLIENT_ID and FYERS_SECRET_KEY must be set in your .env file.")
        return

    try:
        session = SessionModel(
            client_id=CLIENT_ID,
            secret_key=SECRET_KEY,
            redirect_uri=REDIRECT_URI,
            response_type=RESPONSE_TYPE,
            grant_type=GRANT_TYPE
        )
        auth_url = session.generate_authcode()
        print(f"--- Step 1: Generate Auth Code ---")
        print(f"Login URL: {auth_url}")
        webbrowser.open(auth_url, new=1)
        print("\n1. Log in to Fyers in the browser window that just opened.")
        print("2. After logging in, you will be redirected to a URL like 'http://localhost:3000/?s=ok&code=...&auth_code=...'")
        auth_code = input("3. Copy the ENTIRE 'auth_code' value from that URL and paste it here: ")

        session.set_token(auth_code)
        response = session.generate_token()

        if response.get("access_token"):
            access_token = response["access_token"]
            print("\n--- Step 2: Generate Access Token ---")
            print(f"SUCCESS! Your Access Token is:\n\n{access_token}\n")
            print("COPY this token and PASTE it into your .env file for the FYERS_ACCESS_TOKEN variable.")
        else:
            print(f"\n--- FAILED ---\n{response}")

    except Exception as e:
        print(f"\n--- FAILED ---")
        print(f"An error occurred during token generation: {e}")

if __name__ == "__main__":
    generate_token()