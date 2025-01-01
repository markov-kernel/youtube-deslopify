from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import os
import json
from typing import Optional

# OAuth 2.0 scopes that we'll need for accessing playlists
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

def get_client_config():
    """Load client configuration from client_secrets.json"""
    try:
        with open('client_secrets.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        print("Error loading client_secrets.json")
        print(f"Error details: {str(e)}")
        raise

def get_credentials() -> Optional[Credentials]:
    """Gets valid user credentials from storage or initiates OAuth2 flow."""
    try:
        # Get client configuration
        client_config = get_client_config()
        
        # Try to load existing token
        if os.path.exists('token.json'):
            print("Loading existing token...")
            with open('token.json', 'r') as token_file:
                token_data = json.load(token_file)
                credentials = Credentials(
                    token=token_data.get('token'),
                    refresh_token=token_data.get('refresh_token'),
                    token_uri=client_config['web']['token_uri'],
                    client_id=client_config['web']['client_id'],
                    client_secret=client_config['web']['client_secret'],
                    scopes=token_data.get('scopes')
                )
            
            # Check if credentials are valid
            if credentials and credentials.valid:
                print("Existing credentials are valid")
                return credentials
            
            # Refresh if expired
            if credentials and credentials.expired and credentials.refresh_token:
                print("Refreshing expired credentials")
                credentials.refresh(Request())
                save_credentials(credentials)
                return credentials
        
        print("No valid credentials found, starting new OAuth flow")
        return perform_oauth_flow()
        
    except Exception as e:
        print(f"Error in get_credentials: {str(e)}")
        return perform_oauth_flow()

def perform_oauth_flow() -> Optional[Credentials]:
    """Performs the OAuth flow to get new credentials."""
    try:
        print("Starting OAuth flow...")
        
        # Get client configuration
        client_config = get_client_config()
        
        # Create flow instance
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri="http://localhost:8080"
        )
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        print("\nPlease follow these steps:")
        print("1. Visit this URL to authorize the application:")
        print(auth_url)
        print("\n2. After authorization, you'll be redirected to localhost:8080")
        print("3. Copy the ENTIRE URL from your browser's address bar and paste it below")
        
        # Get the full redirect URL from user
        redirect_response = input("\nEnter the full redirect URL: ")
        print("\nProcessing redirect URL...")
        
        # Exchange the authorization response for credentials
        flow.fetch_token(authorization_response=redirect_response)
        credentials = flow.credentials
        
        # Save the credentials
        save_credentials(credentials)
        
        return credentials
        
    except Exception as e:
        print(f"Error during OAuth flow: {str(e)}")
        return None

def save_credentials(credentials: Credentials):
    """Save credentials to token.json"""
    token_data = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    
    with open('token.json', 'w') as token_file:
        json.dump(token_data, token_file)
    print("Credentials saved to token.json")

if __name__ == "__main__":
    # For testing OAuth locally
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    
    print("Starting local OAuth test...")
    credentials = get_credentials()
    
    if credentials:
        print("\nAuthentication successful!")
        print(f"Access token exists: {'Yes' if credentials.token else 'No'}")
        print(f"Refresh token exists: {'Yes' if credentials.refresh_token else 'No'}")
        print(f"Token expiry: {credentials.expiry}")
    else:
        print("\nAuthentication failed!") 