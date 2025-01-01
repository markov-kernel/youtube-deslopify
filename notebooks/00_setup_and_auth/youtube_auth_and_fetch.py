# Databricks notebook source

# COMMAND ----------
# AUTHENTICATION SETUP

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import os
import json
from typing import Optional
from IPython.display import HTML, display

# OAuth 2.0 scopes that we'll need for accessing playlists
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

def get_client_config():
    """Retrieve client configuration from Databricks secrets."""
    try:
        client_id = dbutils.secrets.get(scope="youtube-secrets", key="client-id")
        client_secret = dbutils.secrets.get(scope="youtube-secrets", key="client-secret")
        
        # Debug: Print partial values for verification
        print(f"Retrieved client_id (first 8 chars): {client_id[:8]}...")
        print(f"Retrieved client_secret (first 8 chars): {client_secret[:8]}...")
        
        # Use the exact Azure Databricks URL
        redirect_uri = "https://adb-2449401244759216.16.azuredatabricks.net/oauth2/callback"
        
        config = {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uris": [redirect_uri]
            }
        }
        
        # Debug: Print configuration structure (without sensitive values)
        debug_config = {
            "web": {
                "client_id": "***",
                "client_secret": "***",
                "auth_uri": config["web"]["auth_uri"],
                "token_uri": config["web"]["token_uri"],
                "auth_provider_x509_cert_url": config["web"]["auth_provider_x509_cert_url"],
                "redirect_uris": config["web"]["redirect_uris"]
            }
        }
        print("\nConfiguration structure:")
        print(json.dumps(debug_config, indent=2))
        
        return config
        
    except Exception as e:
        print("Error accessing secrets or configuration.")
        print(f"Error details: {str(e)}")
        raise

def get_credentials() -> Optional[Credentials]:
    """Gets valid user credentials from storage or initiates OAuth2 flow."""
    try:
        # Get client configuration with secrets
        client_config = get_client_config()
        
        # Try to get existing refresh token from the notebook-scoped variable
        if 'saved_refresh_token' in globals():
            refresh_token = saved_refresh_token
            print("Using existing refresh token from notebook variable")
            
            # Create credentials from refresh token
            credentials = Credentials(
                None,  # No access token since we'll refresh it
                refresh_token=refresh_token,
                token_uri=client_config['web']['token_uri'],
                client_id=client_config['web']['client_id'],
                client_secret=client_config['web']['client_secret'],
                scopes=SCOPES
            )
            
            # Force a refresh to verify the credentials work
            credentials.refresh(Request())
            print("Successfully loaded existing credentials")
            return credentials
            
        else:
            print("No existing refresh token found")
            return perform_oauth_flow()
        
    except Exception as e:
        print(f"Need to perform new OAuth flow: {str(e)}")
        return perform_oauth_flow()

def perform_oauth_flow() -> Optional[Credentials]:
    """Performs the OAuth flow to get new credentials."""
    try:
        print("Starting OAuth flow...")
        
        # Get client configuration with secrets
        client_config = get_client_config()
        
        # Get the redirect URI from the config
        redirect_uri = client_config['web']['redirect_uris'][0]
        
        # Create flow instance for web application
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=redirect_uri
        )
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        # Display instructions in Databricks notebook
        html = f"""
        <div style="margin: 20px; padding: 20px; border: 1px solid #ccc; border-radius: 5px;">
            <h3>Please follow these steps:</h3>
            <ol>
                <li>Visit this URL to authorize the application: <a href="{auth_url}" target="_blank">Click here to authenticate</a></li>
                <li>After authorization, you'll be redirected to {redirect_uri}</li>
                <li>Copy the ENTIRE URL from your browser's address bar after being redirected (even if the page shows an error) and paste it below</li>
            </ol>
        </div>
        """
        display(HTML(html))
        
        # Get the full redirect URL from user
        redirect_response = input("Enter the full redirect URL: ")
        print(f"\nProcessing redirect URL...")
        
        # Exchange the authorization response for credentials
        flow.fetch_token(authorization_response=redirect_response)
        credentials = flow.credentials
        
        # Store the refresh token in a notebook-scoped variable
        global saved_refresh_token
        saved_refresh_token = credentials.refresh_token
        
        print("Successfully stored refresh token in notebook variable")
        print("IMPORTANT: Save this refresh token for future use:")
        print(f"Refresh Token: {credentials.refresh_token}")
        
        return credentials
        
    except Exception as e:
        print(f"Error during OAuth flow: {str(e)}")
        return None

# For Databricks, we need to allow insecure transport for OAuth testing
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Get the credentials
credentials = get_credentials()
if credentials is None:
    raise Exception("Failed to obtain credentials")

# Print success message and debug info
print("\nSuccessfully authenticated with YouTube API!")
print(f"Access token exists: {'Yes' if credentials.token else 'No'}")
print(f"Refresh token exists: {'Yes' if credentials.refresh_token else 'No'}")
print(f"Token expiry: {credentials.expiry}")

# Store credentials in a variable that can be accessed by other cells
youtube_credentials = credentials

# COMMAND ----------
# PLAYLIST FETCHING

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from typing import Optional, Dict, List

def fetch_playlists(credentials: Credentials) -> Optional[pd.DataFrame]:
    """Fetch all playlists for the authenticated user."""
    try:
        # Create YouTube API client
        youtube = build('youtube', 'v3', credentials=credentials)
        
        # Initialize empty list to store all playlists
        all_playlists: List[Dict] = []
        next_page_token = None
        
        while True:
            try:
                # Call the playlists.list method to retrieve playlists
                request = youtube.playlists().list(
                    part='snippet,contentDetails,status',
                    mine=True,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                # Process each playlist
                for playlist in response['items']:
                    try:
                        playlist_info = {
                            'id': playlist['id'],
                            'title': playlist['snippet']['title'],
                            'description': playlist['snippet'].get('description', ''),
                            'video_count': playlist['contentDetails']['itemCount'],
                            'privacy_status': playlist['status']['privacyStatus'],
                            'published_at': playlist['snippet']['publishedAt']
                        }
                        all_playlists.append(playlist_info)
                        
                        # Print basic info
                        print(f"Found playlist: {playlist_info['title']} ({playlist_info['video_count']} videos)")
                    except KeyError as e:
                        print(f"Warning: Skipping playlist due to missing data: {str(e)}")
                        continue
                
                # Check if there are more pages
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit exceeded
                    print("Rate limit exceeded. Waiting before retrying...")
                    time.sleep(60)  # Wait 60 seconds before retrying
                    continue
                else:
                    raise
        
        if not all_playlists:
            print("No playlists found")
            return None
            
        # Convert to DataFrame for easier analysis
        playlists_df = pd.DataFrame(all_playlists)
        
        print(f"\nTotal playlists found: {len(all_playlists)}")
        return playlists_df
    
    except HttpError as e:
        print(f'An HTTP error {e.resp.status} occurred: {e.content}')
        return None
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
        return None

# Now we can use the credentials to fetch playlists
playlists_df = fetch_playlists(youtube_credentials)

# Display the results
if playlists_df is not None:
    display(playlists_df)
    
    # Save as a variable that can be accessed by other notebooks
    youtube_playlists = playlists_df 