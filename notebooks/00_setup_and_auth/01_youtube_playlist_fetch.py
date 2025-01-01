# First, run this in a Databricks notebook cell:
# %run ./00_youtube_oauth

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

def fetch_playlists(credentials):
    """Fetch all playlists for the authenticated user."""
    try:
        # Create YouTube API client
        youtube = build('youtube', 'v3', credentials=credentials)
        
        # Initialize empty list to store all playlists
        all_playlists = []
        next_page_token = None
        
        while True:
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
                playlist_info = {
                    'id': playlist['id'],
                    'title': playlist['snippet']['title'],
                    'description': playlist['snippet']['description'],
                    'video_count': playlist['contentDetails']['itemCount'],
                    'privacy_status': playlist['status']['privacyStatus'],
                    'published_at': playlist['snippet']['publishedAt']
                }
                all_playlists.append(playlist_info)
                
                # Print basic info
                print(f"Found playlist: {playlist_info['title']} ({playlist_info['video_count']} videos)")
            
            # Check if there are more pages
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        
        # Convert to DataFrame for easier analysis
        playlists_df = pd.DataFrame(all_playlists)
        
        print(f"\nTotal playlists found: {len(all_playlists)}")
        return playlists_df
    
    except HttpError as e:
        print(f'An HTTP error {e.resp.status} occurred: {e.content}')
        return None

# Check if we have credentials from the auth notebook
try:
    # This will raise NameError if youtube_credentials is not defined
    credentials = youtube_credentials
    print("Using existing credentials from auth notebook")
except NameError:
    print("ERROR: Credentials not found!")
    print("Please run the authentication notebook first using:")
    print("%run ./00_youtube_oauth")
    raise Exception("Please run the authentication notebook first to get youtube_credentials")

# Now we can use the credentials to fetch playlists
playlists_df = fetch_playlists(credentials)

# Display the results
if playlists_df is not None:
    display(playlists_df)
    
    # Save as a variable that can be accessed by other notebooks
    youtube_playlists = playlists_df 