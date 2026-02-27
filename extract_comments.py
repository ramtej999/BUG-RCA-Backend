import os
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def extract_video_id(url):
    """
    Extracts the YouTube video ID from various URL formats.
    """
    # Regex to extract the video ID
    pattern = r"(?:v=|\/)([0-9A-Za-z_-]{11}).*"
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    return None

def fetch_comments(video_url, api_key=None, abort_event=None):
    """
    Fetches all top-level comments using YouTube Data API v3.
    """
    if not api_key:
        api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        print("Error: YouTube API key not provided and not in environment variables.")
        return []
        
    video_id = extract_video_id(video_url)
    if not video_id:
        print(f"Error: Could not extract video ID from {video_url}")
        return []

    try:
        # Initialize the YouTube API client
        youtube = build("youtube", "v3", developerKey=api_key)
        
        comments = []
        next_page_token = None
        
        # Paginate through the comment threads
        while True:
            if abort_event and abort_event.is_set():
                print("Extraction aborted by user.")
                break
                
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            
            # Extract top-level comments
            for item in response.get("items", []):
                comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                if comment_text and comment_text.strip():
                    comments.append(comment_text.strip())
                    
            next_page_token = response.get("nextPageToken")
            
            # If no more pages, break
            if not next_page_token:
                break
                
        return comments

    except HttpError as e:
        print(f"YouTube API Error: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error extracting comments: {e}")
        return []
