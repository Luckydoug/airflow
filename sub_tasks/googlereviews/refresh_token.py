import requests

url = "https://www.googleapis.com/oauth2/v4/token"

payload = "{\r\n    \"grant_type\": \"refresh_token\",\r\n    \"refresh_token\": \"1//04PPs3dxcO3FOCgYIARAAGAQSNwF-L9IrWSd2psBR9xkYPRURrYB0_-KitVIkS8qxPZUp4iUCao70YTZfVUR7YhUR6Vp9U6u9Jcw\",\r\n    \"client_id\": \"33030961902-tfhja4tj5hne2mu3v0i9q3tp5jno49v3.apps.googleusercontent.com\",\r\n    \"client_secret\": \"LRt7xLDWD4p8cotD7M27lk5q\"\r\n}"

headers = {
  'Content-Type': 'text/plain'
}

def refresh_tokens():
    data = requests.request("POST", url, headers=headers, data=payload)
    data = data.json()
    token = data['access_token']
    return token