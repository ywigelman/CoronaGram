import requests
from pprint import pprint

subscription_key = "d2d3dc2b2fc94b64982dc912169f0a85"
endpoint = "https://textanalyticsitc.cognitiveservices.azure.com/"

sentiment_url = endpoint + "/text/analytics/v3.0/sentiment"

documents = {"documents": [
    {"id": "1", "language": "en",
        "text": "What's up man? how are you today?"}
]}

headers = {"Ocp-Apim-Subscription-Key": subscription_key}
response = requests.post(sentiment_url, headers=headers, json=documents)
sentiments = response.json()
pprint(sentiments['documents'][0]['sentiment'])