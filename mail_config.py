import os
import msal
import requests
from email.message import EmailMessage
  
# Azure AD app credentials (read from environment / .env, never hardcode)
client_id = os.environ.get("AZURE_CLIENT_ID", "")
tenant_id = os.environ.get("AZURE_TENANT_ID", "")
client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
shared_mailbox = os.environ.get("AZURE_SHARED_MAILBOX", "dih_noreply@apollotyres.com")
 
# Get OAuth2 token
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)
 
# For application permissions (service-to-service)
scopes = ["https://graph.microsoft.com/.default"]
result = app.acquire_token_for_client(scopes=scopes)
 
if "access_token" in result:
    access_token = result["access_token"]
    print("Token acquired successfully!")
    # Example: Send an email using Microsoft Graph API
    graph_endpoint = f"https://graph.microsoft.com/v1.0/users/{shared_mailbox}/sendMail"
    email_data = {
        "message": {
            "subject": "Test Email",
            "body": {
                "contentType": "Text",
                "content": "This is a test email from the Python script."
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": "tes.com"
                    }
                }
            ]
        },
        "saveToSentItems": "true"
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(graph_endpoint, headers=headers, json=email_data)
    if response.status_code == 202:
        print("Email sent successfully!")
    else:
        print(f"Failed to send email. Status code: {response.status_code}")
        print(f"Response: {response.text}")
else:
    print(f"Token acquisition failed: {result.get('error')}")
    print(f"Error description: {result.get('error_description')}")


