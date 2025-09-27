#!/usr/bin/env python3
"""
Gmail API Refresh Token Generator

This script will help you generate a new refresh token for Gmail API access.
You need to have valid Client ID and Client Secret from Google Cloud Console.

Steps:
1. Go to Google Cloud Console
2. Enable Gmail API
3. Create OAuth 2.0 Desktop Application credentials
4. Copy the Client ID and Client Secret
5. Update this script with your credentials
6. Run this script to get your refresh token
"""

import os
import sys
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Gmail API scopes - these permissions are needed
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

def get_refresh_token():
    """Generate refresh token for Gmail API"""
    
    print("🔑 Gmail API Refresh Token Generator")
    print("=" * 40)
    
    # Get credentials from user input
    client_id = input("📋 Enter your Client ID: ").strip()
    client_secret = input("🔐 Enter your Client Secret: ").strip()
    
    if not client_id or not client_secret:
        print("❌ Client ID and Client Secret are required!")
        return None
    
    print(f"\n✅ Using Client ID: {client_id[:20]}...")
    print(f"✅ Using Client Secret: {client_secret[:10]}...")
    
    try:
        # Create the OAuth flow configuration
        client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"]
            }
        }
        
        # Create the flow
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        
        print("\n🌐 Starting OAuth flow...")
        print("📱 Your browser will open for authentication")
        print("🔐 Please sign in with your Gmail account and grant permissions")
        
        # Run the OAuth flow - this will open browser
        creds = flow.run_local_server(port=0)
        
        print("\n🎉 Success! Authentication completed!")
        
        # Display the credentials
        print("\n" + "=" * 50)
        print("📋 YOUR NEW GMAIL API CREDENTIALS")
        print("=" * 50)
        print(f"GMAIL_CLIENT_ID={client_id}")
        print(f"GMAIL_CLIENT_SECRET={client_secret}")
        print(f"GMAIL_REFRESH_TOKEN={creds.refresh_token}")
        print("=" * 50)
        
        # Save to a file for convenience
        with open('new_credentials.env', 'w') as f:
            f.write(f"GMAIL_CLIENT_ID={client_id}\n")
            f.write(f"GMAIL_CLIENT_SECRET={client_secret}\n")
            f.write(f"GMAIL_REFRESH_TOKEN={creds.refresh_token}\n")
            f.write(f"GMAIL_USER=hankyotenki@gmail.com\n")
            f.write(f"WEBHOOK_URL=https://y8xp2r4oy7i.jp.larksuite.com/base/automation/webhook/event/DuuGaDaKVw5FCFhFKogjybwepic\n")
            f.write(f"PORT=5000\n")
        
        print(f"💾 Credentials also saved to: new_credentials.env")
        print(f"📝 You can copy this file over your .env file")
        
        return creds
        
    except Exception as e:
        print(f"\n❌ Error generating refresh token: {str(e)}")
        
        if "invalid_client" in str(e):
            print("\n🔍 Troubleshooting 'invalid_client' error:")
            print("1. ✅ Double-check your Client ID is correct")
            print("2. ✅ Double-check your Client Secret is correct") 
            print("3. ✅ Make sure you created a 'Desktop Application' (not Web or Mobile)")
            print("4. ✅ Verify Gmail API is enabled in your Google Cloud project")
            print("5. ✅ Check that OAuth consent screen is configured")
            
        elif "redirect_uri_mismatch" in str(e):
            print("\n🔍 Redirect URI issue:")
            print("Make sure you're using Desktop Application type")
            
        return None

def test_credentials(creds):
    """Test the generated credentials"""
    if not creds:
        return False
        
    try:
        from googleapiclient.discovery import build
        
        print("\n🧪 Testing the new credentials...")
        
        # Build Gmail service
        service = build('gmail', 'v1', credentials=creds)
        
        # Test by getting profile
        profile = service.users().getProfile(userId='me').execute()
        
        print("✅ Credentials test successful!")
        print(f"📧 Connected to: {profile.get('emailAddress')}")
        print(f"📊 Total messages: {profile.get('messagesTotal')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Credentials test failed: {str(e)}")
        return False

def main():
    """Main function"""
    print("This script will help you generate new Gmail API credentials.")
    print("Make sure you have:")
    print("1. ✅ Created a Google Cloud Project")
    print("2. ✅ Enabled Gmail API")
    print("3. ✅ Created OAuth 2.0 Desktop Application credentials")
    print("4. ✅ Have your Client ID and Client Secret ready")
    
    proceed = input("\n❓ Ready to proceed? (y/n): ").lower().strip()
    if proceed != 'y':
        print("👋 Exiting. Run this script when you're ready!")
        return
    
    # Generate refresh token
    creds = get_refresh_token()
    
    if creds:
        # Test the credentials
        if test_credentials(creds):
            print("\n🎉 All done! Your Gmail API is ready to use.")
            print("\n📋 Next steps:")
            print("1. 📝 Copy the credentials to your .env file")
            print("2. 🧪 Run: python test_gmail.py")
            print("3. 🚀 Run: python start.py")
        else:
            print("\n⚠️  Credentials generated but test failed. Check the setup.")
    else:
        print("\n❌ Failed to generate credentials. Please check the setup and try again.")

if __name__ == '__main__':
    main()