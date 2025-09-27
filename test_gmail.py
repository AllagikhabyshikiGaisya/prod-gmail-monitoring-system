#!/usr/bin/env python3
"""
Test Gmail API connection
Run this script to verify your Gmail credentials work before starting the main application
"""

import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

def load_env_file():
    """Load environment variables from .env file"""
    env_file = Path('.env')
    
    if not env_file.exists():
        print("❌ .env file not found!")
        return False
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    
    return True

def test_gmail_connection():
    """Test Gmail API connection"""
    print("🔧 Testing Gmail API connection...")
    
    try:
        # Create credentials
        creds = Credentials(
            token=None,
            refresh_token=os.environ['GMAIL_REFRESH_TOKEN'],
            id_token=None,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.environ['GMAIL_CLIENT_ID'],
            client_secret=os.environ['GMAIL_CLIENT_SECRET'],
            scopes=['https://www.googleapis.com/auth/gmail.modify']
        )
        
        print("🔄 Refreshing access token...")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        
        print("🔗 Building Gmail service...")
        service = build('gmail', 'v1', credentials=creds)
        
        print("📋 Getting profile information...")
        profile = service.users().getProfile(userId='me').execute()
        
        print("📧 Testing inbox access...")
        messages = service.users().messages().list(userId='me', maxResults=5).execute()
        message_count = len(messages.get('messages', []))
        
        print("✅ Gmail API connection successful!")
        print(f"   📮 Email: {profile.get('emailAddress')}")
        print(f"   📊 Total messages: {profile.get('messagesTotal', 'Unknown')}")
        print(f"   📥 Recent messages found: {message_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Gmail API connection failed: {str(e)}")
        print("\nPossible issues:")
        print("1. Invalid refresh token - may have expired")
        print("2. Incorrect client ID or client secret")
        print("3. Gmail API not enabled in Google Cloud Console")
        print("4. Network connectivity issues")
        
        return False

def test_webhook():
    """Test webhook connectivity"""
    print("\n🔗 Testing webhook connection...")
    
    import requests
    import json
    
    webhook_url = os.environ.get('WEBHOOK_URL')
    if not webhook_url:
        print("❌ WEBHOOK_URL not set")
        return False
    
    test_data = {
        "test": True,
        "message": "Test from Gmail Monitor",
        "timestamp": "2025-09-27T12:00:00Z"
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Webhook connection successful!")
            print(f"   📡 Response: {response.status_code}")
            return True
        else:
            print(f"⚠️  Webhook responded with status: {response.status_code}")
            print(f"   📄 Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Webhook request timed out")
        return False
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to webhook URL")
        return False
    except Exception as e:
        print(f"❌ Webhook test failed: {str(e)}")
        return False

def main():
    """Main test function"""
    print("🧪 Gmail Monitor - Connection Test")
    print("=" * 40)
    
    # Load environment variables
    if not load_env_file():
        return
    
    print("✅ Environment variables loaded")
    
    # Check required variables
    required_vars = ['GMAIL_CLIENT_ID', 'GMAIL_CLIENT_SECRET', 'GMAIL_REFRESH_TOKEN', 'GMAIL_USER', 'WEBHOOK_URL']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return
    
    print("✅ All required environment variables found")
    
    # Test Gmail connection
    gmail_ok = test_gmail_connection()
    
    # Test webhook connection
    webhook_ok = test_webhook()
    
    print("\n" + "=" * 40)
    print("📋 Test Summary:")
    print(f"   Gmail API: {'✅ OK' if gmail_ok else '❌ FAILED'}")
    print(f"   Webhook:   {'✅ OK' if webhook_ok else '❌ FAILED'}")
    
    if gmail_ok and webhook_ok:
        print("\n🎉 All tests passed! You can now run the main application:")
        print("   python start.py")
    else:
        print("\n⚠️  Please fix the failed connections before running the main application.")

if __name__ == '__main__':
    main()