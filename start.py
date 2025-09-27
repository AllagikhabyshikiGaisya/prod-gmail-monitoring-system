#!/usr/bin/env python3
"""
Startup script for Gmail Email Monitor
This script loads environment variables from .env file and starts the application
"""

import os
import sys
from pathlib import Path

def load_env_file():
    """Load environment variables from .env file if it exists"""
    env_file = Path('.env')
    
    if not env_file.exists():
        print("📁 No .env file found - using system environment variables")
        print("This is normal for Docker/cloud deployments")
        return
    
    print("📁 Loading environment variables from .env file...")
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
                    print(f"   ✓ {key}=***")
    
    print("✅ Environment variables loaded successfully!")

def check_required_env_vars():
    """Check if all required environment variables are set"""
    required_vars = [
        'GMAIL_CLIENT_ID',
        'GMAIL_CLIENT_SECRET', 
        'GMAIL_REFRESH_TOKEN',
        'GMAIL_USER',
        'WEBHOOK_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your environment variables configuration.")
        print("Required variables:")
        for var in required_vars:
            status = "✅" if os.environ.get(var) else "❌"
            print(f"  {status} {var}")
        sys.exit(1)
    
    print("✅ All required environment variables are set!")

def main():
    """Main startup function"""
    print("🚀 Starting Gmail Email Monitor...")
    print("=" * 50)
    
    # Load environment variables
    load_env_file()
    
    # Check required variables
    check_required_env_vars()
    
    # Import and start the main application
    print("📧 Initializing email monitoring system...")
    try:
        import main
        main.main()
    except ImportError as e:
        print(f"❌ Error importing main module: {e}")
        print("Make sure all dependencies are installed: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()