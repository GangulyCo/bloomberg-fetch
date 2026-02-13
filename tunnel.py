#!/usr/bin/env python3
"""
Simple script to tunnel localhost:8194 to a public URL using ngrok
"""

from pyngrok import ngrok, conf
import time
import json
import os
import sys

def load_config():
    """Load configuration from config.json"""
    config_file = os.path.join(os.path.dirname(__file__), 'config.json')
    
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            return json.load(f)
    else:
        print(f"Config file not found: {config_file}")
        print("Creating example config file...")
        example_config = {
            "ngrok_authtoken": "YOUR_NGROK_AUTH_TOKEN_HERE"
        }
        with open(config_file, 'w') as f:
            json.dump(example_config, f, indent=2)
        print(f"Please edit {config_file} and add your ngrok authtoken")
        return None

def start_tunnel(port=8194):
    """
    Start an ngrok tunnel to the specified localhost port
    
    Args:
        port: The localhost port to tunnel (default: 8194)
    """
    try:
        # Load config and set authtoken
        config = load_config()
        if not config:
            return
        
        authtoken = config.get('ngrok_authtoken')
        if not authtoken or authtoken == "YOUR_NGROK_AUTH_TOKEN_HERE":
            print("Error: Please set your ngrok authtoken in config.json")
            print("Get your token from: https://dashboard.ngrok.com/get-started/your-authtoken")
            return
        
        # Check if ngrok.exe is in the same directory as this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_ngrok = os.path.join(script_dir, 'ngrok.exe' if sys.platform == 'win32' else 'ngrok')
        
        if os.path.exists(local_ngrok):
            conf.get_default().ngrok_path = local_ngrok
            print(f"Using ngrok binary from script directory: {local_ngrok}")
            
            # Set config directory to user home to avoid permission issues
            config_dir = os.path.join(os.path.expanduser('~'), '.ngrok2')
            os.makedirs(config_dir, exist_ok=True)
            conf.get_default().config_path = os.path.join(config_dir, 'ngrok.yml')
            print(f"Using config directory: {config_dir}")
        else:
            # Set ngrok binary to a user-writable location
            pyngrok_config = conf.get_default()
            bin_dir = os.path.join(os.path.expanduser('~'), '.ngrok2')
            os.makedirs(bin_dir, exist_ok=True)
            pyngrok_config.ngrok_path = os.path.join(bin_dir, 'ngrok.exe' if sys.platform == 'win32' else 'ngrok')
            print(f"Using ngrok binary location: {pyngrok_config.ngrok_path}")
        
        # Set ngrok authtoken
        ngrok.set_auth_token(authtoken)
        
        # Start ngrok tunnel
        print(f"Starting ngrok tunnel for localhost:{port}...")
        public_url = ngrok.connect(port)
        
        print(f"\n{'='*60}")
        print(f"Tunnel established successfully!")
        print(f"{'='*60}")
        print(f"Local URL:  http://localhost:{port}")
        print(f"Public URL: {public_url}")
        print(f"{'='*60}\n")
        print("PrTroubleshooting:")
        print("1. Make sure pyngrok is installed: pip install pyngrok")
        print("2. Download ngrok from https://ngrok.com/download")
        print("   and place ngrok.exe in the same folder as this script")
        print("3. On Windows, try running as Administrator")
        print("4. Check your antivirus isn't blocking ngrok")
        print("5. Verify your authtoken in config.json is correct")
        # Keep the tunnel alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nStopping tunnel...")
            ngrok.disconnect(public_url)
            print("Tunnel stopped.")
            
    except Exception as e:
        print(f"Error starting tunnel: {e}")
        print("\nMake sure ngrok is installed and configured.")
        print("Install with: pip install pyngrok")

if __name__ == "__main__":
    start_tunnel()
