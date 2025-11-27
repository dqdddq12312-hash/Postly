"""
Authentication and user management module for Postly
Handles user registration, email verification, login, and password management
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv
import requests
from flask import session, redirect, url_for, flash

load_dotenv()

class PasswordHelper:
    """Helper class for password hashing and verification"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using PBKDF2"""
        salt = secrets.token_hex(32)
        iterations = 100000
        hash_obj = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), iterations)
        return f"{salt}${hash_obj.hex()}"
    
    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """Verify a password against its hash"""
        try:
            salt, hash_hex = hashed.split('$')
            iterations = 100000
            hash_obj = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), iterations)
            return hash_obj.hex() == hash_hex
        except Exception as e:
            print(f"Error verifying password: {e}")
            return False


class EmailService:
    """Service for sending emails via SendGrid API (HTTP-based, works on Render)"""
    
    @staticmethod
    def _send_sendgrid_email(recipient: str, subject: str, html_body: str) -> bool:
        """Internal method to send email via SendGrid HTTP API"""
        try:
            sendgrid_api_key = os.getenv('SENDGRID_API_KEY')
            sender_email = os.getenv('SENDER_EMAIL')
            
            # Validate configuration
            if not sendgrid_api_key:
                print("[EMAIL] ERROR: SENDGRID_API_KEY environment variable not set")
                return False
            if not sender_email:
                print("[EMAIL] ERROR:  environment variable not set")
                return False
            
            # SendGrid API endpoint
            sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
            
            # Prepare request headers and data
            headers = {
                "Authorization": f"Bearer {sendgrid_api_key}",
                "Content-Type": "application/json"
            }
            
            data = {
                "personalizations": [
                    {
                        "to": [{"email": recipient}],
                        "subject": subject
                    }
                ],
                "from": {"email": sender_email},
                "content": [
                    {
                        "type": "text/html",
                        "value": html_body
                    }
                ]
            }
            
            print(f"[EMAIL] Sending email to {recipient} via SendGrid")
            response = requests.post(sendgrid_url, json=data, headers=headers, timeout=10)
            
            if response.status_code == 202:
                print(f"[EMAIL] Email sent successfully to {recipient}")
                return True
            else:
                print(f"[EMAIL] SendGrid API error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            print("[EMAIL] ERROR: SendGrid request timeout (10 seconds)")
            return False
        except requests.exceptions.RequestException as e:
            print(f"[EMAIL] ERROR: Network error sending email: {e}")
            return False
        except Exception as e:
            print(f"[EMAIL] Unexpected error sending email: {type(e).__name__}: {e}")
            return False
    
    @staticmethod
    def send_verification_email(user_email: str, verification_code: str, app_url: str) -> bool:
        """Send verification email to user"""
        verification_url = f"{app_url}/verify-email/{verification_code}"
        
        subject = "Verify your Postly email"
        html_body = f"""
        <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background-color: #f5f5f5; padding: 20px;">
                    <h2 style="color: #333;">Welcome to Postly!</h2>
                    <p style="color: #666; font-size: 16px;">
                        Thank you for signing up. Please verify your email address by clicking the button below.
                    </p>
                    <a href="{verification_url}" 
                       style="display: inline-block; padding: 12px 30px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; margin: 20px 0;">
                        Verify Email Address
                    </a>
                    <p style="color: #999; font-size: 12px; margin-top: 30px;">
                        If you didn't create this account, you can safely ignore this email.
                    </p>
                    <p style="color: #999; font-size: 12px;">
                        Or copy and paste this link: <br/>{verification_url}
                    </p>
                </div>
            </body>
        </html>
        """
        
        return EmailService._send_sendgrid_email(user_email, subject, html_body)
    
    @staticmethod
    def send_team_invitation_email(recipient_email: str, inviter_name: str, team_name: str, 
                                   invitation_code: str, app_url: str) -> bool:
        """Send team invitation email to user"""
        invitation_url = f"{app_url}/accept-invitation/{invitation_code}"
        
        subject = f"{inviter_name} invited you to {team_name} on Postly"
        html_body = f"""
        <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background-color: #f5f5f5; padding: 20px;">
                    <h2 style="color: #333;">You're invited to {team_name}!</h2>
                    <p style="color: #666; font-size: 16px;">
                        {inviter_name} has invited you to join <strong>{team_name}</strong> on Postly.
                    </p>
                    <a href="{invitation_url}" 
                       style="display: inline-block; padding: 12px 30px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px; margin: 20px 0;">
                        Accept Invitation
                    </a>
                    <p style="color: #999; font-size: 12px; margin-top: 30px;">
                        If you didn't expect this invitation, you can safely ignore this email.
                    </p>
                </div>
            </body>
        </html>
        """
        
        return EmailService._send_sendgrid_email(recipient_email, subject, html_body)


def login_required(f):
    """Decorator to require login for a route"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def team_access_required(f):
    """Decorator to require team access for a route"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'team_id' not in session:
            flash('Please select a team to continue', 'warning')
            return redirect(url_for('select_team'))
        return f(*args, **kwargs)
    return decorated_function