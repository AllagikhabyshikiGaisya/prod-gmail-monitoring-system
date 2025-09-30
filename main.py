import os
import json
import time
import re
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from flask import Flask, render_template, jsonify
import threading
from logging.handlers import RotatingFileHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Deduplication configuration (in hours)
DEDUPLICATION_WINDOW_HOURS = int(os.environ.get('DEDUP_WINDOW_HOURS', '24'))

class DatabaseManager:
    def __init__(self, db_path='email_monitor.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # FIRST: Create all tables if they don't exist
        # Table for processed emails
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                sender_email TEXT,
                subject TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                extracted_data TEXT,
                webhook_sent BOOLEAN DEFAULT FALSE,
                archived BOOLEAN DEFAULT FALSE,
                customer_email TEXT,
                submission_type TEXT,
                is_duplicate BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # Table for customer deduplication tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_email TEXT NOT NULL,
                submission_type TEXT NOT NULL,
                email_subject TEXT,
                first_submission_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_submission_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                submission_count INTEGER DEFAULT 1,
                first_message_id TEXT,
                last_message_id TEXT
            )
        ''')
        
        # Index for faster customer email lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_customer_submission 
            ON customer_submissions(customer_email, submission_type)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_customer_last_submission 
            ON customer_submissions(customer_email, submission_type, last_submission_at)
        ''')
        
        # Table for logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT,
                category TEXT
            )
        ''')
        
        # Table for errors
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_type TEXT,
                error_message TEXT,
                stack_trace TEXT
            )
        ''')
        
        # Commit table creation before migration
        conn.commit()
        
        # SECOND: Now that tables exist, run migrations for existing databases
        self._migrate_existing_tables(cursor)
        
        conn.commit()
        conn.close()
    
    def _migrate_existing_tables(self, cursor):
        """Migrate existing tables to add new columns"""
        try:
            # Check if customer_email column exists in processed_emails
            cursor.execute("PRAGMA table_info(processed_emails)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'customer_email' not in columns:
                logger.info("Migrating database: Adding customer_email column...")
                cursor.execute('''
                    ALTER TABLE processed_emails 
                    ADD COLUMN customer_email TEXT
                ''')
            
            if 'submission_type' not in columns:
                logger.info("Migrating database: Adding submission_type column...")
                cursor.execute('''
                    ALTER TABLE processed_emails 
                    ADD COLUMN submission_type TEXT
                ''')
            
            if 'is_duplicate' not in columns:
                logger.info("Migrating database: Adding is_duplicate column...")
                cursor.execute('''
                    ALTER TABLE processed_emails 
                    ADD COLUMN is_duplicate BOOLEAN DEFAULT FALSE
                ''')
            
            # Check if submission_type column exists in customer_submissions
            cursor.execute("PRAGMA table_info(customer_submissions)")
            submission_columns = [column[1] for column in cursor.fetchall()]
            
            if 'submission_type' not in submission_columns:
                logger.info("Migrating database: Adding submission_type column...")
                cursor.execute('''
                    ALTER TABLE customer_submissions 
                    ADD COLUMN submission_type TEXT DEFAULT 'unknown'
                ''')
            
            if 'email_subject' not in submission_columns:
                logger.info("Migrating database: Adding email_subject column...")
                cursor.execute('''
                    ALTER TABLE customer_submissions 
                    ADD COLUMN email_subject TEXT
                ''')
            
            logger.info("Database migration check completed")
            
        except Exception as e:
            logger.error(f"Error during database migration: {str(e)}")
            raise
    
    def is_customer_recently_processed(self, customer_email: str, submission_type: str, email_subject: str, window_hours: int = DEDUPLICATION_WINDOW_HOURS) -> Dict:
        """
        Check if customer email was processed recently for the same submission type AND subject
        Returns duplicate only if BOTH email AND subject match within the window
        """
        if not customer_email or customer_email == "":
            return {'is_duplicate': False, 'previous_submission': None}
        
        # Normalize submission type and subject for comparison
        if not submission_type or submission_type == "":
            submission_type = "unknown"
        
        normalized_subject = self._normalize_subject(email_subject)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate the time threshold
        threshold = datetime.now() - timedelta(hours=window_hours)
        threshold_str = threshold.strftime('%Y-%m-%d %H:%M:%S')
        
        # Check for duplicate with SAME customer email AND SAME submission type (subject-based)
        cursor.execute('''
            SELECT customer_email, submission_type, email_subject, first_submission_at, 
                   last_submission_at, submission_count, first_message_id
            FROM customer_submissions 
            WHERE customer_email = ? 
              AND submission_type = ? 
              AND last_submission_at > ?
            ORDER BY last_submission_at DESC
            LIMIT 1
        ''', (customer_email, submission_type, threshold_str))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'is_duplicate': True,
                'previous_submission': {
                    'customer_email': result[0],
                    'submission_type': result[1],
                    'email_subject': result[2],
                    'first_submission_at': result[3],
                    'last_submission_at': result[4],
                    'submission_count': result[5],
                    'first_message_id': result[6]
                }
            }
        
        return {'is_duplicate': False, 'previous_submission': None}
    
    def _normalize_subject(self, subject: str) -> str:
        """Normalize subject for comparison - remove extra whitespace and convert to lowercase"""
        if not subject:
            return ""
        return re.sub(r'\s+', ' ', subject.strip().lower())
    
    def record_customer_submission(self, customer_email: str, submission_type: str, email_subject: str, message_id: str):
        """Record or update customer submission for a specific type"""
        if not customer_email or customer_email == "":
            return
        
        if not submission_type or submission_type == "":
            submission_type = "unknown"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if customer exists for this submission type
        cursor.execute('''
            SELECT id, submission_count, first_message_id 
            FROM customer_submissions 
            WHERE customer_email = ? AND submission_type = ?
        ''', (customer_email, submission_type))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update existing record
            cursor.execute('''
                UPDATE customer_submissions 
                SET last_submission_at = CURRENT_TIMESTAMP,
                    submission_count = submission_count + 1,
                    last_message_id = ?,
                    email_subject = ?
                WHERE customer_email = ? AND submission_type = ?
            ''', (message_id, email_subject, customer_email, submission_type))
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO customer_submissions 
                (customer_email, submission_type, email_subject, first_message_id, last_message_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (customer_email, submission_type, email_subject, message_id, message_id))
        
        conn.commit()
        conn.close()
    
    def add_processed_email(self, message_id: str, sender_email: str, subject: str, 
                           extracted_data: Dict, submission_type: str, is_duplicate: bool = False):
        """Add a processed email to the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        customer_email = extracted_data.get('顧客メール', '')
        
        cursor.execute('''
            INSERT OR REPLACE INTO processed_emails 
            (message_id, sender_email, subject, extracted_data, customer_email, submission_type, is_duplicate)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (message_id, sender_email, subject, json.dumps(extracted_data), customer_email, submission_type, is_duplicate))
        
        conn.commit()
        conn.close()
    
    def is_email_processed(self, message_id: str) -> bool:
        """Check if email has already been processed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM processed_emails WHERE message_id = ?', (message_id,))
        result = cursor.fetchone()
        conn.close()
        
        return result is not None
    
    def mark_webhook_sent(self, message_id: str):
        """Mark that webhook was sent for this email"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE processed_emails SET webhook_sent = TRUE WHERE message_id = ?
        ''', (message_id,))
        
        conn.commit()
        conn.close()
    
    def mark_archived(self, message_id: str):
        """Mark email as archived"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE processed_emails SET archived = TRUE WHERE message_id = ?
        ''', (message_id,))
        
        conn.commit()
        conn.close()
    
    def add_log(self, level: str, message: str, category: str = 'general'):
        """Add a log entry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO logs (level, message, category) VALUES (?, ?, ?)
        ''', (level, message, category))
        
        conn.commit()
        conn.close()
    
    def add_error(self, error_type: str, error_message: str, stack_trace: str = ''):
        """Add an error entry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO errors (error_type, error_message, stack_trace) VALUES (?, ?, ?)
        ''', (error_type, error_message, stack_trace))
        
        conn.commit()
        conn.close()
    
    def get_recent_logs(self, limit: int = 100):
        """Get recent logs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, level, message, category 
            FROM logs ORDER BY timestamp DESC LIMIT ?
        ''', (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return [{'timestamp': row[0], 'level': row[1], 'message': row[2], 'category': row[3]} 
                for row in results]
    
    def get_recent_errors(self, limit: int = 50):
        """Get recent errors"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, error_type, error_message, stack_trace 
            FROM errors ORDER BY timestamp DESC LIMIT ?
        ''', (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return [{'timestamp': row[0], 'error_type': row[1], 'error_message': row[2], 'stack_trace': row[3]} 
                for row in results]
    
    def get_processed_emails_stats(self):
        """Get statistics about processed emails"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM processed_emails')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM processed_emails WHERE webhook_sent = TRUE')
        webhook_sent = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM processed_emails WHERE archived = TRUE')
        archived = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM processed_emails WHERE is_duplicate = TRUE')
        duplicates_skipped = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT customer_email) FROM customer_submissions WHERE customer_email != ""')
        unique_customers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT submission_type) FROM customer_submissions WHERE submission_type != ""')
        unique_submission_types = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_processed': total,
            'webhook_sent': webhook_sent,
            'archived': archived,
            'duplicates_skipped': duplicates_skipped,
            'unique_customers': unique_customers,
            'unique_submission_types': unique_submission_types
        }
    
    def get_customer_submission_history(self, customer_email: str):
        """Get submission history for a specific customer"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM customer_submissions WHERE customer_email = ?
        ''', (customer_email,))
        
        result = cursor.fetchall()
        conn.close()
        
        return result

class EmailDataExtractor:
    """Extract structured data from email content"""
    
    def extract_data(self, email_content: str, sender_email: str, subject: str, message_id: str) -> Dict:
        """Extract structured data from email content"""
        
        # Extract customer email from content (メールアドレス field)
        customer_email = self._extract_email_address_from_content(email_content)
        
        # Base template matching your JSON format
        extracted_data = {
            "sender_email": sender_email,
            "subject": subject,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "問合せ番号": self._extract_inquiry_number(email_content, subject),
            "問合せ日時": self._extract_inquiry_datetime(email_content),
            "名前": self._extract_name(email_content),
            "郵便番号": self._extract_postal_code(email_content),
            "住所": self._extract_address(email_content),
            "電話番号": self._extract_phone_number(email_content),
            "電話番号_2": "",
            "きっかけ": self._extract_trigger(email_content),
            "件名": subject,
            "タイトル": self._extract_title(email_content, subject),
            "URL": self._extract_url(email_content),
            "希望時間": self._extract_preferred_time(email_content),
            "ふりがな": self._extract_furigana(email_content),
            "message_id": message_id,
            "顧客メール": customer_email,
            "来場希望日": self._extract_visit_date(email_content),
            "希望訪問時間": self._extract_visit_time(email_content)
        }
        
        return extracted_data
    
    def _extract_inquiry_number(self, content: str, subject: str) -> str:
        """Extract inquiry number from content or generate one"""
        pattern = r'[A-Z]{2,4}-\d{3,6}'
        match = re.search(pattern, content)
        if match:
            return match.group()
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        return f"INQ-{timestamp}"
    
    def _extract_inquiry_datetime(self, content: str) -> str:
        """Extract inquiry datetime"""
        date_patterns = [
            r'(\d{4})[/年](\d{1,2})[/月](\d{1,2})[\s日]*(\d{1,2}):(\d{2})',
            r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, content)
            if match:
                year, month, day, hour, minute = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour}:{minute}"
        
        return datetime.now().strftime("%Y-%m-%d %H:%M")
    
    def _extract_name(self, content: str) -> str:
        """Extract customer name"""
        patterns = [
            r'▼お名前▼\s*([^\n\r▼]+)',
            r'お名前[\s:：]*([^\n\r▼]+)',
            r'氏名[\s:：]*([^\n\r▼]+)',
            r'名前[\s:：]*([^\n\r▼]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                name = match.group(1).strip()
                name = re.sub(r'[】【\[\]()（）▼]', '', name)
                name = re.sub(r'[\s]+', ' ', name)
                name = name.strip()
                return name
        
        return ""
    
    def _extract_email_address_from_content(self, content: str) -> str:
        """Extract email address from email content (メールアドレス field)"""
        patterns = [
            r'【メールアドレス】\s*([^\n\r\s【】]+@[^\n\r\s【】]+)',
            r'▼メールアドレス▼\s*([^\n\r\s▼]+@[^\n\r\s▼]+)',
            r'メールアドレス[\s:：]*([^\n\r\s▼]+@[^\n\r\s▼]+)',
            r'E-mail[\s:：]*([^\n\r\s▼]+@[^\n\r\s▼]+)',
            r'Email[\s:：]*([^\n\r\s▼]+@[^\n\r\s▼]+)',
            r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                email = match.group(1).strip()
                email = re.sub(r'[▼【】\s]', '', email)
                return email.lower()  # Normalize to lowercase
        
        return ""
    
    def _extract_postal_code(self, content: str) -> str:
        """Extract postal code"""
        patterns = [
            r'▼郵便番号▼\s*([〒]?\d{3}-?\d{4})',
            r'郵便番号[\s:：]*([〒]?\d{3}-?\d{4})',
            r'【郵便番号】[\s　]*([〒]?\d{3}-?\d{4})',
            r'〒(\d{3}-?\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                postal = match.group(1).replace('〒', '')
                postal = re.sub(r'[】【\[\]()（）▼\s]', '', postal)
                postal = postal.strip()
                if '-' not in postal and len(postal) == 7:
                    postal = postal[:3] + '-' + postal[3:]
                return postal
        
        return ""
    
    def _extract_address(self, content: str) -> str:
        """Extract address"""
        patterns = [
            r'▼ご住所▼\s*([^\n\r▼]+)',
            r'ご住所[\s:：]*([^\n\r▼]+)',
            r'住所[\s:：]*([^\n\r▼]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                address = match.group(1).strip()
                address = re.sub(r'[】【\[\]()（）▼]', '', address)
                address = re.sub(r'[\s]+', ' ', address)
                address = address.strip()
                return address
        
        return ""
    
    def _extract_phone_number(self, content: str) -> str:
        """Extract phone number"""
        patterns = [
            r'【電話番号1】\s*([0-9\-]+)',
            r'【電話番号】\s*([0-9\-]+)',
            r'▼電話番号▼\s*([0-9\-]+)',
            r'電話番号[\s:：]*([0-9\-]+)',
            r'【電話番号】[\s:：]*([0-9\-]+)',
            r'TEL[\s:：]*([0-9\-]+)',
            r'Tel[\s:：]*([0-9\-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                phone = match.group(1).strip()
                phone = re.sub(r'[】【\[\]()（）▼\s]', '', phone)
                return phone
        
        return ""
    
    def _extract_trigger(self, content: str) -> str:
        """Extract what triggered the inquiry"""
        patterns = [
            r'▼会員登録のきっかけ▼\s*([^\n\r▼]+)',
            r'▼予約のきっかけ▼\s*([^\n\r▼]+)',
            r'予約のきっかけ[\s:：]*([^\n\r▼]+)',
            r'きっかけ[\s:：]*([^\n\r▼]+)',
            r'経由[\s:：]*([^\n\r▼]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                trigger = match.group(1).strip()
                trigger = re.sub(r'[】【\[\]()（）▼]', '', trigger)
                trigger = re.sub(r'[\s]+', ' ', trigger)
                trigger = trigger.strip()
                return trigger
        
        trigger_map = {
            'HP検索': 'ウェブサイト',
            'インスタグラム': 'インスタグラム',
            'Facebook': 'Facebook',
            'Google': 'ウェブサイト',
            'チラシ': 'チラシ',
            '紹介': '紹介',
            'ホームページ': 'ウェブサイト',
            'ネット': 'ウェブサイト',
            'みのおキューズモール広告': 'みのおキューズモール広告'
        }
        
        for key, value in trigger_map.items():
            if key in content:
                return value
        
        return "ウェブサイト"
    
    def _extract_title(self, content: str, subject: str) -> str:
        """Extract or generate title based on email content and sender"""
        content_lower = content.lower()
        subject_lower = subject.lower()

        if any(keyword in content for keyword in ['会員登録', 'バーズハウス'] + ['会員登録がありました']):
            return "[バーズハウス] 会員登録がありました"
        
        if any(keyword in content for keyword in ['来場予約', '来場希望', '見学予約', '見学希望', 'イベント参加', 'イベント申込']):
            return "[桧家住宅] イベントの参加お申し込みがありました"
        
        if any(keyword in content for keyword in ['会員情報変更', '情報変更', '会員情報更新']):
            return "[桧家住宅] 会員情報の変更がありました"
        
        if any(keyword in content for keyword in ['退会', '会員退会', '退会しました']):
            return "[桧家住宅] 会員の退会がありました"
        
        if any(keyword in content for keyword in ['分譲住宅', '物件', '住宅', '問い合わせ', '問合せ']):
            return "[桧家住宅] 分譲住宅へのお問い合わせがありました"
        
        if any(keyword in content for keyword in ['資料請求', '資料希望', 'カタログ請求']):
            return "[桧家住宅] 資料請求がありました"
        
        if any(keyword in content for keyword in ['お問い合わせフォーム', 'コンタクト', 'フォーム']):
            return "[桧家住宅] お問い合わせフォームからの連絡がありました"
        
        if any(keyword in subject_lower for keyword in ['イベント', 'event', '参加', '申込', '申し込み']):
            return "[桧家住宅] イベントの参加お申し込みがありました"
        
        if any(keyword in subject_lower for keyword in ['問い合わせ', '問合せ', 'inquiry', 'contact']):
            return "[桧家住宅] お問い合わせがありました"
        
        return "[桧家住宅] お問い合わせがありました"
    
    def _extract_url(self, content: str) -> str:
        """Extract URL if present"""
        url_pattern = r'https?://[^\s\n\r]+'
        match = re.search(url_pattern, content)
        if match:
            return match.group()
        return "https://example.com/123"
    
    def _extract_preferred_time(self, content: str) -> str:
        """Extract preferred time"""
        time_patterns = [
            r'ご希望時間[\s:：]*([^\n\r]+)',
            r'希望時間[\s:：]*([^\n\r]+)',
            r'時間[\s:：]*([^\n\r]+)',
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, content)
            if match:
                time_text = match.group(1).strip()
                if any(x in time_text for x in ['9:', '10:', '11:', '午前', 'AM']):
                    return '午前'
                elif any(x in time_text for x in ['12:', '13:', '14:', '15:', '16:', '17:', '18:', '午後', 'PM']):
                    return '午後'
                else:
                    return time_text
        
        if any(x in content for x in ['午前', 'AM', '9:', '10:', '11:']):
            return '午前'
        elif any(x in content for x in ['午後', 'PM', '13:', '14:', '15:', '16:']):
            return '午後'
            
        return '午後'
    
    def _extract_furigana(self, content: str) -> str:
        """Extract furigana (phonetic reading)"""
        patterns = [
            r'▼フリガナ▼\s*([^\n\r▼]+)',
            r'フリガナ[\s:：]*([^\n\r▼]+)',
            r'ふりがな[\s:：]*([^\n\r▼]+)',
            r'カナ[\s:：]*([^\n\r▼]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                furigana = match.group(1).strip()
                furigana = re.sub(r'[】【\[\]()（）▼]', '', furigana)
                furigana = re.sub(r'[\s]+', ' ', furigana)
                furigana = furigana.strip()
                return furigana
        
        return ""
    
    def _extract_visit_date(self, content: str) -> str:
        """Extract desired visit date (来場希望日)"""
        patterns = [
            r'来場希望日[\s:：]*([^\n\r]+)',
            r'ご希望日[\s:：]*([^\n\r]+)',
            r'希望日[\s:：]*([^\n\r]+)',
            r'第1希望[\s:：]*([^\n\r]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                date_text = match.group(1).strip()
                date_text = re.sub(r'[】【\[\]()（）▼]', '', date_text)
                
                date_patterns = [
                    r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                    r'(\d{4})[/年](\d{1,2})[/月](\d{1,2})[日]?',
                    r'(\d{4})-(\d{1,2})-(\d{1,2})',
                ]
                
                for date_pattern in date_patterns:
                    date_match = re.search(date_pattern, date_text)
                    if date_match:
                        year, month, day = date_match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                
                return date_text
        
        return ""
    
    def _extract_visit_time(self, content: str) -> str:
        """Extract desired visit time in HH:MM format"""
        patterns = [
            r'ご希望時間[\s:：]*([^\n\r]+)',
            r'希望時間[\s:：]*([^\n\r]+)',
            r'時間[\s:：]*([^\n\r]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                time_text = match.group(1).strip()
                time_text = re.sub(r'[】【\[\]()（）▼]', '', time_text)
                
                time_patterns = [
                    r'(\d{1,2}):(\d{2})(?:～|〜|-|から)?(?:\d{1,2}:\d{2})?',
                    r'(\d{1,2})時(\d{2})分',
                    r'(\d{1,2})[:：時](\d{2})',
                ]
                
                for time_pattern in time_patterns:
                    time_match = re.search(time_pattern, time_text)
                    if time_match:
                        hour, minute = time_match.groups()
                        return f"{hour.zfill(2)}:{minute.zfill(2)}"
                
                return time_text
        
        return ""

class GmailEmailMonitor:
    def __init__(self):
        self.db = DatabaseManager()
        self.extractor = EmailDataExtractor()
        self.service = None
        self.setup_gmail_service()
    
    def setup_gmail_service(self):
        """Setup Gmail API service using OAuth2 credentials"""
        try:
            creds = Credentials(
                token=None,
                refresh_token=os.environ['GMAIL_REFRESH_TOKEN'],
                id_token=None,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=os.environ['GMAIL_CLIENT_ID'],
                client_secret=os.environ['GMAIL_CLIENT_SECRET'],
                scopes=['https://www.googleapis.com/auth/gmail.modify']
            )
            
            from google.auth.transport.requests import Request
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            
            self.service = build('gmail', 'v1', credentials=creds)
            self.service.users().getProfile(userId='me').execute()
            
            logger.info("Gmail service initialized successfully")
            self.db.add_log('INFO', 'Gmail service initialized successfully', 'gmail')
            
        except Exception as e:
            error_msg = f"Failed to setup Gmail service: {str(e)}"
            logger.error(error_msg)
            self.db.add_error('Gmail Setup', error_msg, str(e))
            raise Exception(f"Gmail API setup failed: {str(e)}. Please check your credentials.")
    
    def get_latest_emails(self, max_results: int = 10) -> List[Dict]:
        """Get latest emails from Gmail inbox"""
        try:
            results = self.service.users().messages().list(
                userId='me',
                q='in:inbox',
                maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            emails = []
            
            for message in messages:
                message_id = message['id']
                
                if self.db.is_email_processed(message_id):
                    continue
                
                full_message = self.service.users().messages().get(
                    userId='me',
                    id=message_id,
                    format='full'
                ).execute()
                
                email_data = self._parse_email_message(full_message)
                if email_data:
                    emails.append(email_data)
            
            logger.info(f"Retrieved {len(emails)} new emails")
            self.db.add_log('INFO', f'Retrieved {len(emails)} new emails', 'email_scan')
            
            return emails
            
        except Exception as e:
            error_msg = f"Error getting emails: {str(e)}"
            logger.error(error_msg)
            self.db.add_error('Email Retrieval', error_msg, str(e))
            return []
    
    def _parse_email_message(self, message: Dict) -> Optional[Dict]:
        """Parse Gmail message into structured format"""
        try:
            headers = message['payload'].get('headers', [])
            
            subject = ''
            sender_email = ''
            date = ''
            
            for header in headers:
                if header['name'] == 'Subject':
                    subject = header['value']
                elif header['name'] == 'From':
                    sender_email = self._extract_email_address(header['value'])
                elif header['name'] == 'Date':
                    date = header['value']
            
            body = self._extract_email_body(message['payload'])
            
            if not body:
                return None
            
            return {
                'message_id': message['id'],
                'subject': subject,
                'sender_email': sender_email,
                'date': date,
                'body': body
            }
            
        except Exception as e:
            logger.error(f"Error parsing email message: {str(e)}")
            return None
    
    def _extract_email_address(self, from_field: str) -> str:
        """Extract just the email address from 'Name <email@domain.com>' format"""
        import re
        
        email_pattern = r'<([^>]+@[^>]+)>|([^\s<>]+@[^\s<>]+)'
        
        match = re.search(email_pattern, from_field)
        if match:
            return match.group(1) if match.group(1) else match.group(2)
        
        return from_field.strip()
    
    def _extract_email_body(self, payload: Dict) -> str:
        """Extract email body from payload"""
        body = ''
        
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    data = part['body']['data']
                    body = base64.urlsafe_b64decode(data).decode('utf-8')
                    break
        elif payload['mimeType'] == 'text/plain':
            data = payload['body']['data']
            body = base64.urlsafe_b64decode(data).decode('utf-8')
        
        return body
    
    def archive_email(self, message_id: str):
        """Archive an email by removing it from inbox"""
        try:
            self.service.users().messages().modify(
                userId='me',
                id=message_id,
                body={'removeLabelIds': ['INBOX']}
            ).execute()
            
            self.db.mark_archived(message_id)
            logger.info(f"Email {message_id} archived successfully")
            self.db.add_log('INFO', f'Email {message_id} archived successfully', 'archive')
            
        except Exception as e:
            error_msg = f"Error archiving email {message_id}: {str(e)}"
            logger.error(error_msg)
            self.db.add_error('Email Archive', error_msg, str(e))
    
    def process_emails(self):
        """
        Process new emails and send to webhook
        
        DEDUPLICATION LOGIC:
        - Blocks duplicate if: SAME customer email + SAME submission type (subject-based category)
        - Allows if: SAME customer email + DIFFERENT submission type
        - Example: customer@example.com can submit "[桧家住宅] イベント参加" and "[桧家住宅] 資料請求" separately
        """
        try:
            emails = self.get_latest_emails()
            
            for email in emails:
                try:
                    # Extract structured data
                    extracted_data = self.extractor.extract_data(
                        email['body'],
                        email['sender_email'],
                        email['subject'],
                        email['message_id']
                    )
                    
                    customer_email = extracted_data.get('顧客メール', '')
                    submission_type = extracted_data.get('タイトル', '')
                    email_subject = email['subject']
                    
                    # If no submission type extracted, use the email subject as fallback
                    if not submission_type:
                        submission_type = email_subject
                    
                    logger.info(f"📧 Processing email from: {customer_email}")
                    logger.info(f"   Subject: {email_subject}")
                    logger.info(f"   Type: {submission_type}")
                    
                    # Check for duplicate: SAME customer email + SAME submission type
                    dedup_result = self.db.is_customer_recently_processed(
                        customer_email,
                        submission_type,
                        email_subject,
                        DEDUPLICATION_WINDOW_HOURS
                    )
                    
                    if dedup_result['is_duplicate']:
                        # This is a duplicate: SAME email + SAME type
                        prev = dedup_result['previous_submission']
                        logger.warning(
                            f"🚫 DUPLICATE BLOCKED\n"
                            f"   Customer: {customer_email}\n"
                            f"   Type: {submission_type}\n"
                            f"   Subject: {email_subject}\n"
                            f"   Previous submission: {prev['last_submission_at']}\n"
                            f"   Total count: {prev['submission_count']}"
                        )
                        self.db.add_log(
                            'WARNING', 
                            f"Duplicate blocked: {customer_email} | {submission_type} | "
                            f"Previous: {prev['last_submission_at']} | Count: {prev['submission_count']}",
                            'deduplication'
                        )
                        
                        # Save to database but mark as duplicate
                        self.db.add_processed_email(
                            email['message_id'],
                            email['sender_email'],
                            email['subject'],
                            extracted_data,
                            submission_type,
                            is_duplicate=True
                        )
                        
                        # Update customer submission record (increases count)
                        self.db.record_customer_submission(customer_email, submission_type, email_subject, email['message_id'])
                        
                        # Archive the email WITHOUT sending webhook
                        self.archive_email(email['message_id'])
                        
                        logger.info(f"   → Archived without webhook")
                        continue
                    
                    # Not a duplicate - either new customer OR same customer with different type
                    logger.info(
                        f"✅ NEW SUBMISSION ALLOWED\n"
                        f"   Customer: {customer_email}\n"
                        f"   Type: {submission_type}\n"
                        f"   Subject: {email_subject}"
                    )
                    
                    # Save to database
                    self.db.add_processed_email(
                        email['message_id'],
                        email['sender_email'],
                        email['subject'],
                        extracted_data,
                        submission_type,
                        is_duplicate=False
                    )
                    
                    # Record customer submission
                    self.db.record_customer_submission(customer_email, submission_type, email_subject, email['message_id'])
                    
                    # Send to webhook
                    if self.send_to_webhook(extracted_data):
                        self.db.mark_webhook_sent(email['message_id'])
                        self.archive_email(email['message_id'])
                        
                        logger.info(f"   → Webhook sent and archived successfully")
                        self.db.add_log(
                            'INFO', 
                            f'Processed: {email["message_id"]} | {customer_email} | {submission_type}', 
                            'processing'
                        )
                    
                except Exception as e:
                    error_msg = f"Error processing email {email['message_id']}: {str(e)}"
                    logger.error(error_msg)
                    self.db.add_error('Email Processing', error_msg, str(e))
                    
        except Exception as e:
            error_msg = f"Error in process_emails: {str(e)}"
            logger.error(error_msg)
            self.db.add_error('Process Emails', error_msg, str(e))
    
    def send_to_webhook(self, data: Dict) -> bool:
        """Send data to webhook URL"""
        try:
            webhook_url = os.environ.get('WEBHOOK_URL')
            if not webhook_url:
                logger.error("WEBHOOK_URL not configured")
                return False
            
            response = requests.post(
                webhook_url,
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("Webhook sent successfully")
                self.db.add_log('INFO', f'Webhook sent successfully for {data.get("message_id")}', 'webhook')
                return True
            else:
                error_msg = f"Webhook failed with status {response.status_code}: {response.text}"
                logger.error(error_msg)
                self.db.add_error('Webhook Send', error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Error sending webhook: {str(e)}"
            logger.error(error_msg)
            self.db.add_error('Webhook Send', error_msg, str(e))
            return False

# Flask Web Dashboard
app = Flask(__name__)

@app.route('/')
def dashboard():
    """Main dashboard"""
    db = DatabaseManager()
    stats = db.get_processed_emails_stats()
    recent_logs = db.get_recent_logs(50)
    recent_errors = db.get_recent_errors(20)
    
    return render_template('dashboard.html', 
                         stats=stats, 
                         logs=recent_logs, 
                         errors=recent_errors,
                         dedup_window=DEDUPLICATION_WINDOW_HOURS)

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    db = DatabaseManager()
    return jsonify(db.get_processed_emails_stats())

@app.route('/api/logs')
def api_logs():
    """API endpoint for logs"""
    db = DatabaseManager()
    return jsonify(db.get_recent_logs(100))

@app.route('/api/errors')
def api_errors():
    """API endpoint for errors"""
    db = DatabaseManager()
    return jsonify(db.get_recent_errors(50))

def run_email_monitor():
    """Run the email monitoring loop"""
    monitor = GmailEmailMonitor()
    
    while True:
        try:
            logger.info("Starting email scan cycle...")
            monitor.process_emails()
            logger.info("Email scan cycle completed")
            
            time.sleep(20)
            
        except Exception as e:
            logger.error(f"Error in email monitoring loop: {str(e)}")
            time.sleep(20)

def main():
    """Main function to start the application"""
    try:
        print("🔧 Setting up database...")
        db = DatabaseManager()
        print("✅ Database initialized successfully!")
        
        print("🔑 Testing Gmail API connection...")
        monitor = GmailEmailMonitor()
        print("✅ Gmail API connection successful!")
        
        print(f"\n📋 DEDUPLICATION RULES:")
        print(f"   ⏱️  Window: {DEDUPLICATION_WINDOW_HOURS} hours")
        print(f"   🚫 Blocks: SAME customer email + SAME submission type")
        print(f"   ✅ Allows: SAME customer email + DIFFERENT submission type")
        print(f"\n   Example scenarios:")
        print(f"   • customer@example.com submits '[桧家住宅] イベント参加' → SENT")
        print(f"   • customer@example.com submits '[桧家住宅] イベント参加' again → BLOCKED")
        print(f"   • customer@example.com submits '[桧家住宅] 資料請求' → SENT (different type)")
        
        print("\n🚀 Starting email monitoring thread...")
        monitor_thread = threading.Thread(target=run_email_monitor)
        monitor_thread.daemon = True
        monitor_thread.start()
        print("✅ Email monitoring started!")
        
        print("\n🌐 Starting web dashboard...")
        port = int(os.environ.get('PORT', 5000))
        print(f"📊 Dashboard will be available at: http://localhost:{port}")
        print("🔄 Email scanning every 20 seconds...")
        print("🛡️  Smart deduplication active!")
        print("\nPress Ctrl+C to stop the application\n")
        
        app.run(host='0.0.0.0', port=port, debug=False)
        
    except KeyboardInterrupt:
        print("\n⏹️  Application stopped by user")
    except Exception as e:
        print(f"❌ Error in main function: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()