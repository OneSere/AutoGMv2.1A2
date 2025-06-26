import os
import time
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
import pyrebase
from pytz import timezone
import random
import json

# --- Firebase Config ---
firebase_config = {
  "apiKey": "AIzaSyBt5ML2Ob9c2BqZRo2N2GN5bI7WBjg-Jzk",
  "authDomain": "autogmv2aa2.firebaseapp.com",
  "databaseURL": "https://autogmv2aa2-default-rtdb.firebaseio.com/",
  "projectId": "autogmv2aa2",
  "storageBucket": "autogmv2aa2.firebasestorage.app",
  "messagingSenderId": "734385232100",
  "appId": "1:734385232100:web:c9dd04e084a80bfee074f1"
}
firebase = pyrebase.initialize_app(firebase_config)
db = firebase.database()

API_ID = 25843334
API_HASH = "e752bb9ebc151b7e36741d7ead8e4fd0"
PHONE = "+919772303434"  # The phone number to login
FIREBASE_PROMOS_PATH = "promos"
FIREBASE_INTERVAL_PATH = "interval"
FIREBASE_STATUS_PATH = "live_status"
FIREBASE_OTP_PATH = "otp"
FIREBASE_SESSION_PATH = "session"
FIREBASE_USER_REPLIES_PATH = "user_replies"  # Track user replies
FIREBASE_GROUPS_PATH = "groups"  # Groups list and selection

# --- Helper Functions ---
def save_status(msg, level="INFO"):
    """Enhanced status logging with levels and better formatting"""
    now = datetime.utcnow().isoformat()
    ist_time = get_current_ist().strftime("%Y-%m-%d %H:%M:%S IST")
    
    # Format message with timestamp and level
    formatted_msg = f"[{level}] {msg} | {ist_time}"
    
    db.child(FIREBASE_STATUS_PATH).push({
        "msg": formatted_msg, 
        "ts": now,
        "level": level,
        "ist_time": ist_time
    })
    
    # Delete old status messages (older than 1 hour)
    all_status = db.child(FIREBASE_STATUS_PATH).get().val() or {}
    cutoff = datetime.utcnow() - timedelta(hours=1)
    for key, val in all_status.items():
        try:
            ts = datetime.fromisoformat(val["ts"])
            if ts < cutoff:
                db.child(FIREBASE_STATUS_PATH).child(key).remove()
        except Exception:
            db.child(FIREBASE_STATUS_PATH).child(key).remove()

def save_groups_to_firebase(groups_data):
    """Save groups data to Firebase"""
    try:
        db.child(FIREBASE_GROUPS_PATH).set(groups_data)
        save_status(f"Saved {len(groups_data)} groups to Firebase", "SUCCESS")
        return True
    except Exception as e:
        save_status(f"Error saving groups to Firebase: {e}", "ERROR")
        return False

def load_groups_from_firebase():
    """Load groups data from Firebase"""
    try:
        groups_data = db.child(FIREBASE_GROUPS_PATH).get().val()
        if groups_data:
            save_status(f"Loaded {len(groups_data)} groups from Firebase", "INFO")
            return groups_data
        else:
            save_status("No saved groups found in Firebase", "WARNING")
            return []
    except Exception as e:
        save_status(f"Error loading groups from Firebase: {e}", "ERROR")
        return []

async def get_groups_from_folder(client, folder_name="123456"):
    """Get groups from a specific Telegram folder"""
    try:
        save_status(f"Fetching groups from Telegram folder: {folder_name}", "INFO")
        groups_data = []
        
        # Get all dialogs first
        all_dialogs = []
        async for dialog in client.iter_dialogs():
            all_dialogs.append(dialog)
        
        # Try to find the folder by name
        folder_entity = None
        for dialog in all_dialogs:
            if hasattr(dialog.entity, 'title') and dialog.entity.title == folder_name:
                folder_entity = dialog.entity
                break
        
        if folder_entity:
            save_status(f"Found folder: {folder_name}", "SUCCESS")
            
            # Get dialogs from this folder
            try:
                # For folders, we need to get the participants/members
                if hasattr(folder_entity, 'participants_count'):
                    # This is a folder, get its contents
                    async for dialog in client.iter_dialogs():
                        # Check if this dialog belongs to our folder
                        # We'll check by looking at the folder structure
                        if (dialog.is_group or dialog.is_channel) and not dialog.is_user:
                            # For now, let's get all groups and channels
                            # You can refine this logic based on your folder structure
                            group_info = {
                                "id": dialog.id,
                                "title": dialog.title,
                                "username": dialog.entity.username if hasattr(dialog.entity, 'username') else None,
                                "type": "group" if dialog.is_group else "channel",
                                "participants_count": getattr(dialog.entity, 'participants_count', 0)
                            }
                            groups_data.append(group_info)
                            save_status(f"Found group in folder: {dialog.title} (ID: {dialog.id})", "INFO")
                
                # Alternative approach: get all groups and assume they're in the folder
                # This is more reliable since folder access can be tricky
                if not groups_data:
                    save_status("Using alternative method: fetching all groups", "INFO")
                    async for dialog in client.iter_dialogs():
                        if (dialog.is_group or dialog.is_channel) and not dialog.is_user:
                            group_info = {
                                "id": dialog.id,
                                "title": dialog.title,
                                "username": dialog.entity.username if hasattr(dialog.entity, 'username') else None,
                                "type": "group" if dialog.is_group else "channel",
                                "participants_count": getattr(dialog.entity, 'participants_count', 0)
                            }
                            groups_data.append(group_info)
                            save_status(f"Found group: {dialog.title} (ID: {dialog.id})", "INFO")
                
            except Exception as e:
                save_status(f"Error accessing folder contents: {e}", "ERROR")
                # Fallback: get all groups
                async for dialog in client.iter_dialogs():
                    if (dialog.is_group or dialog.is_channel) and not dialog.is_user:
                        group_info = {
                            "id": dialog.id,
                            "title": dialog.title,
                            "username": dialog.entity.username if hasattr(dialog.entity, 'username') else None,
                            "type": "group" if dialog.is_group else "channel",
                            "participants_count": getattr(dialog.entity, 'participants_count', 0)
                        }
                        groups_data.append(group_info)
                        save_status(f"Found group (fallback): {dialog.title} (ID: {dialog.id})", "INFO")
        else:
            save_status(f"Folder '{folder_name}' not found, fetching all groups", "WARNING")
            # Fallback: get all groups if folder not found
            async for dialog in client.iter_dialogs():
                if (dialog.is_group or dialog.is_channel) and not dialog.is_user:
                    group_info = {
                        "id": dialog.id,
                        "title": dialog.title,
                        "username": dialog.entity.username if hasattr(dialog.entity, 'username') else None,
                        "type": "group" if dialog.is_group else "channel",
                        "participants_count": getattr(dialog.entity, 'participants_count', 0)
                    }
                    groups_data.append(group_info)
                    save_status(f"Found group: {dialog.title} (ID: {dialog.id})", "INFO")
        
        if groups_data:
            save_status(f"Successfully found {len(groups_data)} groups", "SUCCESS")
            return groups_data
        else:
            save_status("No groups found", "WARNING")
            return []
            
    except Exception as e:
        save_status(f"Error fetching groups from folder: {e}", "ERROR")
        return []

async def fetch_and_save_groups_list(client):
    """Fetch all groups from Telegram and save numbered list to Firebase"""
    try:
        save_status("Fetching all groups from Telegram...", "INFO")
        groups_data = []
        group_number = 1
        
        async for dialog in client.iter_dialogs():
            # Only include groups and channels, never personal users
            if (dialog.is_group or dialog.is_channel) and not dialog.is_user:
                group_info = {
                    "number": group_number,
                    "id": dialog.id,
                    "title": dialog.title,
                    "username": dialog.entity.username if hasattr(dialog.entity, 'username') else None,
                    "type": "group" if dialog.is_group else "channel",
                    "participants_count": getattr(dialog.entity, 'participants_count', 0)
                }
                groups_data.append(group_info)
                save_status(f"Found group {group_number}: {dialog.title} (ID: {dialog.id})", "INFO")
                group_number += 1
        
        if groups_data:
            # Create the groups list in Firebase
            groups_dict = {}
            for group in groups_data:
                groups_dict[str(group["number"])] = f"{group['number']}. {group['title']}"
            
            # Store the full group data in a separate, simpler format
            full_data_simple = []
            for group in groups_data:
                full_data_simple.append({
                    "num": group["number"],
                    "id": group["id"],
                    "title": group["title"],
                    "type": group["type"]
                })
            groups_dict["fulldata"] = full_data_simple
            
            # Add the selection instruction at the end
            groups_dict["group"] = "enter numbers to select (e.g., 1,2,4,6)"
            
            # Save to Firebase
            db.child(FIREBASE_GROUPS_PATH).set(groups_dict)
            save_status(f"Successfully saved {len(groups_data)} groups to Firebase", "SUCCESS")
            save_status("Please select groups by entering numbers in Firebase group field", "INFO")
            return groups_data
        else:
            save_status("No groups found in Telegram", "WARNING")
            return []
            
    except Exception as e:
        save_status(f"Error fetching groups: {e}", "ERROR")
        return []

def get_selected_groups():
    """Get the selected groups from Firebase"""
    try:
        groups_data = db.child(FIREBASE_GROUPS_PATH).get().val()
        if not groups_data:
            return []
        
        # Get the selection from group field
        selection = groups_data.get("group", "")
        if not selection or selection == "enter numbers to select (e.g., 1,2,4,6)":
            return []
        
        # Parse the selection (e.g., "1,2,4,6")
        try:
            selected_numbers = [int(x.strip()) for x in selection.split(",") if x.strip().isdigit()]
        except:
            save_status("Invalid group selection format. Use numbers separated by commas (e.g., 1,2,4,6)", "ERROR")
            return []
        
        # Get the full group data
        full_data = groups_data.get("fulldata", [])
        if not full_data:
            save_status("Full group data not found, please refresh groups", "ERROR")
            return []
        
        # Get the selected groups with their actual IDs
        selected_groups = []
        for num in selected_numbers:
            for group in full_data:
                if group["num"] == num:
                    selected_groups.append({
                        "number": group["num"],
                        "id": group["id"],
                        "title": group["title"],
                        "type": group["type"]
                    })
                    break
        
        if selected_groups:
            save_status(f"Selected {len(selected_groups)} groups: {[g['number'] for g in selected_groups]}", "SUCCESS")
        else:
            save_status("No groups selected or invalid selection", "WARNING")
        
        return selected_groups
        
    except Exception as e:
        save_status(f"Error getting selected groups: {e}", "ERROR")
        return []

async def get_groups_list(client):
    """Get groups list - fetch from Telegram and allow manual selection"""
    # First check if groups are already saved
    existing_groups = db.child(FIREBASE_GROUPS_PATH).get().val()
    
    if not existing_groups:
        # No groups saved, fetch them
        save_status("No groups found in Firebase, fetching from Telegram...", "INFO")
        return await fetch_and_save_groups_list(client)
    else:
        # Groups are saved, check if selection is made
        selected_groups = get_selected_groups()
        if selected_groups:
            save_status(f"Using {len(selected_groups)} selected groups", "INFO")
            return selected_groups
        else:
            save_status("Groups found but none selected. Please select groups in Firebase", "WARNING")
            return []

def can_reply_to_user(user_id):
    """Check if we can reply to this user (once per 24 hours)"""
    try:
        user_replies = db.child(FIREBASE_USER_REPLIES_PATH).get().val() or {}
        user_id_str = str(user_id)
        
        if user_id_str not in user_replies:
            return True
        
        last_reply_time = user_replies[user_id_str]
        last_reply_dt = datetime.fromisoformat(last_reply_time)
        now = datetime.utcnow()
        
        # Check if 24 hours have passed
        if now - last_reply_dt >= timedelta(hours=24):
            return True
        
        return False
    except Exception as e:
        save_status(f"Error checking user reply status: {e}", "ERROR")
        return False

def mark_user_replied(user_id):
    """Mark that we've replied to this user"""
    try:
        user_id_str = str(user_id)
        now = datetime.utcnow().isoformat()
        db.child(FIREBASE_USER_REPLIES_PATH).child(user_id_str).set(now)
        
        # Clean up old user entries (older than 48 hours)
        user_replies = db.child(FIREBASE_USER_REPLIES_PATH).get().val() or {}
        cutoff = datetime.utcnow() - timedelta(hours=48)
        for uid, reply_time in user_replies.items():
            try:
                reply_dt = datetime.fromisoformat(reply_time)
                if reply_dt < cutoff:
                    db.child(FIREBASE_USER_REPLIES_PATH).child(uid).remove()
            except Exception:
                db.child(FIREBASE_USER_REPLIES_PATH).child(uid).remove()
    except Exception as e:
        save_status(f"Error marking user replied: {e}", "ERROR")

def get_promos():
    promos = db.child(FIREBASE_PROMOS_PATH).get().val()
    # Only use non-empty, non-whitespace promos
    if promos and isinstance(promos, list):
        return [p for p in promos if p and str(p).strip()]
    elif promos and isinstance(promos, dict):
        return [v for k, v in sorted(promos.items()) if v and str(v).strip()]
    return []

def get_interval():
    val = db.child(FIREBASE_INTERVAL_PATH).get().val()
    try:
        return int(val)
    except Exception:
        return 10  # default 10 minutes

def save_session(session_str):
    db.child(FIREBASE_SESSION_PATH).set(session_str)

def load_session():
    return db.child(FIREBASE_SESSION_PATH).get().val()

def get_otp_from_firebase():
    return db.child(FIREBASE_OTP_PATH).get().val()

def clear_otp_in_firebase():
    db.child(FIREBASE_OTP_PATH).remove()

# --- Humanize Delays ---
def get_current_ist():
    return datetime.now(timezone('Asia/Kolkata'))

def get_next_active_delay():
    now = get_current_ist()
    hour = now.hour
    minute = now.minute
    t = hour * 60 + minute
    # Adjust for after midnight
    if t < 600:
        t += 1440
    slots = [
        (600, 870, 'active'),         # 10:00 AM – 2:30 PM
        (870, 890, 'tea'),           # 2:30 PM – 2:50 PM
        (890, 990, 'active'),        # 2:50 PM – 4:30 PM
        (990, 1050, 'lunch'),        # 4:30 PM – 5:30 PM
        (1050, 1200, 'active'),      # 5:30 PM – 8:00 PM
        (1200, 1220, 'tea'),         # 8:00 PM – 8:20 PM
        (1220, 1560, 'active'),      # 8:20 PM – 2:00 AM (next day)
        (1560, 1680, 'lunch'),       # 2:00 AM – 4:00 AM (next day)
        (1680, 1980, 'active'),      # 4:00 AM – 9:00 AM (next day)
        (1980, 2160, 'sleep'),       # 9:00 AM – 12:00 PM (next day)
    ]
    for start, end, status in slots:
        if start <= t < end:
            if status == 'active':
                return 0, 'active'
            else:
                mins_to_wait = end - t
                return mins_to_wait * 60, status
    # Default: sleep until 10:00 AM
    mins_to_wait = (600 + 1440) - t
    return mins_to_wait * 60, 'sleep'

# --- Telegram Login ---
async def telegram_login():
    session_str = load_session()
    if session_str:
        client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
        try:
            await client.connect()
            if await client.is_user_authorized():
                save_status("Auto-login successful using saved session", "SUCCESS")
                return client
            else:
                save_status("Session expired, clearing and doing fresh login", "WARNING")
                # Clear the invalid session
                try:
                    db.child(FIREBASE_SESSION_PATH).remove()
                    save_status("Cleared expired session from Firebase", "INFO")
                except:
                    pass
                await client.disconnect()
        except Exception as e:
            save_status(f"Session login failed: {e}", "WARNING")
            # Clear the invalid session
            try:
                db.child(FIREBASE_SESSION_PATH).remove()
                save_status("Cleared invalid session from Firebase", "INFO")
            except:
                pass
            await client.disconnect()
    
    # No session or session invalid, do fresh login
    save_status("Starting fresh login process", "INFO")
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
        await client.send_code_request(PHONE)
        # Explicitly create the /otp key in Firebase for you to paste the OTP
        db.child(FIREBASE_OTP_PATH).set("PASTE OTP HERE")
        save_status(f"OTP sent to {PHONE}. Waiting for OTP in Firebase...", "INFO")
        
        # Wait for OTP to appear in Firebase
        for _ in range(20):  # Wait up to 20*3=60 seconds
            otp = get_otp_from_firebase()
            if otp and otp != "PASTE OTP HERE":
                try:
                    await client.sign_in(PHONE, otp)
                    session_str = client.session.save()
                    save_session(session_str)
                    save_status("Login successful, session saved", "SUCCESS")
                    clear_otp_in_firebase()
                    return client
                except Exception as e:
                    save_status(f"OTP error: {e}", "ERROR")
                    clear_otp_in_firebase()
                    break
            await asyncio.sleep(3)
        
        save_status("OTP not found or invalid after 60 seconds", "ERROR")
        await client.disconnect()
        return None
    except Exception as e:
        save_status(f"Login error: {e}", "ERROR")
        await client.disconnect()
        return None

# --- Firebase Initialization ---
def ensure_firebase_defaults():
    # Promos
    promos = db.child(FIREBASE_PROMOS_PATH).get().val()
    default_promos = [
        "🔥 1. Automate Telegram Like a Pro!\n🚀 Say goodbye to manual posting!\nThe 24/7 Telegram Auto Message Sender sends your messages on loop across groups while you're chilling, sleeping, or working.\n\n✨ Bonus: Smart delays, anti-ban features, and loop settings included!\n\n💰 Only ₹159 – lifetime tool\n📩 DM: @curiositymind\n✅ Escrow Secured | 💬 Nego On | 🛡 Warranty Included",
        "💸 2. Still Working! Zepto Refund Trick 2025\n💯 Get your money back using our 100% tested Zepto refund method.\nPerfect for ₹50–₹100 items, with real steps and guidance — no nonsense.\n\n🎁 Bonus: One-time support for your first refund attempt!\n\n💸 Just ₹99\n📩 DM @curiositymind\n🔐 Escrow Ready | 💬 Price Talk Open | ✔ Comes with Warranty",
        "🤖 3. Get Your Own Telegram Bot — Coded For You!\nHave an idea for a Telegram bot?\nWe'll code it from scratch — be it for replying, forwarding, filtering, adding members, or anything else.\n\n💡 Bonus: 1 FREE update after delivery!\n\n💵 ₹300 Flat\n📩 Contact @curiositymind\n✅ Escrow | 💬 Flexible Price | 🛠 Guaranteed Support",
        "👥 4. Extract Real Telegram Members in Seconds\n📥 Use our Telegram group scraping tool to collect members from ANY group — build your list fast and smart.\n\n📦 Bonus: Includes CSV export + filters for bots/deleted users!\n\n💰 Only ₹49\n📩 Message @curiositymind\n✅ Escrow | 💸 Negotiable | 📘 Simple Guide Included",
        "🌐 5. Run Your Telegram Bot Without VPS!\nHost your Telegram bot online 24x7 without paying for a server.\nOur Telegram Bot Hosting Method runs your bot on lightweight cloud for just ₹30/month.\n\n🎁 Bonus: Setup video, restart code & log monitoring included!\n\n💵 ₹30/Month\n📩 DM @curiositymind\n✅ Escrow | 🛠 With Setup Help | 💬 Price Chat Open",
        "🕒 6. Send Messages Like a Human – Non-Stop!\nUse our smart Telegram auto-messaging tool to send daily or loop messages while staying under Telegram's radar.\n\n⚡ Bonus: Anti-ban delay engine + support for multiple sessions!\n\n💸 ₹159\n📩 Ping @curiositymind\n✅ Escrow | 💬 Negotiation On | ✔ Warranty & Manual",
        "🤖 7. Any Telegram Bot You Imagine – We Build It!\n🧠 Whether you want an admin bot, welcome bot, forwarder, scheduler, or data fetcher — we can create it!\n\n🎁 Bonus: Full code access + FREE update within 7 days\n\n💰 ₹300\n📩 Talk to @curiositymind\n✅ Escrow Secured | 💬 Flexible Rates | 🛡 Warranty Always",
        "☁️ 8. Host Telegram Bots 24/7 Without Headaches\nAvoid expensive VPS or Linux errors.\nUse our Telegram bot hosting method to keep your bot live — cheap, fast, reliable.\n\n📦 Bonus: Auto restart, downtime recovery & monitoring tricks\n\n💵 ₹30/month\n📩 DM @curiositymind\n🔐 Escrow | 💬 Nego Friendly | 🛠 Setup Assistance Provided",
        "📈 9. Grow Your Telegram with Targeted Real Users\nTired of fake users?\nWith our group scraping tool, pull real people from niche groups to fuel your channel, store, or community!\n\n🎁 Bonus: Clean export + optional filtering\n\n💸 ₹49\n📩 DM @curiositymind\n✅ Escrow Secured | 💬 Negotiable | 📘 Instant Delivery"
    ]
    if not promos:
        db.child(FIREBASE_PROMOS_PATH).set(default_promos)
    
    # Interval
    interval = db.child(FIREBASE_INTERVAL_PATH).get().val()
    if not interval or str(interval).strip() == "":
        db.child(FIREBASE_INTERVAL_PATH).set(10)
    
    # Live status
    live_status = db.child(FIREBASE_STATUS_PATH).get().val()
    if not live_status:
        now = datetime.utcnow().isoformat()
        db.child(FIREBASE_STATUS_PATH).push({"msg": "[INIT] Bot started. Waiting for login.", "ts": now})
    
    # Start/Stop system
    startstop = db.child("startstopsystem").get().val()
    if startstop is None:
        db.child("startstopsystem").set("")
    
    # OTP
    otp = db.child(FIREBASE_OTP_PATH).get().val()
    if otp is None:
        db.child(FIREBASE_OTP_PATH).set("")
    
    # Session
    session = db.child(FIREBASE_SESSION_PATH).get().val()
    if session is None:
        db.child(FIREBASE_SESSION_PATH).set("")
    
    # User replies tracking
    user_replies = db.child(FIREBASE_USER_REPLIES_PATH).get().val()
    if user_replies is None:
        db.child(FIREBASE_USER_REPLIES_PATH).set({})
    
    # Groups
    groups = db.child(FIREBASE_GROUPS_PATH).get().val()
    if groups is None:
        db.child(FIREBASE_GROUPS_PATH).set({})

ADMIN_NOTE = ("📢 Note from Admin \n"
              "Hey dosto! This is just an advertising/demo account.\n"
              "Ye account sirf promotion ke liye use ho raha hai.\n\n"
              "👉 For any real tasks, queries, or services, kindly contact: @curiositymind on telegram \n\n"
              "📋 This account was officially purchased on 25th June / यह अकाउंट 25 जून को खरीदा गया था।")

async def handle_incoming_messages(client):
    @client.on(events.NewMessage(incoming=True, outgoing=False))
    async def handler(event):
        try:
            # Only reply to private users, not groups or channels
            if event.is_private:
                user_id = event.sender_id
                if can_reply_to_user(user_id):
                    await asyncio.sleep(5)
                    await event.reply(ADMIN_NOTE)
                    mark_user_replied(user_id)
                    save_status(f"Sent admin note to user {user_id} (first time in 24h)", "INFO")
                else:
                    save_status(f"Skipped reply to user {user_id} (already replied in last 24h)", "INFO")
        except Exception as e:
            save_status(f"Auto-reply error: {e}", "ERROR")

def should_stop():
    val = db.child('startstopsystem').get().val()
    return val and str(val).strip().upper() == 'STOP'

async def wait_until_start():
    while True:
        startstop = db.child("startstopsystem").get().val()
        if not startstop or str(startstop).strip().upper() != "STOP":
            save_status("STOP command cleared. Resuming message sending", "SUCCESS")
            break
        save_status("STOP command active. Waiting for resume...", "PAUSED")
        await asyncio.sleep(10)

async def health_check(client):
    """Perform a health check on the client connection"""
    try:
        if not client.is_connected():
            return False
        
        # Try to get account info to test authorization
        me = await client.get_me()
        if me:
            save_status(f"Health check passed - Connected as {me.first_name}", "INFO")
            return True
        else:
            save_status("Health check failed - Could not get account info", "ERROR")
            return False
    except Exception as e:
        save_status(f"Health check failed: {e}", "ERROR")
        return False

async def ensure_client_connected(client):
    """Ensure client is connected and handle reconnection"""
    try:
        if not client.is_connected():
            save_status("Client disconnected, attempting reconnection...", "WARNING")
            await client.connect()
            if await client.is_user_authorized():
                # Perform health check
                if await health_check(client):
                    save_status("Reconnection successful", "SUCCESS")
                    return True
                else:
                    save_status("Reconnection failed - health check failed", "ERROR")
                    return False
            else:
                save_status("Reconnection failed - session expired, will force fresh login", "ERROR")
                # Clear the invalid session
                try:
                    db.child(FIREBASE_SESSION_PATH).remove()
                    save_status("Cleared invalid session from Firebase", "INFO")
                except:
                    pass
                return False
        else:
            # Client is connected, perform health check
            if await health_check(client):
                return True
            else:
                save_status("Connected but health check failed", "ERROR")
                return False
    except Exception as e:
        save_status(f"Connection check failed: {e}", "ERROR")
        return False

# --- Main Message Sending Loop ---
async def main_loop():
    last_sent_promo = {}  # group_id -> last promo index sent
    consecutive_auth_failures = 0  # Track consecutive auth failures
    consecutive_connection_failures = 0  # Track connection failures
    group_idx = 0  # Track which group to message next
    promo_idx = 0  # Track which promo to send next
    
    while True:
        try:
            client = await telegram_login()
            if not client:
                consecutive_auth_failures += 1
                if consecutive_auth_failures >= 3:
                    save_status("Multiple login failures. Clearing session and waiting 10 minutes", "ERROR")
                    try:
                        db.child(FIREBASE_SESSION_PATH).remove()
                    except:
                        pass
                    consecutive_auth_failures = 0
                    consecutive_connection_failures = 0
                    await asyncio.sleep(600)  # Wait 10 minutes
                else:
                    save_status(f"Could not login. Retrying in 5 minutes (attempt {consecutive_auth_failures})", "ERROR")
                    await asyncio.sleep(300)  # Wait 5 minutes
                continue
            
            # Reset failure counters on successful login
            consecutive_auth_failures = 0
            consecutive_connection_failures = 0
            
            # Test connection immediately after login
            if not await ensure_client_connected(client):
                save_status("Initial connection test failed, restarting login process", "ERROR")
                consecutive_connection_failures += 1
                if consecutive_connection_failures >= 2:
                    save_status("Multiple connection failures. Waiting 10 minutes before retry", "ERROR")
                    await asyncio.sleep(600)
                    consecutive_connection_failures = 0
                continue
            
            # Start message handler
            await handle_incoming_messages(client)
            
            # Get groups list (from Telegram folder)
            groups_data = await get_groups_list(client)
            
            if not groups_data:
                save_status("No groups available. Sleeping 10 min", "WARNING")
                await client.disconnect()
                await asyncio.sleep(600)
                continue
            
            promos = get_promos()
            if not promos:
                save_status("No promos found in Firebase. Sleeping 10 min", "WARNING")
                await client.disconnect()
                await asyncio.sleep(600)
                continue
            
            group_list = groups_data
            promo_list = promos
            group_count = len(group_list)
            promo_count = len(promo_list)
            
            save_status(f"Starting message loop with {group_count} groups and {promo_count} promos", "INFO")
            
            while True:
                # Always check start/stop system before sending
                startstop = db.child("startstopsystem").get().val()
                if startstop and str(startstop).strip().upper() == "STOP":
                    jitter = random.randint(5, 30)
                    save_status(f"STOP command active. Waiting {jitter}s before checking again", "PAUSED")
                    await asyncio.sleep(jitter)
                    await wait_until_start()
                    jitter = random.randint(5, 30)
                    save_status(f"STOP cleared. Waiting {jitter}s before resuming", "SUCCESS")
                    await asyncio.sleep(jitter)
                
                # Check if in break or active slot
                delay, status = get_next_active_delay()
                if status != 'active':
                    save_status(f"{status.title()} Break: resting {delay//60} min", "PAUSED")
                    await asyncio.sleep(delay)
                    jitter = random.randint(1, 5)
                    save_status(f"Post-break random delay: {jitter}s", "INFO")
                    await asyncio.sleep(jitter)
                    
                    # After break, check start/stop again
                    startstop = db.child("startstopsystem").get().val()
                    if startstop and str(startstop).strip().upper() == "STOP":
                        jitter = random.randint(5, 30)
                        save_status(f"STOP command active after break. Waiting {jitter}s", "PAUSED")
                        await asyncio.sleep(jitter)
                        await wait_until_start()
                        jitter = random.randint(5, 30)
                        save_status(f"STOP cleared after break. Waiting {jitter}s", "SUCCESS")
                        await asyncio.sleep(jitter)
                        continue
                
                # Ensure client is connected before sending
                if not await ensure_client_connected(client):
                    save_status("Client connection failed, restarting main loop", "ERROR")
                    consecutive_connection_failures += 1
                    if consecutive_connection_failures >= 3:
                        save_status("Multiple connection failures. Waiting 15 minutes before retry", "ERROR")
                        await asyncio.sleep(900)  # Wait 15 minutes
                        consecutive_connection_failures = 0
                    break
                
                interval = get_interval()
                # Send to one group per interval, round-robin
                group_info = group_list[group_idx % group_count]
                promo = promo_list[promo_idx % promo_count]
                gid = str(group_info["id"])
                try:
                    jitter = random.randint(5, 15)
                    save_status(f"Waiting {jitter}s before sending to {group_info['title']} ({group_info['id']})", "INFO")
                    await asyncio.sleep(jitter)
                    
                    # Send message using group ID
                    await client.send_message(int(group_info["id"]), promo)
                    save_status(f"✅ Sent promo {promo_idx+1} to {group_info['title']} ({group_info['id']})", "SUCCESS")
                    last_sent_promo[gid] = promo_idx
                    
                    jitter2 = random.randint(5, 15)
                    save_status(f"Waiting {jitter2}s after sending to {group_info['title']}", "INFO")
                    await asyncio.sleep(jitter2)
                except Exception as e:
                    save_status(f"❌ Error sending to {group_info['title']}: {e}", "ERROR")
                    # If it's an authorization error, break the loop to force re-login
                    if "authorized" in str(e).lower() or "session" in str(e).lower():
                        save_status("Authorization error detected, forcing fresh login", "ERROR")
                        try:
                            db.child(FIREBASE_SESSION_PATH).remove()
                            save_status("Cleared invalid session", "INFO")
                        except:
                            pass
                        break
                    # If it's a disconnection error, try to reconnect
                    elif "disconnected" in str(e).lower():
                        save_status("Detected disconnection, attempting reconnection", "WARNING")
                        if not await ensure_client_connected(client):
                            save_status("Reconnection failed, restarting main loop", "ERROR")
                            consecutive_connection_failures += 1
                    break
                # Move to next group and promo for next interval
                group_idx = (group_idx + 1) % group_count
                promo_idx = (promo_idx + 1) % promo_count
                # Wait interval before next message
                real_interval = max(1, interval * 60)
                save_status(f"⏰ Waiting {real_interval//60}m {real_interval%60}s before next message", "INFO")
                await asyncio.sleep(real_interval)
        except Exception as e:
            save_status(f"Main loop error: {e}", "ERROR")
            # If it's an authorization error, clear session
            if "authorized" in str(e).lower() or "session" in str(e).lower():
                try:
                    db.child(FIREBASE_SESSION_PATH).remove()
                    save_status("Cleared invalid session due to error", "INFO")
                except:
                    pass
            consecutive_connection_failures += 1
            if consecutive_connection_failures >= 5:
                save_status("Too many consecutive failures. Waiting 20 minutes before retry", "ERROR")
                await asyncio.sleep(1200)  # Wait 20 minutes
                consecutive_connection_failures = 0
            else:
                await asyncio.sleep(120)  # Wait 2 minutes
        finally:
            try:
                await client.disconnect()
            except:
                pass
            await asyncio.sleep(30)  # Wait 30 seconds before next attempt

if __name__ == "__main__":
    ensure_firebase_defaults()
    asyncio.run(main_loop())
