import os
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import credentials, auth, firestore
import socketio
import uvicorn
from datetime import datetime, timezone
import uuid
import random
import string
from typing import Dict, List, Optional
import json
import asyncio
from dataclasses import dataclass, asdict
from enum import Enum

# Initialize Firebase Admin
if os.path.exists("firebase-service-account.json"):
    cred = credentials.Certificate("firebase-service-account.json")
else:
    firebase_creds = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH") or os.getenv("FIREBASE_CREDENTIALS")
    if firebase_creds:
        cred = credentials.Certificate(json.loads(firebase_creds))
    else:
        raise Exception("Firebase credentials not found")

firebase_admin.initialize_app(cred)
db = firestore.client()

app = FastAPI(title="Nova Meet API")
security = HTTPBearer()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://localhost:3000", "https://novameetx22.web.app", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(
    cors_allowed_origins=["http://localhost:3000", "https://localhost:3000", "https://novameetx22.web.app", "*"],
    async_mode='asgi',
    ping_timeout=60,
    ping_interval=25,
    logger=True,
    engineio_logger=True,
    always_connect=True
)
socket_app = socketio.ASGIApp(sio, app)

class ParticipantRole(Enum):
    HOST = "host"
    PARTICIPANT = "participant"
    MODERATOR = "moderator"

@dataclass
class Participant:
    uid: str
    name: str
    email: str
    sid: str
    role: ParticipantRole
    joined_at: str
    is_audio_on: bool = True
    is_video_on: bool = True
    is_screen_sharing: bool = False
    is_hand_raised: bool = False
    connection_quality: str = "good"

@dataclass
class Meeting:
    id: str
    code: str
    host_uid: str
    host_name: str
    created_at: str
    status: str
    max_participants: int = 100
    is_recording: bool = False
    waiting_room_enabled: bool = False
    
socket_to_user: Dict[str, dict] = {}
active_speakers: Dict[str, str] = {}

def generate_meeting_code():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=3)) + '-' + \
           ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' + \
           ''.join(random.choices(string.ascii_lowercase + string.digits, k=1))

@app.get("/")
async def root():
    return {
        "service": "Nova Meet API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "meetings": "/api/meetings",
            "stats": "/api/stats"
        }
    }

async def verify_firebase_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )

@app.post("/api/meetings")
async def create_meeting(user: dict = Depends(verify_firebase_token)):
    meeting_code = generate_meeting_code()
    
    while db.collection('meetings').document(meeting_code).get().exists:
        meeting_code = generate_meeting_code()
    
    meeting_data = {
        "id": meeting_code,
        "code": meeting_code,
        "host_uid": user["uid"],
        "host_name": user.get("name", user.get("email", "Unknown")),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "active",
        "max_participants": 100,
        "is_recording": False,
        "waiting_room_enabled": False
    }
    
    db.collection('meetings').document(meeting_code).set(meeting_data)
    
    return {
        "id": meeting_code,
        "code": meeting_code,
        "join_url": f"https://novameetx22.web.app/join/{meeting_code}",
        "meeting": meeting_data
    }

@app.get("/api/meetings")
async def list_meetings(user: dict = Depends(verify_firebase_token)):
    meetings_ref = db.collection('meetings').where('status', '==', 'active').stream()
    meetings_list = []
    
    for meeting_doc in meetings_ref:
        meeting_data = meeting_doc.to_dict()
        participants_count = db.collection('meetings').document(meeting_doc.id).collection('participants').stream()
        meetings_list.append({
            "id": meeting_data['id'],
            "code": meeting_data['code'],
            "host_name": meeting_data['host_name'],
            "participant_count": len(list(participants_count))
        })
    
    return {"meetings": meetings_list}

@app.get("/api/meetings/{meeting_code}")
async def get_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    meeting_doc = db.collection('meetings').document(meeting_code).get()
    
    if not meeting_doc.exists:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting_data = meeting_doc.to_dict()
    participants_ref = db.collection('meetings').document(meeting_code).collection('participants').stream()
    participants = [p.to_dict() for p in participants_ref]
    
    return {
        "meeting": meeting_data,
        "participants": participants,
        "participant_count": len(participants)
    }

@app.post("/api/meetings/{meeting_code}/join")
async def join_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    meeting_doc = db.collection('meetings').document(meeting_code).get()
    
    if not meeting_doc.exists:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting_data = meeting_doc.to_dict()
    participants_ref = db.collection('meetings').document(meeting_code).collection('participants').stream()
    participant_count = len(list(participants_ref))
    
    return {
        "meeting": meeting_data,
        "can_join": True,
        "participant_count": participant_count
    }

@app.post("/api/meetings/{meeting_code}/end")
async def end_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    meeting_doc = db.collection('meetings').document(meeting_code).get()
    
    if not meeting_doc.exists:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting_data = meeting_doc.to_dict()
    if meeting_data['host_uid'] != user["uid"]:
        raise HTTPException(status_code=403, detail="Only host can end meeting")
    
    await sio.emit('meeting-ended', {'reason': 'Host ended the meeting'}, room=meeting_code)
    db.collection('meetings').document(meeting_code).update({'status': 'ended'})
    
    return {"message": "Meeting ended successfully"}

@sio.event
async def connect(sid, environ, auth):
    print(f"Connect attempt - SID: {sid}")
    try:
        token = auth.get("token") if auth else None
        meeting_code = auth.get("meetingId") if auth else None
        
        if not token or not meeting_code:
            print(f"Missing auth data: token={bool(token)}, meetingId={bool(meeting_code)}")
            return False
        
        try:
            from firebase_admin import auth as firebase_auth
            decoded_token = firebase_auth.verify_id_token(token)
        except Exception as e:
            print(f"Token verification failed: {e}")
            return False
            
        user_id = decoded_token["uid"]
        user_name = decoded_token.get("name", decoded_token.get("email", "Unknown"))
        
        meeting_doc = db.collection('meetings').document(meeting_code).get()
        if not meeting_doc.exists:
            print(f"Meeting not found: {meeting_code}")
            await sio.emit('error', {'message': 'Meeting not found'}, room=sid)
            return False
        
        meeting_data = meeting_doc.to_dict()
        role = ParticipantRole.HOST if meeting_data['host_uid'] == user_id else ParticipantRole.PARTICIPANT
        
        participant_data = {
            "uid": user_id,
            "name": user_name,
            "email": decoded_token.get("email", ""),
            "sid": sid,
            "role": role.value,
            "joined_at": datetime.now(timezone.utc).isoformat(),
            "is_audio_on": True,
            "is_video_on": True,
            "is_screen_sharing": False,
            "is_hand_raised": False,
            "connection_quality": "good"
        }
        
        db.collection('meetings').document(meeting_code).collection('participants').document(user_id).set(participant_data)
        
        socket_to_user[sid] = {
            "uid": user_id,
            "name": user_name,
            "email": decoded_token.get("email", ""),
            "meeting_code": meeting_code,
            "role": role.value
        }
        
        await sio.enter_room(sid, meeting_code)
        print(f"User {user_name} joined meeting {meeting_code}")
        
        # Get all existing participants
        participants_ref = db.collection('meetings').document(meeting_code).collection('participants').stream()
        all_participants = [p.to_dict() for p in participants_ref]
        
        print(f"Total participants in meeting: {len(all_participants)}")
        print(f"Participant list: {[p['name'] for p in all_participants]}")
        
        # Send complete participant list to new user
        await sio.emit('participants-update', all_participants, room=sid)
        
        # Notify all others about new participant (broadcast to room except sender)
        await sio.emit('user-joined', participant_data, room=meeting_code, skip_sid=sid)
        
        # Broadcast updated participants list to everyone in the room
        await sio.emit('participants-update', all_participants, room=meeting_code)
        
        return True
        
    except Exception as e:
        print(f"Connection error: {e}")
        import traceback
        traceback.print_exc()
        return False

@sio.event
async def disconnect(sid):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        print(f"User {user_info['name']} disconnected from meeting {meeting_code}")
        
        # Remove participant from Firestore
        db.collection('meetings').document(meeting_code).collection('participants').document(user_id).delete()
        
        # Notify others that user left
        await sio.emit('user-left', {
            "uid": user_id,
            "name": user_info["name"],
            "sid": sid
        }, room=meeting_code)
        
        # Get updated participant list and broadcast
        participants_ref = db.collection('meetings').document(meeting_code).collection('participants').stream()
        remaining_participants = [p.to_dict() for p in participants_ref]
        await sio.emit('participants-update', remaining_participants, room=meeting_code)
        
        print(f"Remaining participants: {len(remaining_participants)}")
        
        if meeting_code in active_speakers and active_speakers[meeting_code] == user_id:
            del active_speakers[meeting_code]
            await sio.emit('active-speaker-changed', {'speaker_uid': None}, room=meeting_code)
        
        del socket_to_user[sid]

@sio.event
async def offer(sid, data):
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('offer', {
            'offer': data['offer'],
            'from': sid,
            'type': data.get('type', 'video')
        }, room=target_sid)

@sio.event
async def answer(sid, data):
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('answer', {
            'answer': data['answer'],
            'from': sid,
            'type': data.get('type', 'video')
        }, room=target_sid)

@sio.event
async def ice_candidate(sid, data):
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('ice-candidate', {
            'candidate': data['candidate'],
            'from': sid
        }, room=target_sid)

@sio.event
async def toggle_audio(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        db.collection('meetings').document(meeting_code).collection('participants').document(user_id).update({
            'is_audio_on': data.get('isAudioOn', False)
        })
        
        await sio.emit('participant-audio-changed', {
            'uid': user_id,
            'isAudioOn': data.get('isAudioOn', False)
        }, room=meeting_code)

@sio.event
async def toggle_video(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        db.collection('meetings').document(meeting_code).collection('participants').document(user_id).update({
            'is_video_on': data.get('isVideoOn', False)
        })
        
        await sio.emit('participant-video-changed', {
            'uid': user_id,
            'isVideoOn': data.get('isVideoOn', False)
        }, room=meeting_code)

@sio.event
async def chat_message(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        
        message = {
            'id': str(uuid.uuid4()),
            'text': data['text'],
            'sender': user_info['name'],
            'sender_uid': user_info['uid'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'type': data.get('type', 'text')
        }
        
        await sio.emit('chat-message', message, room=meeting_code)

@sio.event
async def raise_hand(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        is_hand_raised = data.get('raised', False)
        db.collection('meetings').document(meeting_code).collection('participants').document(user_id).update({
            'is_hand_raised': is_hand_raised
        })
        
        await sio.emit('hand-raised-changed', {
            'uid': user_id,
            'name': user_info['name'],
            'isHandRaised': is_hand_raised
        }, room=meeting_code)

@sio.event
async def reaction(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        
        await sio.emit('reaction', {
            'uid': user_info['uid'],
            'name': user_info['name'],
            'emoji': data.get('emoji'),
            'timestamp': data.get('timestamp')
        }, room=meeting_code, skip_sid=sid)

@app.get("/health")
async def health_check():
    meetings_ref = db.collection('meetings').where('status', '==', 'active').stream()
    active_meetings = len(list(meetings_ref))
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "2.0.0",
        "active_meetings": active_meetings,
        "total_participants": len(socket_to_user)
    }

@app.get("/api/stats")
async def get_stats(user: dict = Depends(verify_firebase_token)):
    meetings_ref = db.collection('meetings').where('status', '==', 'active').stream()
    active_meetings = len(list(meetings_ref))
    
    return {
        "active_meetings": active_meetings,
        "total_participants": len(socket_to_user),
        "meetings_by_status": {
            "active": active_meetings,
        },
        "server_uptime": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=8000, log_level="info")
