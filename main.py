from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import credentials, auth
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
cred = credentials.Certificate("firebase-service-account.json")
firebase_admin.initialize_app(cred)

app = FastAPI(title="Nova Meet API")
security = HTTPBearer()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://localhost:3000", "https://novameetx22.web.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Socket.IO server
sio = socketio.AsyncServer(
    cors_allowed_origins=["http://localhost:3000", "https://localhost:3000", "https://novameetx22.web.app"],
    async_mode='asgi',
    ping_timeout=60,
    ping_interval=25
)
socket_app = socketio.ASGIApp(sio, app)

# Enhanced data structures for SFU architecture
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
    
# In-memory storage (Phase 1 - will be replaced with database)
meetings: Dict[str, Meeting] = {}
meeting_participants: Dict[str, Dict[str, Participant]] = {}
socket_to_user: Dict[str, dict] = {}
active_speakers: Dict[str, str] = {}  # meeting_id -> current_speaker_uid

def generate_meeting_code():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=3)) + '-' + \
           ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' + \
           ''.join(random.choices(string.ascii_lowercase + string.digits, k=1))

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
    while meeting_code in meetings:
        meeting_code = generate_meeting_code()
    
    meeting = Meeting(
        id=meeting_code,
        code=meeting_code,
        host_uid=user["uid"],
        host_name=user.get("name", user.get("email", "Unknown")),
        created_at=datetime.now(timezone.utc).isoformat(),
        status="active"
    )
    
    meetings[meeting_code] = meeting
    meeting_participants[meeting_code] = {}
    
    return {
        "id": meeting_code,
        "code": meeting_code,
        "join_url": f"http://localhost:3000/join/{meeting_code}",
        "meeting": asdict(meeting)
    }

@app.get("/api/meetings")
async def list_meetings(user: dict = Depends(verify_firebase_token)):
    """List all active meetings for debugging"""
    return {
        "meetings": [{
            "id": meeting.id,
            "code": meeting.code,
            "host_name": meeting.host_name,
            "participant_count": len(meeting_participants.get(meeting.id, {}))
        } for meeting in meetings.values()]
    }

@app.get("/api/meetings/{meeting_code}")
async def get_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    if meeting_code not in meetings:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting = meetings[meeting_code]
    participants = meeting_participants.get(meeting_code, {})
    
    return {
        "meeting": asdict(meeting),
        "participants": [{
            "uid": p.uid,
            "name": p.name,
            "email": p.email,
            "sid": p.sid,
            "role": p.role.value,
            "joined_at": p.joined_at,
            "is_audio_on": p.is_audio_on,
            "is_video_on": p.is_video_on,
            "is_screen_sharing": p.is_screen_sharing,
            "is_hand_raised": p.is_hand_raised,
            "connection_quality": p.connection_quality
        } for p in participants.values()],
        "participant_count": len(participants)
    }

@app.post("/api/meetings/{meeting_code}/join")
async def join_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    if meeting_code not in meetings:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting = meetings[meeting_code]
    participants = meeting_participants.get(meeting_code, {})
    
    return {
        "meeting": asdict(meeting),
        "can_join": True,
        "participant_count": len(participants)
    }

@app.post("/api/meetings/{meeting_code}/end")
async def end_meeting(meeting_code: str, user: dict = Depends(verify_firebase_token)):
    if meeting_code not in meetings:
        raise HTTPException(status_code=404, detail="Meeting not found")
    
    meeting = meetings[meeting_code]
    if meeting.host_uid != user["uid"]:
        raise HTTPException(status_code=403, detail="Only host can end meeting")
    
    # Notify all participants
    await sio.emit('meeting-ended', {'reason': 'Host ended the meeting'}, room=meeting_code)
    
    # Cleanup
    del meetings[meeting_code]
    if meeting_code in meeting_participants:
        del meeting_participants[meeting_code]
    
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
            decoded_token = auth.verify_id_token(token)
        except Exception as e:
            print(f"Token verification failed: {e}")
            await sio.disconnect(sid)
            return False
            
        user_id = decoded_token["uid"]
        user_name = decoded_token.get("name", decoded_token.get("email", "Unknown"))
        
        # Check if meeting exists
        if meeting_code not in meetings:
            print(f"Meeting not found: {meeting_code}. Available meetings: {list(meetings.keys())}")
            await sio.emit('error', {'message': 'Meeting not found'}, room=sid)
            await sio.disconnect(sid)
            return False
        
        meeting = meetings[meeting_code]
        role = ParticipantRole.HOST if meeting.host_uid == user_id else ParticipantRole.PARTICIPANT
        
        participant = Participant(
            uid=user_id,
            name=user_name,
            email=decoded_token.get("email", ""),
            sid=sid,
            role=role,
            joined_at=datetime.now(timezone.utc).isoformat()
        )
        
        socket_to_user[sid] = {
            "uid": user_id,
            "name": user_name,
            "email": decoded_token.get("email", ""),
            "meeting_code": meeting_code,
            "role": role.value
        }
        
        await sio.enter_room(sid, meeting_code)
        
        if meeting_code not in meeting_participants:
            meeting_participants[meeting_code] = {}
        
        meeting_participants[meeting_code][user_id] = participant
        
        # Send existing participants to new user
        existing_participants = [{
            "uid": p.uid,
            "name": p.name,
            "email": p.email,
            "sid": p.sid,
            "role": p.role.value,
            "joined_at": p.joined_at,
            "is_audio_on": p.is_audio_on,
            "is_video_on": p.is_video_on,
            "is_screen_sharing": p.is_screen_sharing,
            "is_hand_raised": p.is_hand_raised
        } for p in meeting_participants[meeting_code].values() if p.uid != user_id]
        
        # Send existing participants to new user
        for existing_p in existing_participants:
            await sio.emit('user-joined', existing_p, room=sid)
        
        # Notify others about new participant
        await sio.emit('user-joined', {
            "uid": participant.uid,
            "name": participant.name,
            "email": participant.email,
            "sid": participant.sid,
            "role": participant.role.value,
            "joined_at": participant.joined_at,
            "is_audio_on": participant.is_audio_on,
            "is_video_on": participant.is_video_on,
            "is_screen_sharing": participant.is_screen_sharing,
            "is_hand_raised": participant.is_hand_raised
        }, room=meeting_code, skip_sid=sid)
        
        # Send meeting info
        await sio.emit('meeting-info', {
            'meeting': asdict(meeting),
            'your_role': role.value,
            'participant_count': len(meeting_participants[meeting_code])
        }, room=sid)
        
    except Exception as e:
        print(f"Connection error: {e}")
        await sio.emit('error', {'message': 'Authentication failed'}, room=sid)
        await sio.disconnect(sid)

@sio.event
async def disconnect(sid):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            del meeting_participants[meeting_code][user_id]
            
            await sio.emit('user-left', {
                "uid": user_id,
                "name": participant.name,
                "sid": sid
            }, room=meeting_code)
            
            # Update active speaker if this was the active speaker
            if meeting_code in active_speakers and active_speakers[meeting_code] == user_id:
                del active_speakers[meeting_code]
                await sio.emit('active-speaker-changed', {'speaker_uid': None}, room=meeting_code)
        
        del socket_to_user[sid]

# SFU-Ready WebRTC Signaling Events
@sio.event
async def join_room(sid, data):
    """Join media room - SFU integration point"""
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        
        # This is where SFU media server integration happens
        # For now, emit to all participants for P2P fallback
        await sio.emit('participant-ready-to-connect', {
            'uid': user_info['uid'],
            'sid': sid
        }, room=meeting_code, skip_sid=sid)

@sio.event
async def offer(sid, data):
    """WebRTC offer - will route through SFU in Phase 2"""
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('offer', {
            'offer': data['offer'],
            'from': sid,
            'type': data.get('type', 'video')  # video, audio, screen
        }, room=target_sid)

@sio.event
async def answer(sid, data):
    """WebRTC answer - will route through SFU in Phase 2"""
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('answer', {
            'answer': data['answer'],
            'from': sid,
            'type': data.get('type', 'video')
        }, room=target_sid)

@sio.event
async def ice_candidate(sid, data):
    """ICE candidate - will route through SFU in Phase 2"""
    target_sid = data.get('to')
    if target_sid:
        await sio.emit('ice-candidate', {
            'candidate': data['candidate'],
            'from': sid
        }, room=target_sid)

# Media Control Events
@sio.event
async def toggle_audio(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.is_audio_on = data.get('isAudioOn', False)
            
            await sio.emit('participant-audio-changed', {
                'uid': user_id,
                'isAudioOn': participant.is_audio_on
            }, room=meeting_code)

@sio.event
async def toggle_video(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.is_video_on = data.get('isVideoOn', False)
            
            await sio.emit('participant-video-changed', {
                'uid': user_id,
                'isVideoOn': participant.is_video_on
            }, room=meeting_code)

@sio.event
async def start_screen_share(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.is_screen_sharing = True
            
            await sio.emit('screen-share-started', {
                'uid': user_id,
                'name': participant.name
            }, room=meeting_code)

@sio.event
async def stop_screen_share(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.is_screen_sharing = False
            
            await sio.emit('screen-share-stopped', {
                'uid': user_id
            }, room=meeting_code)

@sio.event
async def active_speaker(sid, data):
    """Track active speaker for SFU optimization"""
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        # Update active speaker
        active_speakers[meeting_code] = user_id
        
        await sio.emit('active-speaker-changed', {
            'speaker_uid': user_id,
            'speaker_name': user_info['name']
        }, room=meeting_code, skip_sid=sid)

# Enhanced Chat System
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
            'type': data.get('type', 'text')  # text, emoji, file
        }
        
        await sio.emit('chat-message', message, room=meeting_code)

# Host Control Events
@sio.event
async def mute_participant(sid, data):
    """Host can mute participants"""
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        target_uid = data.get('target_uid')
        
        # Check if user is host
        if meeting_code in meetings and meetings[meeting_code].host_uid == user_info['uid']:
            # Find target participant's socket
            target_participant = None
            for uid, participant in meeting_participants[meeting_code].items():
                if uid == target_uid:
                    target_participant = participant
                    break
            
            if target_participant:
                await sio.emit('force-mute', {
                    'reason': 'Muted by host'
                }, room=target_participant.sid)
                
                target_participant.is_audio_on = False
                
                await sio.emit('participant-audio-changed', {
                    'uid': target_uid,
                    'isAudioOn': False,
                    'muted_by_host': True
                }, room=meeting_code)

@sio.event
async def remove_participant(sid, data):
    """Host can remove participants"""
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        target_uid = data.get('target_uid')
        
        # Check if user is host
        if meeting_code in meetings and meetings[meeting_code].host_uid == user_info['uid']:
            target_participant = None
            for uid, participant in meeting_participants[meeting_code].items():
                if uid == target_uid:
                    target_participant = participant
                    break
            
            if target_participant:
                await sio.emit('removed-from-meeting', {
                    'reason': 'Removed by host'
                }, room=target_participant.sid)
                
                await sio.disconnect(target_participant.sid)

@sio.event
async def raise_hand(sid, data):
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.is_hand_raised = data.get('isHandRaised', False)
            
            await sio.emit('hand-raised-changed', {
                'uid': user_id,
                'name': participant.name,
                'isHandRaised': participant.is_hand_raised
            }, room=meeting_code)

# Connection Quality Monitoring
@sio.event
async def connection_quality(sid, data):
    """Monitor connection quality for SFU optimization"""
    if sid in socket_to_user:
        user_info = socket_to_user[sid]
        meeting_code = user_info["meeting_code"]
        user_id = user_info["uid"]
        
        if meeting_code in meeting_participants and user_id in meeting_participants[meeting_code]:
            participant = meeting_participants[meeting_code][user_id]
            participant.connection_quality = data.get('quality', 'good')  # poor, fair, good, excellent
            
            # Only notify host about poor connections
            if participant.connection_quality == 'poor':
                host_uid = meetings[meeting_code].host_uid
                host_participant = meeting_participants[meeting_code].get(host_uid)
                if host_participant:
                    await sio.emit('participant-connection-poor', {
                        'uid': user_id,
                        'name': participant.name,
                        'quality': participant.connection_quality
                    }, room=host_participant.sid)

# Health Check and Monitoring Endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint for load balancers"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "2.0.0",
        "active_meetings": len(meetings),
        "total_participants": sum(len(participants) for participants in meeting_participants.values())
    }

@app.get("/api/stats")
async def get_stats(user: dict = Depends(verify_firebase_token)):
    """Get server statistics"""
    return {
        "active_meetings": len(meetings),
        "total_participants": sum(len(participants) for participants in meeting_participants.values()),
        "meetings_by_status": {
            "active": len([m for m in meetings.values() if m.status == "active"]),
        },
        "server_uptime": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=8000, log_level="info")