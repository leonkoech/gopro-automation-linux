import asyncio
import websockets
import struct
import hashlib
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
from datetime import datetime
import threading
import os
import json

LOCALSENSE_IP = "127.0.0.1"
PORT = 48300
USERNAME = "admin"
PASSWORD = "Uball_Tracking"
SALT = "abcdefghijklmnopqrstuvwxyz20191107salt"
LOGS_DIR = "tracker_logs"
SESSIONS_FILE = "sessions.json"

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Create logs directory if it doesn't exist
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Session Management ---
class SessionManager:
    def __init__(self):
        self.sessions = self.load_sessions()
        self.current_session = None
    
    def load_sessions(self):
        if os.path.exists(SESSIONS_FILE):
            with open(SESSIONS_FILE, 'r') as f:
                return json.load(f)
        return {}
    
    def save_sessions(self):
        with open(SESSIONS_FILE, 'w') as f:
            json.dump(self.sessions, f, indent=2)
    
    def start_session(self, session_id):
        self.current_session = {
            'session_id': session_id,
            'start_time': datetime.utcnow().isoformat(),
            'log_file': os.path.join(LOGS_DIR, f"{session_id}.log")
        }
        self.sessions[session_id] = self.current_session
        self.save_sessions()
        return self.current_session
    
    def stop_session(self):
        if self.current_session:
            self.current_session['end_time'] = datetime.utcnow().isoformat()
            self.sessions[self.current_session['session_id']] = self.current_session
            self.save_sessions()
            session = self.current_session
            self.current_session = None
            return session
        return None
    
    def get_session(self, session_id):
        return self.sessions.get(session_id)
    
    def get_current_log_file(self):
        if self.current_session:
            return self.current_session['log_file']
        return None

session_manager = SessionManager()

# --- CRC-16 MODBUS ---
def crc16_modbus(data: bytes):
    crc = 0xFFFF
    for pos in data:
        crc ^= pos
        for i in range(8):
            if crc & 1:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    return crc

# --- Build authentication packet ---
def build_auth_packet(username, password):
    md5_1 = hashlib.md5(password.encode()).hexdigest()
    md5_final = hashlib.md5((md5_1 + SALT).encode()).hexdigest()

    username_bytes = username.encode()
    password_bytes = md5_final.encode()

    frame = b""
    frame += struct.pack(">H", 0xCC5F)
    frame += struct.pack("B", 0x27)
    frame += struct.pack(">I", len(username_bytes))
    frame += username_bytes
    frame += struct.pack(">I", len(password_bytes))
    frame += password_bytes

    crc = crc16_modbus(frame[2:])
    frame += struct.pack(">H", crc)
    frame += struct.pack(">H", 0xAABB)
    return frame

# --- Listener stop flag ---
app.stop_listener_flag = False

# --- WebSocket listener ---
async def listen_localsense():
    ws_url = f"ws://{LOCALSENSE_IP}:{PORT}"
    try:
        async with websockets.connect(ws_url, subprotocols=["localSensePush-protocol"]) as ws:
            print("Connected to LocalSense!")
            auth_packet = build_auth_packet(USERNAME, PASSWORD)
            await ws.send(auth_packet)
            print("Auth packet sent.")

            while not app.stop_listener_flag:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if raw[2] == 0x81:  # position packet
                    num_tags = raw[3]
                    offset = 4
                    for _ in range(num_tags):
                        tag_id = struct.unpack(">I", raw[offset:offset+4])[0]
                        x = struct.unpack(">I", raw[offset+4:offset+8])[0]
                        y = struct.unpack(">I", raw[offset+8:offset+12])[0]
                        timestamp = struct.unpack(">I", raw[offset+16:offset+20])[0]

                        utc_now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        log_line = f"{utc_now} | Tag {tag_id} | X={x} | Y={y} | Timestamp={timestamp}\n"
                        print(log_line, end='')

                        # Write to current session log file
                        log_file = session_manager.get_current_log_file()
                        if log_file:
                            with open(log_file, "a") as f:
                                f.write(log_line)

                        offset += 23
    except Exception as e:
        print(f"WebSocket error: {e}")

# --- Start listener in background thread ---
def start_listener_thread():
    app.stop_listener_flag = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(listen_localsense())

# --- Flask routes ---
@app.route("/")
def home():
    return jsonify({
        "service": "LocalSense Tracker API",
        "version": "2.0",
        "status": "running"
    })

@app.route("/start", methods=['POST'])
def start():
    data = request.json or {}
    session_id = data.get('session_id', f"session_{int(datetime.utcnow().timestamp())}")
    
    if not hasattr(app, "listener_thread") or not app.listener_thread.is_alive():
        session_manager.start_session(session_id)
        app.listener_thread = threading.Thread(target=start_listener_thread, daemon=True)
        app.listener_thread.start()
        return jsonify({
            "status": "started",
            "session_id": session_id,
            "log_file": session_manager.get_current_log_file()
        })
    return jsonify({
        "status": "already running",
        "session_id": session_manager.current_session['session_id'] if session_manager.current_session else None
    }), 400

@app.route("/stop", methods=['POST'])
def stop():
    if hasattr(app, "listener_thread") and app.listener_thread.is_alive():
        app.stop_listener_flag = True
        session = session_manager.stop_session()
        
        if session:
            download_url = f"/session/{session['session_id']}/download"
            return jsonify({
                "status": "stopped",
                "session_id": session['session_id'],
                "download_url": download_url,
                "log_file": session['log_file']
            })
        
        return jsonify({"status": "stopped"})
    
    return jsonify({"status": "not running"}), 400

@app.route("/status")
def status():
    running = hasattr(app, "listener_thread") and app.listener_thread.is_alive()
    response = {"running": running}
    
    if running and session_manager.current_session:
        response["session_id"] = session_manager.current_session['session_id']
        response["start_time"] = session_manager.current_session['start_time']
    
    return jsonify(response)

@app.route("/sessions")
def list_sessions():
    sessions = [
        {
            "session_id": sid,
            "start_time": sdata.get('start_time'),
            "end_time": sdata.get('end_time'),
            "log_file": sdata.get('log_file')
        }
        for sid, sdata in session_manager.sessions.items()
    ]
    return jsonify({"sessions": sessions})

@app.route("/session/<session_id>/logs")
def get_session_logs(session_id):
    session = session_manager.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    
    log_file = session['log_file']
    if not os.path.exists(log_file):
        return jsonify({"error": "Log file not found"}), 404
    
    with open(log_file, 'r') as f:
        logs = f.readlines()
    
    return jsonify({"logs": logs, "count": len(logs)})

@app.route("/session/<session_id>/download")
def download_session(session_id):
    session = session_manager.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    
    log_file = session['log_file']
    if not os.path.exists(log_file):
        return jsonify({"error": "Log file not found"}), 404
    
    return send_file(log_file, as_attachment=True, download_name=f"{session_id}.log")

@app.route("/download")
def download_logs_by_time():
    """Legacy endpoint - download logs by time range"""
    start_utc = request.args.get("from")
    end_utc = request.args.get("to")
    
    if not start_utc or not end_utc:
        return jsonify({"error": "Please provide 'from' and 'to' query params"}), 400

    try:
        start_dt = datetime.strptime(start_utc, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_utc, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return jsonify({"error": "Invalid datetime format. Use: YYYY-MM-DD HH:MM:SS"}), 400

    # Search through all log files
    filtered_lines = []
    for session_id, session in session_manager.sessions.items():
        log_file = session['log_file']
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                for line in f:
                    try:
                        line_dt_str = line.split(" | ")[0]
                        line_dt = datetime.strptime(line_dt_str, "%Y-%m-%d %H:%M:%S.%f")
                        if start_dt <= line_dt <= end_dt:
                            filtered_lines.append(line)
                    except (ValueError, IndexError):
                        continue

    # Create temporary file
    safe_start = start_utc.replace(":", "-").replace(" ", "T")
    safe_end = end_utc.replace(":", "-").replace(" ", "T")
    filename = f"logs_{safe_start}_to_{safe_end}.txt"
    temp_file = os.path.join(LOGS_DIR, filename)

    with open(temp_file, "w") as f:
        f.writelines(filtered_lines)

    return send_file(temp_file, as_attachment=True)

# --- Run Flask ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)