import cv2
import json
import base64
import time
import Jetson.GPIO as GPIO

extra lineeee


moreee


from pymongo import MongoClient
import config
from google.cloud import storage

# GPIO SETUP
GPIO.setmode(GPIO.BOARD)
GPIO.setup(config.LEFT_SPRAY_PIN, GPIO.OUT, initial=GPIO.LOW)
GPIO.setup(config.RIGHT_SPRAY_PIN, GPIO.OUT, initial=GPIO.LOW)

# DATABASE CONNECTION
client = MongoClient(config.MONGO_URI)
db = client[config.MONGO_DB_NAME]

#Setup GCS client
storage_client = storage.Client.from_service_account_json(config.GCS_CREDENTIALS_JSON)
bucket = storage_client.bucket(config.GCS_BUCKET_NAME)

def upload_to_gcs(file_path, destination_blob_name):
    """Uploads the captured image to GCS and returns the public URL."""
    client = storage.Client.from_service_account_json(config.GCS_CREDENTIALS_JSON)
    bucket = client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return blob.public_url

def get_db_timestamp():
    return datetime.datetime.utcnow().isoformat() + "Z"

def capture_image():
    """Bypasses camera hardware and loads a local test image."""
    image_path = "test_image.jpg"
    
    if not os.path.exists(image_path):
        print(f"--- [ERROR] Local file {image_path} not found! ---")
        return None
    
    print(f"--- [DEBUG] Loading local image: {image_path} ---")
    frame = cv2.imread(image_path)
    
    if frame is None:
        print("--- [ERROR] Failed to decode image file. ---")
        return None

    # Encode to base64 for Gemini
    _, buffer = cv2.imencode('.jpg', frame)
    jpg_as_text = base64.b64encode(buffer).decode('utf-8')
    return jpg_as_text

def get_historical_context(user_id, wound_id):
    user = db[config.USER_COLLECTION].find_one({"user_id": user_id})
    allergies = user.get("allergies", []) if user else []
    last_wound = db[config.WOUND_COLLECTION].find_one(
        {"user_id": user_id, "wound_id": wound_id},
        sort=[("timestamp", -1)]
    )
    prev_diag = last_wound["ai_analysis"].get("diagnosis", "None") if last_wound else "New Wound"
    return allergies, prev_diag

def call_gemini_analysis(image_b64, allergies, prev_diag):
    # Injecting context into the pre-configured system prompt
    full_prompt = f"{config.SYSTEM_PROMPT}\nContext: Allergies: {allergies}, Previous State: {prev_diag}"
    
    payload = {
        "contents": [{
            "parts": [{"text": full_prompt}, {"inline_data": {"mime_type": "image/jpeg", "data": image_b64}}]
        }],
        "safetySettings": [
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"}
        ],
        "generationConfig": {"response_mime_type": "application/json"}
    }
    
    try:
        response = requests.post(config.GEMINI_URL, json=payload, timeout=25).json()
        raw_text = response['candidates'][0]['content']['parts'][0]['text']
        return json.loads(raw_text)
    except Exception as e:
        print(f"AI Error: {e}")
        return None

def execute_spray(decision):
    if not decision or not decision.get("spray"):
        print("--- [HARDWARE] No spray needed based on AI decision. ---")
        return False
    
    pin = config.LEFT_SPRAY_PIN if decision["direction"] == "LEFT" else config.RIGHT_SPRAY_PIN
    print(f"--- [HARDWARE] Triggering spray on Pin {pin} ({decision['direction']}) ---")
    
    GPIO.output(pin, GPIO.HIGH)
    time.sleep(0.5)
    GPIO.output(pin, GPIO.LOW)
    return True

def process_command(cmd_doc):
    # Atomic check/set to prevent double-processing
    res = db[config.CMD_COLLECTION].update_one(
        {"_id": cmd_doc["_id"], "status": "pending"},
        {"$set": {"status": "processing"}}
    )
    if res.modified_count == 0: return

    print(f"--- Processing Command for {cmd_doc['user_id']} ---")
    img_data = capture_image()
    
    if img_data:
        # --- START GCS UPLOAD LOGIC ---
        local_image_path = "test_image.jpg"
        # Generate a unique path in the bucket using user ID and timestamp
        gcs_blob_name = f"wounds/{cmd_doc['user_id']}_{int(time.time())}.jpg"
        
        try:
            image_url = upload_to_gcs(local_image_path, gcs_blob_name)
            print(f"--- [SUCCESS] Image uploaded to GCS: {image_url} ---")
        except Exception as e:
            print(f"--- [ERROR] GCS Upload failed: {e} ---")
            image_url = "Error uploading to cloud"
        # --- END GCS UPLOAD LOGIC ---

        allergies, prev = get_historical_context(cmd_doc['user_id'], cmd_doc['wound_id'])
        analysis = call_gemini_analysis(img_data, allergies, prev)
        
        if analysis:
            spray_done = execute_spray(analysis)
            
            # Save Session to Wounds - now including the link
            session_data = {
                "user_id": cmd_doc['user_id'],
                "wound_id": cmd_doc['wound_id'],
                "image_url": image_url, # Returns the GCS link to the database
                "timestamp": get_db_timestamp(),
                "ai_analysis": {**analysis, "spray_confirmed": spray_done}
            }
            db[config.WOUND_COLLECTION].insert_one(session_data)
            db[config.CMD_COLLECTION].update_one({"_id": cmd_doc["_id"]}, {"$set": {"status": "completed"}})
            print(f"Success: {analysis['diagnosis']} - {analysis['remarks']}")
        else:
            db[config.CMD_COLLECTION].update_one({"_id": cmd_doc["_id"]}, {"$set": {"status": "failed"}})
    else:
        db[config.CMD_COLLECTION].update_one({"_id": cmd_doc["_id"]}, {"$set": {"status": "image_error"}})

def main():
    print("--- [SYSTEM START] BayMini Wake Up (Local Image Mode) ---")
    
    try:
        db.command('ping')
        print("--- [SUCCESS] Connected to MongoDB Atlas ---")
    except Exception as e:
        print(f"--- [ERROR] Could not connect to MongoDB: {e} ---")
        return

    # 1. IMMEDIATE SWEEP
    missed_commands = list(db[config.CMD_COLLECTION].find({"status": "pending"}))
    if missed_commands:
        print(f"--- [SWEEP] Handling {len(missed_commands)} missed commands. ---")
        for doc in missed_commands:
            process_command(doc)
    
    # 2. WATCH FOR NEW COMMANDS
    print("--- [LISTENING] Waiting for new App Requests... ---")
    try:
        with db[config.CMD_COLLECTION].watch([{"$match": {"operationType": "insert"}}]) as stream:
            for change in stream:
                doc = change['fullDocument']
                if doc.get('status') == 'pending':
                    process_command(doc)
    except Exception as e:
        print(f"--- [WATCH ERROR] Falling back to Polling: {e} ---")
        while True:
            for doc in db[config.CMD_COLLECTION].find({"status": "pending"}):
                process_command(doc)
            time.sleep(5)

if __name__ == "__main__":
    try: 
        main()
    finally: 
        GPIO.cleanup()
