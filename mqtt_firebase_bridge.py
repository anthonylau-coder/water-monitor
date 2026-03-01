#!/usr/bin/env python3
"""
MQTT to Firebase Bridge Script
Subscribes to MQTT topic, processes water level data, stores in Firebase, and sends responses
"""

import json
import logging
import ssl
import time
from datetime import datetime
from typing import Dict, Any, Optional

import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db
import schedule

# ==================== CONFIGURATION ====================

# MQTT Configuration
MQTT_CONFIG = {
    'broker': 'mqtt.smart-waterpumpsystem.com',
    'port': 8883,  # Using SSL port
    'username': 'test',
    'password': 'test1234',
    'subscribe_topic': '/waterlevel/866207074802772/status',
    'publish_topic': '/waterlevel/866207074802772/command',
    'client_id': f'PYTHON_BRIDGE_{int(time.time())}'
}

# Firebase Configuration
FIREBASE_CONFIG = {
    'apiKey': "AIzaSyBAzQFLLDACisyZN6CEIFunkA2by0LzocE",
    'authDomain': "water-monitor-system-cbccd.firebaseapp.com",
    'databaseURL': "https://water-monitor-system-cbccd-default-rtdb.asia-southeast1.firebasedatabase.app",
    'projectId': "water-monitor-system-cbccd",
    'storageBucket': "water-monitor-system-cbccd.firebasestorage.app",
    'messagingSenderId': "924884619328",
    'appId': "1:924884619328:web:0e187dd574cd54ee4e8fa5"
}

# Service account file (you need to download this from Firebase Console)
# Go to Project Settings > Service Accounts > Generate New Private Key
SERVICE_ACCOUNT_FILE = 'serviceAccountKey.json'

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_firebase_bridge.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== FIREBASE INITIALIZATION ====================

def initialize_firebase():
    """Initialize Firebase Admin SDK"""
    try:
        # Try to initialize with service account
        cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_CONFIG['databaseURL']
        })
        logger.info("Firebase initialized with service account")
    except Exception as e:
        logger.error(f"Failed to initialize Firebase with service account: {e}")
        logger.info("Attempting to initialize with default credentials...")
        
        # Alternative: Initialize with default credentials (for development)
        try:
            firebase_admin.initialize_app(options={
                'databaseURL': FIREBASE_CONFIG['databaseURL']
            })
            logger.info("Firebase initialized with default credentials")
        except Exception as e2:
            logger.error(f"Failed to initialize Firebase: {e2}")
            raise

# ==================== DATA PROCESSING FUNCTIONS ====================

def process_water_level_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and validate incoming water level data
    
    Args:
        data: Raw MQTT message data
        
    Returns:
        Processed data dictionary
    """
    try:
        # Extract and convert values
        water_level_raw = float(data.get('waterLevel', 0))
        water_level_mm = water_level_raw * 1000 if water_level_raw < 5 else water_level_raw
        
        processed = {
            'deviceCode': data.get('deviceCode', 'unknown'),
            'waterLevel': water_level_raw,
            'waterLevel_mm': round(water_level_mm, 2),
            'waterTemperature': float(data.get('waterTemperature', 0)),
            'collectionTime': data.get('collectionTime', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            'relayVoltage': float(data.get('relayVoltage', 0)),
            'sensorVoltage': float(data.get('sensorVoltage', 0)),
            'signalStrength': data.get('signalStrength', '0'),
            'alarmCode': data.get('alarmCode', '0'),
            'receivedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add calculated fields
        processed['batteryLow'] = processed['sensorVoltage'] < 3.4
        processed['alarmLevel'] = calculate_alarm_level(processed['waterLevel_mm'])
        
        return processed
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return data

def calculate_alarm_level(water_level_mm: float) -> int:
    """Calculate alarm level based on water level"""
    if water_level_mm >= 300:
        return 3
    elif water_level_mm >= 120:
        return 2
    elif water_level_mm >= 70:
        return 1
    else:
        return 0

def create_response_message(received_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create response message based on received data
    
    Args:
        received_data: The original received message data
        
    Returns:
        Response message dictionary
    """
    return {
        "msg": "Message Received",
        "collectionTime": received_data.get('collectionTime', 
                                           datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        "code": 200,
        "deviceCode": received_data.get('deviceCode', '2025122201')
    }

# ==================== FIREBASE STORAGE FUNCTIONS ====================

def save_to_firebase(data: Dict[str, Any]):
    """
    Save processed data to Firebase Realtime Database
    
    Args:
        data: Processed data dictionary
    """
    try:
        ref = db.reference('/')
        
        # Save to latest status
        latest_status = {
            'level': data['waterLevel_mm'],
            'temp': data['waterTemperature'],
            'sig': data['signalStrength'],
            'vol': data['sensorVoltage'],
            'time': data['collectionTime'],
            'alarmCode': data['alarmCode'],
            'deviceCode': data['deviceCode']
        }
        ref.child('latest_status').set(latest_status)
        logger.debug(f"Latest status updated: {latest_status}")
        
        # Save to history
        history_entry = {
            'level': data['waterLevel_mm'],
            'waterLevel': data['waterLevel'],
            'temperature': data['waterTemperature'],
            'time': data['collectionTime'],
            'signalStrength': data['signalStrength'],
            'alarmLevel': data['alarmLevel']
        }
        
        # Use timestamp as key for history
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        ref.child('history').child(timestamp).set(history_entry)
        
        # Also push to history list (for easy querying)
        ref.child('history_list').push(history_entry)
        
        # Save alarm if present
        if data['alarmLevel'] > 0 or data['batteryLow']:
            alarm_data = {
                'level': data['alarmLevel'],
                'batteryLow': data['batteryLow'],
                'waterLevel': data['waterLevel_mm'],
                'time': data['collectionTime'],
                'deviceCode': data['deviceCode']
            }
            ref.child('alarms').push(alarm_data)
        
        logger.info(f"Data saved to Firebase for device {data['deviceCode']}")
        
    except Exception as e:
        logger.error(f"Error saving to Firebase: {e}")

# ==================== MQTT CALLBACK FUNCTIONS ====================

def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server"""
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
        # Subscribe to topic
        client.subscribe(MQTT_CONFIG['subscribe_topic'])
        logger.info(f"Subscribed to topic: {MQTT_CONFIG['subscribe_topic']}")
    else:
        logger.error(f"Failed to connect to MQTT broker, return code {rc}")

def on_disconnect(client, userdata, rc):
    """Callback for when the client disconnects from the server"""
    logger.warning(f"Disconnected from MQTT broker with code {rc}")
    if rc != 0:
        logger.info("Attempting to reconnect...")

def on_message(client, userdata, msg):
    """
    Callback for when a PUBLISH message is received from the server
    
    This is the main processing function that:
    1. Parses incoming message
    2. Processes the data
    3. Saves to Firebase
    4. Sends response
    """
    try:
        logger.info(f"Message received on topic: {msg.topic}")
        
        # Parse JSON payload
        payload = json.loads(msg.payload.decode('utf-8'))
        logger.debug(f"Received payload: {payload}")
        
        # Process the data
        processed_data = process_water_level_data(payload)
        logger.info(f"Processed data for device: {processed_data['deviceCode']}")
        
        # Save to Firebase
        save_to_firebase(processed_data)
        
        # Create and send response
        response = create_response_message(processed_data)
        response_json = json.dumps(response)
        
        # Publish response
        result = client.publish(
            MQTT_CONFIG['publish_topic'], 
            response_json, 
            qos=1
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Response sent to {MQTT_CONFIG['publish_topic']}: {response}")
        else:
            logger.error(f"Failed to send response, error code: {result.rc}")
            
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON payload: {e}")
        logger.debug(f"Raw payload: {msg.payload}")
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)

def on_publish(client, userdata, mid):
    """Callback for when a message is published"""
    logger.debug(f"Message published with mid: {mid}")

def on_subscribe(client, userdata, mid, granted_qos):
    """Callback for when a subscription is completed"""
    logger.info(f"Subscribed to topic with QoS: {granted_qos}")

# ==================== MQTT CLIENT SETUP ====================

def setup_mqtt_client() -> mqtt.Client:
    """
    Setup and configure MQTT client
    
    Returns:
        Configured MQTT client instance
    """
    # Create MQTT client instance
    client = mqtt.Client(
        client_id=MQTT_CONFIG['client_id'],
        clean_session=True,
        protocol=mqtt.MQTTv311
    )
    
    # Set username and password
    client.username_pw_set(MQTT_CONFIG['username'], MQTT_CONFIG['password'])
    
    # Set SSL/TLS for secure connection
    client.tls_set(
        cert_reqs=ssl.CERT_NONE,  # For testing; use CERT_REQUIRED in production
        tls_version=ssl.PROTOCOL_TLS
    )
    
    # Assign callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    
    return client

# ==================== HEALTH CHECK FUNCTIONS ====================

def health_check():
    """Periodic health check function"""
    logger.info("Health check - Bridge is running")
    
    # Check Firebase connection
    try:
        ref = db.reference('/')
        ref.get(shallow=True)
        logger.debug("Firebase connection OK")
    except Exception as e:
        logger.error(f"Firebase connection error: {e}")

def reconnect_mqtt():
    """Attempt to reconnect to MQTT broker if disconnected"""
    if not mqtt_client.is_connected():
        logger.info("Attempting to reconnect to MQTT broker...")
        try:
            mqtt_client.reconnect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")

# ==================== MAIN FUNCTION ====================

def main():
    """Main function to run the MQTT to Firebase bridge"""
    global mqtt_client
    
    logger.info("Starting MQTT to Firebase Bridge...")
    logger.info(f"MQTT Broker: {MQTT_CONFIG['broker']}:{MQTT_CONFIG['port']}")
    logger.info(f"Subscribe Topic: {MQTT_CONFIG['subscribe_topic']}")
    logger.info(f"Publish Topic: {MQTT_CONFIG['publish_topic']}")
    
    # Initialize Firebase
    initialize_firebase()
    
    # Setup MQTT client
    mqtt_client = setup_mqtt_client()
    
    # Schedule health checks
    schedule.every(5).minutes.do(health_check)
    schedule.every(1).minutes.do(reconnect_mqtt)
    
    # Connect to MQTT broker
    try:
        logger.info(f"Connecting to MQTT broker at {MQTT_CONFIG['broker']}:{MQTT_CONFIG['port']}...")
        mqtt_client.connect(
            MQTT_CONFIG['broker'], 
            MQTT_CONFIG['port'], 
            keepalive=60
        )
        
        # Start the MQTT loop in a non-blocking thread
        mqtt_client.loop_start()
        logger.info("MQTT client loop started")
        
        # Keep the main thread running
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down bridge...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("Bridge stopped")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

if __name__ == "__main__":
    # Global variable for MQTT client
    mqtt_client = None
    main()