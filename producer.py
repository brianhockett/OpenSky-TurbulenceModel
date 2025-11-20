# Imports
from quixstreams.models import TopicConfig
from quixstreams import Application
from dotenv import load_dotenv
import requests
import logging
import time
import json
import os

# os.getenv() will now refer to the .env file in the directory
load_dotenv()


# OpenSky URL for token authentication and OpenSky Credentials
opensky_auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
opensky_clientid = os.getenv("OPENSKY_CLIENTID")
opensky_clientsecret = os.getenv("OPENSKY_CLIENTSECRET")

# OpenSky API URL and Parameters for Sky-box to gather state vectors from
opensky_api_url = "https://opensky-network.org/api/states/all"
opensky_params = {
    "lamin": "37", # Southern Colorado Border
    "lomin": "-112", # Western Utah Border
    "lamax": "49", # US/Canada Border
    "lomax": "-110" # Central Wyoming
}

# Setting the current access token status at the start of the script
    # Each time the script is run, a fresh token will be generated, which lasts 24 minutes (This ensures a new token is generated prior to the 30 minute expiration time)
token_info = {
    'access_token' : None,
    'expires_at' : 0
}

# Key names for State Vector Response (This is not included in the API response)
state_vector_keys = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity", 
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", 
    "spi", "position_source"
]

# Number of seconds between polls (Ensures program never exceeds the limit of 4000 API calls per day)
poll_interval = 22

# Kafka address
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Function to create a new authentication token
def get_token():

    global token_info
    logging.info("Requesting new OpenSky access token")

    # Credentials
    credentials = {
        'grant_type' : 'client_credentials',
        'client_id': opensky_clientid,
        'client_secret': opensky_clientsecret
    }

    try:
        # Requesting access token
        response = requests.post(
            opensky_auth_url,
            data = credentials,
            headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
        )
        token_data = response.json()

        # Overwriting global token_info with new token
        token_info['access_token'] = token_data.get('access_token')
        token_info['expires_at'] = time.time() + (token_data.get('expires_in', 1800) * 0.8)
        logging.info("Successfully obtained new token")
    except Exception as e:
        # Exit the program if we are unable to receive an access token
        print(f"Exception {e}")
        raise SystemExit(1)

# Function to poll the API for events
def get_events():

    # If token_access is None or the previous token is going to expire soon, request a new token
    global token_info
    if token_info['access_token'] is None or time.time() > token_info['expires_at']:
        get_token()

    # Header for using Access Token in API request
    headers = {
        'Authorization' : f"Bearer {token_info['access_token']}"
    }

    try:
        # Poll API
        response = requests.get(opensky_api_url,
                                params = opensky_params,
                                headers = headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Exception {e}")
        return None
    
def main():

    logging.info("Starting OpenSky Producer")

    # Setting up Application
    app = Application(
        broker_address = KAFKA_BROKER,
        producer_extra_config = {
            'broker.address.family' : 'v4',
            'linger.ms' : 5,
            'batch.size' : 10000,
        }
    )

    # Creating Kafka topic for events to be stored in
    topic = app.topic(
        name = 'airspace-events',
        value_serializer = 'json',
        config = TopicConfig(
            num_partitions = 3,
            replication_factor = 3,
            extra_config = {
                "retention.ms": -1 
            }
        )
    )

    # Initializing producer object
    producer = app.get_producer()

    count = 0

    # Continuously polling the API every `poll_interval` seconds
    while True:

        # Poll API
        events = get_events()

        # Ensure response is valid
        if events and events.get('states'):

            # Extract state vectors from the events
            state_vectors = events['states']
            logging.info(f"Publishing {len(state_vectors)} state vectors to Kafka")

            # Loop through the state vectors
            for state_vector in state_vectors:
                # Add keys to state vector response
                keyed_state_vector = dict(zip(state_vector_keys, state_vector))
                if count == 0:
                    logging.info(keyed_state_vector)
                    count += 1


                # Must force key to be a string to prevent formatting issue
                    # Ex. icao ids with the form 543e9 results in scientific notation 54390000...
                key_string = f"ICAO_{keyed_state_vector['icao24']}"

                # Produce the state vector to the Kafka topic
                producer.produce(
                    topic = 'airspace-events',
                    key = key_string.encode('utf-8'),
                    value = json.dumps(keyed_state_vector).encode('utf-8')
                )

            # Force send all messages
            producer.flush()

        # Sleep to prevent exceeding rate limit
        time.sleep(poll_interval)

# Initialize logger and run the producer program
if __name__ == '__main__':
    try:
        logging.basicConfig(level = 'INFO')
        main()
    except KeyboardInterrupt:
        pass