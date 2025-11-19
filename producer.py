import requests
import time
import os
import json
import logging
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models import TopicConfig

load_dotenv()

opensky_auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

token_info = {
    'access_token' : None,
    'expires_at' : 0
}

opensky_url = "https://opensky-network.org/api/states/all"
opensky_params = {
    "lamin": "45.8389",
    "lomin": "5.9962",
    "lamax": "47.8229",
    "lomax": "10.5226"
}
opensky_clientid = os.getenv("OPENSKY_CLIENTID")
opensky_clientsecret = os.getenv("OPENSKY_CLIENTSECRET")

state_vector_keys = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity", 
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", 
    "spi", "position_source", "category"
]

poll_interval = 22

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

def get_token():
    global token_info
    logging.info("Requesting new OpenSky access token")

    data = {
        'grant_type' : 'client_credentials',
        'client_id': opensky_clientid,
        'client_secret': opensky_clientsecret
    }

    try:
        response = requests.post(
            opensky_auth_url,
            data = data,
            headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
        )
        token_data = response.json()

        token_info['access_token'] = token_data.get('access_token')
        token_info['expires_at'] = time.time() + (token_data.get('expires_in', 1800) * 0.8)

        logging.info("Successfully obtained new token")
    except Exception as e:
        print(f"Exception {e}")
        raise SystemExit(1)

def get_events():
    global token_info

    if token_info['access_token'] is None or time.time() > token_info['expires_at']:
        get_token()

    headers = {
        'Authorization' : f"Bearer {token_info['access_token']}"
    }

    try:
        response = requests.get(opensky_url,
                                params = opensky_params,
                                headers = headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Exception {e}")
        return None
    
def main():
    logging.info("Starting OpenSky Producer")

    app = Application(
        broker_address = KAFKA_BROKER,
        producer_extra_config = {
            'broker.address.family' : 'v4',
            'linger.ms' : 5,
            'batch.size' : 10000,
        }
    )

    topic = app.topic(
        name = 'airspace-events',
        value_serializer = 'json',
        config = TopicConfig(
            num_partitions = 3,
            replication_factor = 3
        )
    )
    producer = app.get_producer()

    count = 0
    while True:
        events = get_events()
        if events and events.get('states'):
            state_vectors = events['states']
            logging.info(f"Publishing {len(state_vectors)} state vectors to Kafka")

            for state_vector in state_vectors:
                keyed_state_vector = dict(zip(state_vector_keys, state_vector))
                if count == 0:
                    print(keyed_state_vector)
                    count += 1
                producer.produce(
                    topic = 'airspace-events',
                    key = keyed_state_vector['icao24'].encode('utf-8'),
                    value = json.dumps(keyed_state_vector).encode('utf-8')
                )

            producer.flush()

        time.sleep(poll_interval)

if __name__ == '__main__':
    try:
        logging.basicConfig(level = 'DEBUG')
        main()
    except KeyboardInterrupt:
        pass