from kafka import KafkaProducer
import time

def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    return KafkaProducer(bootstrap_servers=['localhost:9092'])

def start_streaming_from_file(file_path):
    """
    Reads lines from a .txt file and streams them to Kafka every 5 seconds
    """
    producer = create_kafka_producer()

    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line:  # avoid empty lines
                    producer.send("devices", value=line.encode('utf-8'))
                    print(f"Sent: {line}")
                    time.sleep(5)  # wait 5 seconds before sending the next message
    except Exception as e:
        print("Error:", e)
    finally:
        producer.close()

if __name__ == "__main__":
    file_path = 'device_data.txt'  # make sure this file exists in the same folder
    start_streaming_from_file(file_path)
