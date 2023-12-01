import asyncio
import json
import websockets
import http.server
import socketserver

class Partition:
    def __init__(self, topic, partition_number, agent_address):
        self.topic = topic
        self.partition_number = partition_number
        self.agent_address = agent_address # (Leader) agent address

class SubscriptionHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        client_info = json.loads(post_data.decode('utf-8'))

        # Handle the subscription request
        result = BackendController.handle_subscription_request(client_info)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'message': result}).encode('utf-8'))

        # Optionally, you can print the client_info for debugging purposes
        print(f"Received client_info: {client_info}")
class BackendController:
    def __init__(self):
        self.HOST = "localhost"
        self.PORT = 8080

        # Stores list of topics and corresponding partitions
        # Topic (String) --> Partition
        self.topics = {}

        # Dictionary to store the mapping of clients to their subscribed topics
        # CliendID (String) --> Topics List(string)
        self.client_subscriptions = {}

    def add_topic(self, topic, agent):
        if topic not in self.topics:
            # If the topic doesn't exist, create it with a default partition
            initial_partition = Partition(topic, 1, agent)
            self.topics[topic] = [initial_partition]
            return True
        else:
            return False

    def remove_topic(self, topic):
        if topic in self.topics:
            del self.topics[topic]
            return True
        return False

    def change_leader_agent(self, topic, partition_number, new_agent_address):
        if topic in self.topics:
            for partition in self.topics[topic]:
                if partition.partition_number == partition_number:
                    partition.agent_address = new_agent_address
                    return True
        return False

    def add_partition(self, topic, agent_address):
        if topic in self.topics:
            partition = Partition(topic, len(self.topics) + 1, agent_address)
            self.topics[topic].append(partition)
            return True
        return False

    def remove_partition(self, topic, partition_number):
        if topic in self.topics:
            new_partitions = []
            for partition in self.topics[topic]:
                curr_partition_no = partition.partition_number
                if curr_partition_no is partition_number:
                    continue
                if curr_partition_no > partition_number:
                    partition.partition_number = curr_partition_no - 1
                new_partitions.append(partition)
            self.topics[topic] = new_partitions
            return True
        return False

    def run_server(self):
        start_server = websockets.serve(self.handle_subscribe, "localhost", 8765)

        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()


# Temporary for testing
if __name__ == '__main__':
    backend_controller = BackendController()
    fake_agent = "fake_address"
    backend_controller.add_topic("Temperature", fake_agent)
    backend_controller.add_topic("Power Usage", fake_agent)
    backend_controller.run_server()