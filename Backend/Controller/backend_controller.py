import asyncio
import json
import websockets
import socket


class Partition:
    def __init__(self, topic, partition_number, agent_address):
        self.topic = topic
        self.partition_number = partition_number
        self.agent_address = agent_address  # (Leader) agent address


class BackendController:
    def __init__(self):
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

    async def send_data_to_agent(self, agent, topic, partition, client_address_str):
        # Build the WebSocket connection URL
        websocket_url = f"ws://{agent}"
        print("Sending data to agent...", websocket_url)

        try:
            # Establish a WebSocket connection with the server
            async with websockets.connect(websocket_url) as websocket:
                # Define client information and subscription details
                client_info = {'client_address': client_address_str}
                subscription_data = {
                    'client_info': client_info,
                    'topic': topic,
                    'partition_number': partition
                }

                # Send subscription request to the server
                await websocket.send(json.dumps(subscription_data))
                print(f"Sent subscription request to the server")

                # Wait for a response from the server
                response = await websocket.recv()
                print(f"Received response from the server: {response}")

        except Exception as e:
            print(f"Error connecting to the server: {str(e)}")

    async def handle_subscribe(self, websocket, json_data):
        # Get subscribed topics from requests
        subscribed_topics = json_data.get("subscriptions", [])
        print(subscribed_topics)
        client_address_str = json_data.get("listener-address")
        for topic in subscribed_topics:
            if topic in self.topics:
                for partition in self.topics[topic]:
                    agent = partition.agent_address
                    await self.send_data_to_agent(agent, topic, partition.partition_number, client_address_str)

        # Send a response back to the client
        response = {"message": "Subscription successful. Agents will now send information about subscribed topics."}
        response_json = json.dumps(response)
        await websocket.send(response_json)
    async def handle_get_topics(self, websocket):
        response = {"topics": self.topics.keys()}
        await websocket.send(response)
    async def handle_message(self, websocket):
        try:
            request_data = await websocket.recv()
            # Parse the JSON data
            json_data = json.loads(request_data)

            print(json_data)

            # Get action from websocket request
            action = json_data.get("action", "Invalid")

            if action == "Invalid":
                await websocket.send("Message must include valid action: getTopics, subscribe")

            if action == "getTopics":
                await self.handle_get_topics(websocket)
            if action == "subscribe":
                await self.handle_subscribe(websocket, json_data)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")


def main():
    backend_controller = BackendController()
    fake_agent = "127.0.0.1:8000"
    backend_controller.add_topic("Temperature", fake_agent)

    host = "127.0.0.1"
    port = 12345
    server = websockets.serve(backend_controller.handle_message, host, port)
    print(f"Backend controller server running on ws://{host}:{port}")

    asyncio.get_event_loop().run_until_complete(server)
    asyncio.get_event_loop().run_forever()

# Temporary for testing
if __name__ == '__main__':
    main()

