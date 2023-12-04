import json
import asyncio
import websockets
import threading
import time

class DataAgent:
    def __init__(self, host, port, backend_controller_url, is_leader=False):
        self.host = host
        self.port = port
        self.backend_controller_url =  backend_controller_url
        self.is_leader = is_leader
        self.data_store = {}
        self.sequence_numbers = {}
        # Dictionary {Topic ---> Dictionary {Partition# ---> [ClientAddys] (list) }}
        self.subscriptions = {}  # Stores client subscriptions by topic and partition
        self.followers = {}  # Format: {partition_id: [follower WebSocket URLs]}
        self.heartbeat_interval = 5  # seconds

        # Dictionary: {Topic ---> Dictionary {Partition# ---> Queue (List)}}
        self.data = {}

        # Data lock. Ensures threads do not conflict when modifying self.data
        self.datalock = threading.Lock()

    async def start(self):
        while True:
            time.sleep(5)
            #continue
            for topic in self.subscriptions:
                for partition in self.subscriptions[topic]:
                    with self.datalock:
                        if topic in self.data and partition in self.data[topic]:
                            message = self.data[topic][partition].pop(0)
                            print("Sending message ", message)
                            for listener_client in self.subscriptions[topic][partition]:
                                await self.push_data_to_subscribers(listener_client, message)


    async def listen_to_backend_controller(self, websocket):
        """ Listen for messages from the BackendController. """
        async for message in websocket:
            data = json.loads(message)
            print("Received subscription request!")
            await self.handle_subscription(data)

    async def handle_subscription(self, data):
        """ Handle subscription request from BackendController. """
        client_address = data['client_info']['client_address']
        topic = data['topic']
        partition = data['partition_number']

        # Store subscription info
        if topic not in self.subscriptions:
            self.subscriptions[topic] = {}
        if partition not in self.subscriptions[topic]:
            self.subscriptions[topic][partition] = []
        self.subscriptions[topic][partition].append(client_address)
        print(f"Added {client_address} to subscriptions: subscriptions[{topic}][{partition}] = ", self.subscriptions[topic][partition])

    # def start_heartbeat(self):
    #     """ Start sending heartbeat messages if the agent is a leader. """
    #     if self.is_leader:
    #         while True:
    #             self.send_heartbeat_to_followers()
    #             time.sleep(self.heartbeat_interval)

    # async def respond_to_heartbeat(self, message):
    #     """ Respond to a heartbeat message from the leader. """
    #     ack_message = json.dumps({'type': 'heartbeat_ack', 'timestamp': message['timestamp']})
    #     # Send the ack_message to the leader (implementation depends on leader's WebSocket URL)
    #     pass  # Placeholder for sending the acknowledgment to the leader

    # async def process_message_as_leader(self, message, websocket):
    #     if message.get('type') == 'data':
    #         topic = message['topic']
    #         partition = message['partition']
    #         sequence_number = message['sequence_number']

    #         with self.lock:
    #             if partition not in self.data_store:
    #                 self.data_store[partition] = []
    #                 self.sequence_numbers[partition] = -1

    #             if sequence_number == self.sequence_numbers[partition] + 1:
    #                 self.data_store[partition].append(message)
    #                 self.sequence_numbers[partition] = sequence_number
    #                 await self.replicate_to_followers(partition, message)
    #             else:
    #                 print(f"Sequence number mismatch in partition {partition}.")
    #     elif message.get('type') == 'heartbeat_ack':
    #         # Process heartbeat acknowledgment
    #         pass  # Placeholder for handling heartbeat acknowledgments

    # async def replicate_to_followers(self, partition, message):
    #     """ Replicate data to follower agents using WebSockets. """
    #     followers = self.followers.get(partition, [])
    #     for url in followers:
    #         async with websockets.connect(url) as websocket:
    #             await websocket.send(json.dumps(message))
    async def listen_to_producer(self, websocket):
        """ Listen for messages from the BackendController. """
        async for message in websocket:
            data = json.loads(message)
            topic = data.get("Topic")
            partition = data.pop("Partition")
            with self.datalock:
                if topic not in self.data:
                    self.data[topic] = {}
                if partition not in self.data[topic]:
                    self.data[topic][partition] = list()
                self.data[topic][partition].append(data)
            print(self.data[topic][partition])

    async def push_data_to_subscribers(self, client_address, message):
        print("Reached!!! Attempting to send ", message, " to ", client_address)
        try:
            async with websockets.connect("ws://" + client_address) as websocket:
                await websocket.send(json.dumps(message))
        except Exception as e:
            print(f"Error in sending data to client {client_address}: {e}")


    # async def start_leader_election(self):
    #     """
    #     Initiates a leader election process using the bully algorithm.
    #     """
    #     self.is_leader = False
    #     await self.notify_others_for_election()
    #     await asyncio.sleep(random.uniform(0.5, 1.5))  # Random wait to avoid message collision
    #     if not self.received_higher_priority_message:
    #         self.is_leader = True
    #         await self.notify_others_leader_election_result()

    # async def notify_others_for_election(self):
    #     """
    #     Notify other agents that a leader election is taking place.
    #     """
    #     self.received_higher_priority_message = False
    #     for agent in self.other_agents:  # Adjusted for WebSocket communication
    #         try:
    #             async with websockets.connect(f"ws://{agent['websocket_address']}:{agent['websocket_port']}") as websocket:
    #                 await websocket.send(json.dumps({'type': 'election', 'id': self.id}))
    #         except Exception as e:
    #             print(f"Error in sending election message to agent {agent}: {e}")


    # async def notify_others_leader_election_result(self):
    #     """
    #     Notify other agents about the election result.
    #     """
    #     for agent in self.other_agents:
    #         try:
    #             async with websockets.connect(f"ws://{agent['address']}:{agent['port']}") as websocket:
    #                 await websocket.send(json.dumps({'type': 'new_leader', 'id': self.id}))
    #         except Exception as e:
    #             print(f"Error in sending new leader message to agent {agent}: {e}")

    # async def handle_election_messages(self, message):
    #     """
    #     Handle incoming messages related to leader election.
    #     """
    #     if message.get('type') == 'election' and message['id'] > self.id:
    #         self.received_higher_priority_message = True
    #     elif message.get('type') == 'new_leader':
    #         self.is_leader = False  # A new leader has been elected

# Start the agent and heartbeat in async context
def start_backend_controller_listener(agent):
    host = "127.0.0.1"
    port = 8000

    # Create a new event loop for the thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    server = websockets.serve(agent.listen_to_backend_controller, host, port)
    print(f"Backend controller listener server running on ws://{host}:{port}")

    loop.run_until_complete(server)
    loop.run_forever()

def start_producer_listener(agent):
    host = "127.0.0.1"
    port = 8001

    # Create a new event loop for the thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    server = websockets.serve(agent.listen_to_producer, host, port)
    print(f"Producer listener server running on ws://{host}:{port}")

    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == '__main__':
    # Example usage
    backend_controller_url = 'ws://http://127.0.0.1:8000'
    agent = DataAgent('localhost', 5000, backend_controller_url, is_leader=True)
    thread1 = threading.Thread(target=start_backend_controller_listener, args=(agent,))
    thread2 = threading.Thread(target=start_producer_listener, args=(agent,))
    thread3 = threading.Thread(target=asyncio.run, args=(agent.start(),))

    # Start the threads
    thread1.start()
    thread2.start()
    thread3.start()

    # Wait for both threads to finish (they won't)
    thread1.join()
    thread2.join()
    thread3.join()
