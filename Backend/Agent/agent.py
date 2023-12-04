import json
import asyncio
import websockets

class DataAgent:
    def __init__(self, host, port, backend_controller_url, is_leader=False):
        self.host = host
        self.port = port
        self.backend_controller_url =  backend_controller_url
        self.is_leader = is_leader
        self.data_store = {}
        self.sequence_numbers = {}
        self.subscriptions = {}  # Stores client subscriptions by topic and partition
        self.followers = {}  # Format: {partition_id: [follower WebSocket URLs]}
        self.heartbeat_interval = 5  # seconds
    async def start(self):
            """ Start the WebSocket server and connect to the BackendController. """
            # Start the WebSocket server for incoming connections
            # server = websockets.serve(self.listen_to_backend_controller, self.host, self.port)
            # asyncio.create_task(server)

            # Connect to the BackendController's WebSocket server

            
            await self.connect_to_backend_controller()


    async def connect_to_backend_controller(self):
        """ Connect to the BackendController's WebSocket server. """
        host = "127.0.0.1"
        port = self.port
        server = websockets.serve(self.listen_to_backend_controller, host, port)
        print(f"Backend controller server running on ws://{host}:{port}")

        asyncio.get_event_loop().run_until_complete(server)
        asyncio.get_event_loop().run_forever()
        

    async def listen_to_backend_controller(self, websocket):
        """ Listen for messages from the BackendController. """
        async for message in websocket:
            data = json.loads(message)
            print("RECEIVED DATA!!!", data)
            if 'client_info' in data:
                await self.handle_subscription(data)

    async def handle_subscription(self, data):
        """ Handle subscription request from BackendController. """
        client_address = data['client_info']['client_address']
        topic = data['topic']
        partition = data['partition_number']

        # Store subscription info
        if topic not in self.subscriptions:
            self.subscriptions[topic] = {}
        self.subscriptions[topic][partition] = client_address
        print("CLIENT_ADDR: ", client_address)
        await self.push_data_to_subscribers(client_address)
        print("REACHED!")

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

    async def push_data_to_subscribers(self, client_address):
        message = "garbage"
            #""" Push data to all subscribed clients for a given partition. """
            #topic = message['topic']
            #if topic in self.subscriptions and partition in self.subscriptions[topic]:
                #client_address = self.subscriptions[topic][partition]
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

# Example usage
backend_controller_url = 'ws://http://127.0.0.1:8000'
agent = DataAgent('localhost', 5000, backend_controller_url, is_leader=True)

# Start the agent and heartbeat in async context
def main():
    host = "127.0.0.1"
    port = 8000
    server = websockets.serve(agent.listen_to_backend_controller, host, port)
    print(f"Backend controller server running on ws://{host}:{port}")

    asyncio.get_event_loop().run_until_complete(server)
    asyncio.get_event_loop().run_forever()

# Temporary for testing
if __name__ == '__main__':
     main()