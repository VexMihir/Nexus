import socket
import threading
import json
import requests
import time
import asyncio
import websockets
import random

class DataAgent:
    def __init__(self, host, port, backend_controller_url, is_leader=False):
        self.host = host
        self.port = port
        self.backend_controller_url = backend_controller_url
        self.is_leader = is_leader
        self.data_store = {}
        self.sequence_numbers = {}
        self.assigned_partitions = {}
        self.followers = {}  # Format: {partition_id: [follower WebSocket URLs]}
        self.heartbeat_interval = 5  # seconds

        # Heartbeat settings
        self.heartbeat_interval = 5  # seconds
        self.last_heartbeat = {}  # Last heartbeat timestamp for each follower

    async def start(self):
        """ Start the WebSocket server to listen for incoming connections. """
        async with websockets.serve(self.handle_connection, self.host, self.port):
            await asyncio.Future()  # Run forever

    async def handle_connection(self, websocket, path):
        """ Handle incoming WebSocket connection. """
        async for message in websocket:
            data = json.loads(message)
            if self.is_leader:
                await self.process_message_as_leader(data, websocket)
            else:
                await self.process_message_as_follower(data)

    def start_heartbeat(self):
        """ Start sending heartbeat messages if the agent is a leader. """
        if self.is_leader:
            while True:
                self.send_heartbeat_to_followers()
                time.sleep(self.heartbeat_interval)

    async def handle_connection(self, websocket, path):
        """ Handle incoming WebSocket connection. """
        async for message in websocket:
            data = json.loads(message)
            if self.is_leader:
                await self.process_message_as_leader(data, websocket)
            else:
                await self.process_message_as_follower(data)

    async def respond_to_heartbeat(self, message):
        """ Respond to a heartbeat message from the leader. """
        ack_message = json.dumps({'type': 'heartbeat_ack', 'timestamp': message['timestamp']})
        # Send the ack_message to the leader (implementation depends on leader's WebSocket URL)
        pass  # Placeholder for sending the acknowledgment to the leader

    async def process_message_as_leader(self, message, websocket):
        if message.get('type') == 'data':
            topic = message['topic']
            partition = message['partition']
            sequence_number = message['sequence_number']

            with self.lock:
                if partition not in self.data_store:
                    self.data_store[partition] = []
                    self.sequence_numbers[partition] = -1

                if sequence_number == self.sequence_numbers[partition] + 1:
                    self.data_store[partition].append(message)
                    self.sequence_numbers[partition] = sequence_number
                    await self.replicate_to_followers(partition, message)
                else:
                    print(f"Sequence number mismatch in partition {partition}.")
        elif message.get('type') == 'heartbeat_ack':
            # Process heartbeat acknowledgment
            pass  # Placeholder for handling heartbeat acknowledgments

    async def replicate_to_followers(self, partition, message):
        """ Replicate data to follower agents using WebSockets. """
        followers = self.followers.get(partition, [])
        for url in followers:
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(message))

    async def push_data_to_subscribers(self, partition, message):
            """
            Push data to all subscribed clients for a given partition.
            """
            # Retrieve the list of subscribers for the partition from the backend controller
            subscribers = await self.get_subscribers_for_partition(partition)
            
            for subscriber in subscribers:
                try:
                    # Establish a WebSocket connection to the subscriber and send the message
                    async with websockets.connect(f"ws://{subscriber['address']}:{subscriber['port']}") as websocket:
                        await websocket.send(json.dumps(message))
                except Exception as e:
                    print(f"Error in sending data to subscriber {subscriber}: {e}")

    async def get_subscribers_for_partition(self, partition):
        """
        Retrieve the list of subscribers for a specific partition from the backend controller.
        """
        try:
            response = await requests.get(f"{self.backend_controller_url}/subscribers?partition={partition}")
            if response.status_code == 200:
                return response.json()  # Assuming the response is a JSON list of subscribers
            else:
                print(f"Failed to retrieve subscribers for partition {partition}")
                return []
        except Exception as e:
            print(f"Error in retrieving subscribers: {e}")
            return []
        


    async def start_leader_election(self):
            """
            Initiates a leader election process using the bully algorithm.
            """
            self.is_leader = False
            await self.notify_others_for_election()
            await asyncio.sleep(random.uniform(0.5, 1.5))  # Random wait to avoid message collision
            if not self.received_higher_priority_message:
                self.is_leader = True
                await self.notify_others_leader_election_result()
    
    async def notify_others_for_election(self):
        """
        Notify other agents that a leader election is taking place.
        """
        self.received_higher_priority_message = False
        for agent in self.other_agents:  # Assuming a list of other agents' addresses
            try:
                async with websockets.connect(f"ws://{agent['address']}:{agent['port']}") as websocket:
                    await websocket.send(json.dumps({'type': 'election', 'id': self.id}))
            except Exception as e:
                print(f"Error in sending election message to agent {agent}: {e}")

    async def notify_others_leader_election_result(self):
        """
        Notify other agents about the election result.
        """
        for agent in self.other_agents:
            try:
                async with websockets.connect(f"ws://{agent['address']}:{agent['port']}") as websocket:
                    await websocket.send(json.dumps({'type': 'new_leader', 'id': self.id}))
            except Exception as e:
                print(f"Error in sending new leader message to agent {agent}: {e}")

    async def handle_election_messages(self, message):
            """
            Handle incoming messages related to leader election.
            """
            if message.get('type') == 'election' and message['id'] > self.id:
                self.received_higher_priority_message = True
            elif message.get('type') == 'new_leader':
                self.is_leader = False  # A new leader has been elected

# Example usage
backend_controller_url = 'http://localhost:8080'
agent = DataAgent('localhost', 5000, backend_controller_url, is_leader=True)

# Start the agent and heartbeat in async context
async def main():
    await agent.start()

asyncio.run(main())