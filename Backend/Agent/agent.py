import json
import asyncio
import websockets
import threading
import time
import random

class DataAgent:
    def __init__(self, host, port, backend_controller_url, agent_id, other_agents_info, is_leader=False):
        self.host = host
        self.port = port
        self.backend_controller_url =  backend_controller_url
        self.is_leader = is_leader
        # Dictionary {Topic ---> Dictionary {Partition# ---> [ClientAddys] (list) }}
        self.subscriptions = {}  # Stores client subscriptions by topic and partition
        self.followers = []  # Format: {partition_id: [follower WebSocket URLs]}
        self.heartbeat_interval = 5  # seconds

        # Dictionary: {Topic ---> Dictionary {Partition# ---> Queue (List)}}
        self.data = {}

        # Data lock. Ensures threads do not conflict when modifying self.data
        self.datalock = threading.Lock()

        self.agent_id = agent_id  # Unique identifier for this agent
        self.other_agents_info = other_agents_info  # Information about other agents
        self.received_higher_priority_message = False  # Flag for election process

    async def start(self):
        await self.start_heartbeat()
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

        # Create a new event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        host = "127.0.0.1"
        start_server = websockets.serve(agent.listen_to_producer, host, 0)

        # Run the server and retrieve the port number
        server = loop.run_until_complete(start_server)
        assigned_port = server.sockets[0].getsockname()[1]
        print(f"Producer listener server running on ws://{host}:{assigned_port}")

        loop.run_until_complete(agent.send_port_info_to_frontend(assigned_port))

        loop.run_forever()

    async def send_port_info_to_frontend(self, port):
            """ Send the dynamically assigned port to another WebSocket address. """
            other_websocket_url = 'ws://127.0.0.1:1350'  # Replace with actual URL
            try:
                async with websockets.connect(other_websocket_url) as websocket:
                    await websocket.send(json.dumps({'agent_id': self.agent_id, 'port': port}))
            except Exception as e:
                print(f"Error in sending port information: {e}")

    async def start_leader_election(self):
        """ Initiates a leader election process using the bully algorithm. """
        print(f"Agent {self.agent_id} starting election process")
        self.is_leader = False
        self.received_higher_priority_message = False
        await self.notify_others_for_election()
        await asyncio.sleep(random.uniform(2, 4))  # Random wait to avoid message collision
        if not self.received_higher_priority_message:
            self.is_leader = True
            print(f"Agent {self.agent_id} is elected as leader")
            await self.notify_others_leader_election_result()
    
    async def notify_others_for_election(self):
        """ Notify other agents that a leader election is taking place. """
        for agent_info in self.other_agents_info:
            if agent_info['id'] > self.agent_id:  # Notify only agents with higher IDs
                try:
                    print("yay?")
                    async with websockets.connect(f"ws://{agent_info['address']}:{agent_info['port']}") as websocket:
                        print("yay")
                        await websocket.send(json.dumps({'type': 'election', 'id': self.agent_id}))
                except Exception as e:
                    print(f"Error in sending election message to agent {agent_info['id']}: {e}")

    async def notify_others_leader_election_result(self):
        """ Notify other agents about the election result. """
        for agent_info in self.other_agents_info:
            try:
                async with websockets.connect(f"ws://{agent_info['address']}:{agent_info['port']}") as websocket:
                    print("got here")
                    await websocket.send(json.dumps({'type': 'new_leader', 'id': self.agent_id}))
            except Exception as e:
                print(f"Error in sending new leader message to agent {agent_info['id']}: {e}")

    async def handle_election_messages(self, message):
        """ Handle incoming messages related to leader election. """
        if message.get('type') == 'election' and message['id'] > self.agent_id:
            self.received_higher_priority_message = True
            print(f"Agent {self.agent_id} received higher priority election message from Agent {message['id']}")
            

    def start_agent_listener(self):
        """ Start the WebSocket server for the agent. """
        # Create a new event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Define the WebSocket server for this agent
        server = websockets.serve(self.handle_agent_messages, self.host, self.port)
        print(f"Agent listener server running on ws://{self.host}:{self.port}")
        loop.run_until_complete(server)
        loop.run_forever()
    
    async def handle_agent_messages(self, websocket, path):
            """ Handle messages from other agents (like leader election). """
            async for message in websocket:
                data = json.loads(message)
                if data.get('type') == 'election':
                    await self.handle_election_messages(data)
                elif data.get('type') == 'new_leader':
                    if data['id'] != self.agent_id:
                        self.is_leader = False
                        print(f"Agent {self.agent_id} acknowledges new leader: Agent {data['id']}")
                        await self.start_heartbeat()
                elif data.get('type') == 'heartbeat':
                    print("heartbeat received! Sending ack...")
                    await websocket.send(json.dumps({'type': 'heartbeat', 'id': self.agent_id}))
                    print("ack sent from follower")
                        
    # initiating heartbeat
    #note: have to recall everytime new leader is selected
    async def start_heartbeat(self):
        """ Start sending heartbeat messages if the agent is a leader. """
        if self.is_leader:
            while True:
                await self.send_heartbeat_to_followers()
                time.sleep(self.heartbeat_interval)   

    # leader sending out heartbeat
    async def send_heartbeat_to_followers(self):
        print("Prepping to send heartbeat to followers")
        for follower in self.followers:
            websocket_url = "ws://" + follower.get("address") + ":" + str(follower.get("port"))
            try:
                async with websockets.connect(websocket_url) as websocket:
                    await websocket.send(json.dumps({'type': 'heartbeat', 'id': follower.get("agent_id")}))
                    print("heartbeat sent!")

                    #response will be id of the follower being acked
                    response = await websocket.recv()
                    print("ack received!")
                    print(f"Received ack from {response}" )
            except:
                print("bruh something went so wrong idk how to fix it")
        

if __name__ == '__main__':
    # Example usage
    backend_controller_url = 'ws://http://127.0.0.1:8000'

    # Code to start leader election process must be done manually. The process is done within main in agent.py
    #for testing
    agents_info = [
        {
            'id': 1,  # Unique identifier for the first agent
            'address': '127.0.0.1',  # Localhost address
            'port': 5001  # Port number for the first agent
        },
        {
            'id': 2,  # Unique identifier for the second agent
            'address': '127.0.0.1',  # Localhost address
            'port': 5002  # Port number for the second agent
        }
    ]

    agent1 = DataAgent('localhost', 5001, backend_controller_url, 1, agents_info, is_leader=True)
    agent2 = DataAgent('localhost', 5002, backend_controller_url, 2, agents_info, is_leader=False)

    # Start WebSocket servers for each agent
    thread1 = threading.Thread(target=agent1.start_agent_listener)
    thread2 = threading.Thread(target=agent2.start_agent_listener)
    thread3 = threading.Thread(target=agent2.start_producer_listener)
    thread4 = threading.Thread(target=agent2.start_backend_controller_listener)
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    # Wait for WebSocket servers to be fully up and running
    time.sleep(5)  # Adjust this delay as needed

    # Now start the leader election process
    thread5 = threading.Thread(target=asyncio.run, args=(agent1.start(),))
    thread6 = threading.Thread(target=asyncio.run, args=(agent2.start(),))
    thread5.start()
    thread6.start()

    # Wait for threads
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()

    # Run this to try leader election

    # thread7 = threading.Thread(target=asyncio.run, args = (agent1.start_leader_election(),))
    # thread7.start()
    # thread7.join()