import websockets
import roundrobin
import asyncio
import json
from collections import defaultdict

class FrontendController:
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.agents = {}
        self.publishers = defaultdict(str)
        self.topics = defaultdict(set)
        
    
    def start(self):
        """ Start the FrontendController. """
        # Start the websocket server
        self.start_websocket_server()
        
        # Start the websocket client
        self.start_websocket_client()

    def start_websocket_server(self):
        """ Start the websocket server. """
        print("Starting websocket server...")
        start_server = websockets.serve(self.listen_to_frontend_agent, self.host, self.port)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()
    
    def listen_to_clients(self, websocket, path):
        """ Listen to clients. """
        while True:
            message = websocket.recv()
            print(f"Received message from client: {message}")
                        
    def load_balance(self):
        """ Load balance the incoming messages. """
        message_agent = roundrobin.basic(self.agents.keys())
        return message_agent
    
    async def listen_to_frontend_controller(self, websocket):
        """ Listen for messages from the FrontendController. """
        async for message in websocket:
            data = json.loads(message)
            print("Received subscription request!")
            await self.handle_subscription(data)

    def send_agent_information(self):
        choosen_agent = load_balance()
    
    async def handle_subscription(self, data):
        """ Handle subscription request from FrontendController. """
        # client_address = data['publisher_info']['publisher_address']
        topic = data['Topic']
        pub_num = data['PubNum']

        # Store subscription info
        self.topics[topic].add(pub_num)
        self.publishers[pub_num] = topic

        # Send subscription info to agents
        await self.send_subscription_info_to_agents(topic, pub_num)

        
        self.subscriptions[topic][partition].append(client_address)
        print(f"Added {client_address} to subscriptions: subscriptions[{topic}][{partition}] = ", self.subscriptions[topic][partition])

    async def connect_agent(self):
        pass


def start_frontend_controller_listener(agent):
        host = "127.0.0.1"
        port = 1350

        # Create a new event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        server = websockets.serve(agent.listen_to_frontend_controller, host, port)
        print(f"Backend controller listener server running on ws://{host}:{port}")

        loop.run_until_complete(server)
        loop.run_forever()