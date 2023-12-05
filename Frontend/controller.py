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
        self.topic_partitions = defaultdict(defaultdict(list))
        # self.partitions = (defaultdict(list))
        self.round_robin = {}
        
        
    
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
                        
    def load_balance(self, topic):
        """ Load balance the incoming messages. """
        if topic in self.round_robin:
            agent = self.round_robin[topic]()
            return agent
        else:
            self.round_robin[topic] = roundrobin.basic([self.topic_partitions[topic]])
            agent = self.round_robin[topic]()
            return agent
    
    def add_parition(self, topic, agent):
        """ Add a partition to a topic. """
        self.topic_partitions[topic].append(agent)

        
    
    # async def listen_to_frontend_controller(self, websocket):
    #     """ Listen for messages from the FrontendController. """
    #     async for message in websocket:
    #         data = json.loads(message)
    #         print("Received subscription request!")
    #         await self.handle_subscription(data)
    
    async def handle_subscription(self, data):
        """ Handle subscription request from FrontendController. """
        # client_address = data['publisher_info']['publisher_address']
        topic = data['Topic']
        pub_num = data['PubNum']

        # get parition for topic
        partition_num = self.topic_partitions[topic]

        self.publishers[pub_num] = f'{topic}/{}'

        # get parition for topic
        partition_num = self.topic_partitions[topic]

        # Store subscription info
        self.topics[topic].add(pub_num)
        self.topic_partitions[parition_num] = topic

        self.partitions[topic][pub_num] = 

        # select agent to send subscription info to
        agent = self.load_balance()
        print(f"Sending agent info to publisher: {agent}")
        await self.send_agent_info_to_publisher(agent, topic, pub_num)

        
        self.subscriptions[topic][partition].append(client_address)
        print(f"Added {client_address} to subscriptions: subscriptions[{topic}][{partition}] = ", self.subscriptions[topic][partition])
    
    async def send_agent_info_to_publisher(self, agent, topic, pub_num):
        """ Send subscription info to agent. """
        # Create subscription message
        message = {
            "action": "subscribe",
            "topic": topic,
            "pub_num": pub_num
        }
        message_json = json.dumps(message)

        # Send subscription message to agent
        async with websockets.connect(agent) as websocket:
            await websocket.send(message_json)

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