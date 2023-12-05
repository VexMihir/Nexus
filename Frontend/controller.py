import websockets
import roundrobin
import asyncio
import json
import uuid
from collections import defaultdict

class FrontendController:
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.agents = defaultdict(lambda: defaultdict(list))
        self.agents_ports = defaultdict(lambda: defaultdict(list))
        self.publishers = defaultdict(set)
        self.topics = defaultdict(set)
        self.topic_partitions = defaultdict(lambda: defaultdict(list))
        self.round_robin = {} 
        self.round_robin_agents = {} # first part will be topics and partitions, second will be lower agents
        self.leader_agents = {}
        self.ping_data = b'alive'
        self.agent_id_to_port = {}
    

    def initialize_topic(self, topic):
        self.topic_partitions[topic]['parition_0'] = []
        self.topic_partitions[topic]['parition_1'] = []
        self.topic_partitions[topic]['parition_2'] = []
        self.round_robin[topic] = roundrobin.basic(self.topic_partitions[topic].keys())
        self.topics[topic] = set() # why set?
    
    def add_topic_partition(self, topic):
        """ Add a partition to the topic. """
        partition_num = len(self.topic_partitions[topic])
        self.topic_partitions[topic][f'partition_{partition_num}'] = []
        self.round_robin[topic] = roundrobin.basic(self.topic_partitions[topic].keys())
    
    def remove_topic_partition(self, topic):
        """ Remove a partition from the topic. """
        partition_num = len(self.topic_partitions[topic]) - 1
        del self.topic_partitions[topic][f'partition_{partition_num}']
        self.round_robin[topic] = roundrobin.basic(self.topic_partitions[topic].keys())
    
    # async def add_agent(self, data):
    #     """ Add an agent to the FrontendController. """
    #     tp = data['topic_partition']
    #     self.agents[

    async def respond_to_ping(self, websocket, ping_data):
        """ Respond to ping frames with a pong frame. """
        await websocket.pong(ping_data)

    def round_robin(self, topic):
        """ Round robin the topic. """
        agent = self.round_robin[topic]()
        return agent
    
    def round_robin_agent(self):
        """ Round robin the topic. """
        agent = roundrobin.basic(self.agents_ports.keys())
        return agent
    async def send_publisher_info_to_agent(self, port, topic, partition, pub_port, websocket):
        """ Send publisher info to agent. """
        message = {
            "topic": topic,
            "partition": partition,
            "agent": port
        }
        message_json = json.dumps(message)
        websocket_url = f"ws://{self.host}:{pub_port}"
        print(f"Sending publisher info to publisher: {port}")
        try: 
            async with websockets.connect(websocket_url) as websocket:
                await websocket.send(message_json)
                print(f"Sent publisher info to publisher: {pub_port}")
        except Exception as e:
            print(f"Error sending publisher info to publisher: {e}")
    
    async def add_agent(self, data, websocket):
        agent_id = data['agent_id']
        port = data['port']
        sent_data = {}

        # add agent to agents
        if len(self.leader_agents) == 0: # if no leader agent
            self.leader_agents = {agent_id: port}
            self.round_robin_agents[agent_id] = {'nodes': [], 'topics': {}}
            self.agent_id_to_port[agent_id] = port
            senn_data = {'leader': True}
        else:
            leader = self.round_robin_agent()
            self.round_robin_agents[leader]['nodes'].append(agent_id)
            self.agent_id_to_port[agent_id] = port
        
        # need to do a bit more here

    async def remove_agent(self, data, websocket):
        agent_id = data['agent_id']
        port = data['port']
        sent_data = {}

        # remove agent from agents
        if agent_id in self.leader_agents:
            del self.leader_agents[agent_id]
            del self.round_robin_agents[agent_id]
            del self.agent_id_to_port[agent_id]
        else:
            leader = self.round_robin_agent()
            self.round_robin_agents[leader]['nodes'].remove(agent_id)
            del self.agent_id_to_port[agent_id]
        
        # need to do a bit more here
    
    async def ping_leaders(self):
        """ Ping the leader agents. """
        for agent_id, port in self.leader_agents.items():
            websocket_url = f"ws://{self.host}:{port}"
            i = 0
            while i < 3:
                try: 
                    async with websockets.connect(websocket_url) as websocket:
                        await websocket.send(json.dumps({'type': 'heartbeat'}))
                        i = 8
                except websockets.exceptions.ConnectionClosedError:
                    print("connection closed, retrying...")
                    await asyncio.sleep(2)
                    i += 1
                except Exception as e:
                    print(f"Error pinging agent: {e}")
            if i == 8:
                return
            else:
                self.start_leader_election(agent_id, port)
                # remove agent from agents
                del self.leader_agents[agent_id]
                del self.round_robin_agents[agent_id]
                del self.agent_id_to_port[agent_id]
    
    async def start_leader_election(self, agent_id):
        """ Start leader election. """
        followers = self.round_robin_agents[agent_id]['nodes']
        # send leader election message to followers
        for follower in followers:
            websocket_url = f"ws://{self.host}:{self.agent_id_to_port[follower]}"
            i = 0
            while i < 3:
                try: 
                    async with websockets.connect(websocket_url) as websocket:
                        await websocket.send(json.dumps({'type': 'election'}))
                        i = 8
                except websockets.exceptions.ConnectionClosedError:
                    print("connection closed, retrying...")
                    await asyncio.sleep(2)
                    i += 1
                except Exception as e:
                    print(f"Error sending leader election message to follower: {e}")

    async def handle_publisher_info(self, data, websocket):
        """ Handle publisher info from FrontendAgent. """
        pub_name = uuid.uuid4() # might be better to just have the publisher deal with it

        # need 
        topic = data['topic']
        pub_port = data['pubPort']

        if topic not in self.topics:
            self.initialize_topic(topic)
        else:
            partition = self.round_robin(topic)
        
        # get agent port
        port = self.agents_ports[topic][partition]
        
        self.publishers[pub_name] = {
            'topic': topic,
            'partition': partition,
            'agent': port # agent port
        }

        # send publisher info to agent
        await self.send_publisher_info_to_agent(port, topic, partition, pub_port, websocket)

    async def pong(self, websocket):
        """ Respond to ping frames with a pong frame. """
        await websocket.pong(self.ping_data)

    async def handle_message(self, websocket):
        """ Handle messages to FrontendAgent. """
        try:
            message = await websocket.recv()
            json_message = json.loads(message)
            if isinstance(message, bytes):
                # If the received message is a ping frame, respond with a pong frame
                await self.pong(websocket)
            elif json_message['action'] == 'add_agent':
                await self.add_agent(json_message, websocket)
            elif json_message['action'] == 'remove_agent':
                await self.remove_agent(json_message, websocket)
            elif json_message['action'] == 'get_agent':
                await self.handle_publisher_info(json_message, websocket)
            else:
                print(f"Received message: {json_message}")
            
        except asyncio.TimeoutError:
            # No message received within the timeout, continue with other tasks
            pass
    


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 1350

    # Create a new event loop for the thread
    agent = FrontendController(host, port)
    server = websockets.serve(agent.handle_message, host, port)
    print(f"Backend controller listener server running on ws://{host}:{port}")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server)
    ping_leaders = asyncio.get_event_loop()
    ping_leaders.call_later(60, agent.ping_leaders)

    ping_leaders.run_forever()


    asyncio.get_event_loop().run_forever()