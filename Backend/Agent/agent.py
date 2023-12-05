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
        # Dictionary {Topic ---> Dictionary {Partition# ---> [ClientAddys] (list) }}
        self.subscriptions = {}  # Stores client subscriptions by topic and partition
        self.followers = []  # Format: {partition_id: [follower WebSocket URLs]}
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
        host = "127.0.0.1"
        port = 8001

        # Create a new event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        server = websockets.serve(agent.listen_to_producer, host, port)
        print(f"Producer listener server running on ws://{host}:{port}")

        loop.run_until_complete(server)
        loop.run_forever()


    # initiating heartbeat
    #note: have to recall everytime new leader is selected
    def start_heartbeat(self):
        """ Start sending heartbeat messages if the agent is a leader. """
        if self.is_leader:
            while True:
                self.send_heartbeat_to_followers()
                time.sleep(self.heartbeat_interval)
                #check heartbeat acks from followers

        # not leader so listening for heartbeat instead
        # if not self.is_leader:
        #     while True:
        #         # awaiting for heartbeat
        #         message = await 
                

    # leader sending out heartbeat
    async def send_heartbeat_to_followers(self):
        print("Prepping to send heartbeat to followers")
        for follower in self.followers:
            websocket_url = "ws://" + follower.address + ":" + follower.port

            try:
                async with websockets.connect(websocket_url) as websocket:
                    await websocket.send("Heartbeat")
                    print("heartbeat sent!")

                    #response will be id of the follower being acked
                    response = await websocket.recv()
                    print(f"Received ack from {response}" )
            except:
                # note: do logic for failed shit here.
                # note: check for timeout in this case
                print("bruh something went so wrong idk how to fix it")

if __name__ == '__main__':
    # Example usage
    backend_controller_url = 'ws://http://127.0.0.1:8000'
    agent = DataAgent('localhost', 5000, backend_controller_url, is_leader=True)
    thread1 = threading.Thread(target=DataAgent.start_backend_controller_listener, args=(agent,))
    thread2 = threading.Thread(target=DataAgent.start_producer_listener, args=(agent,))
    thread3 = threading.Thread(target=asyncio.run, args=(agent.start(),))

    # Start the threads
    thread1.start()
    thread2.start()
    thread3.start()

    # Wait for both threads to finish (they won't)
    thread1.join()
    thread2.join()
    thread3.join()
