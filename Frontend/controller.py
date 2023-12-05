import websockets
import roundrobin
import asyncio

class FrontendController:
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
    
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
            print(f"Received message from client: {message}")\
            
    def load_balance(self):
        """ Load balance the incoming messages. """
        pass