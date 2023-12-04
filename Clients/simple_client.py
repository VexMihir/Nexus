import socket
import json
import websockets
import asyncio

def send_mock_subscription_request(client_address, client_port):
    subscriptions = ["Temperature"]
    controller_host = "127.0.0.1"
    controller_port = 12345

    # Create a socket connection to the server
    print("Attempting to connect to controller")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((controller_host, controller_port))

        # Prepare the JSON request
        request_data = {"action": "subscribe", "subscriptions": subscriptions, "socket-address": f"{controller_host}:{controller_port}"}
        print(request_data)
        request_json = json.dumps(request_data)

        # Send the request to the server
        client_socket.sendall(request_json.encode('utf-8'))

        # Receive and print the server's response
        response = client_socket.recv(1024).decode('utf-8')
        print(f"Received response from the server: {response}")

async def handle_agent_connection(websocket):
    print(f"Received connection")
    try:
        async for message in websocket:
            print(f"Received message from agent: {message}")
            await websocket.send(f"Server received: {message}")
    except websockets.exceptions.ConnectionClosedOK:
        print(f"Connection closed by the agent")


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 23456

    # Accept incoming WebSocket connections from agents
    agent_server = websockets.serve(handle_agent_connection, host, port)  # Adjust the port as needed
    print(f"Client server running on ws://{host}:{port}")
    asyncio.get_event_loop().run_until_complete(agent_server)

    print("Reached")
    send_mock_subscription_request(host, port)
    asyncio.get_event_loop().run_forever()

