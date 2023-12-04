import asyncio
import json
import websockets

async def initiate_websocket_connection(client_info):
    # Extract necessary client information
    client_address = client_info.get('client_address')

    # Build the WebSocket connection URL
    websocket_url = f"ws://{client_address}"

    try:
        print("Trying to create ws connection to ", websocket_url)
        # Establish a WebSocket connection with the client
        async with websockets.connect(websocket_url) as websocket:
            await websocket.send(json.dumps({'message': 'Agent sent message'}))
            response = await websocket.recv()
            print(f"Received response from client: {response}")
    except Exception as e:
        print(f"Error establishing WebSocket connection with the client: {str(e)}")

async def handle_subscription(websocket):
    try:
        # Receive client information from the subscription controller
        data = await websocket.recv()
        data = json.loads(data)

        # Extract client information
        client_info = data.get('client_info')

        topic = data.get('topic')
        partition = data.get('partition_number')

        print(f"Received subscription request from {client_info}")

        await initiate_websocket_connection(client_info)
    except Exception as e:
        print(f"Error handling subscription: {str(e)}")

if __name__ == "__main__":
    # Adjust the host and port based on your requirements
    host = "127.0.0.1"
    port = 8000

    server = websockets.serve(handle_subscription, host, port)

    print(f"Agent server running on ws://{host}:{port}")

    asyncio.get_event_loop().run_until_complete(server)
    asyncio.get_event_loop().run_forever()
