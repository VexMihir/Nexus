import asyncio
import websockets
import random
import json
import time

#Vars
publisher_num = 1 #input("type the publisher number")
topic = "Temperature" #input("what topic does this publisher belong to")

title = "Thermometer" #input("what is this device's title")
desc = "Some Thermometer" # input("device description")
temperature_data = 0

async def respond_to_ping(websocket, ping_data):
    # Respond to ping frames with a pong frame
    await websocket.pong(ping_data)

async def connect_to_frontend():
    frontend_uri = "ws://127.0.0.1:1350"
    async with websockets.connect(frontend_uri) as websocket:
        print("Connected to WebSocket")

        # Start a task to respond to ping frames
        ping_response_task = asyncio.create_task(respond_to_ping(websocket, b''))

        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1)
                if isinstance(message, bytes):
                    # If the received message is a ping frame, respond with a pong frame
                    await ping_response_task
                else:
                    print(f"Received message: {message}")
                    topic = message.topic           #!!!AMOGH pls change this part, I'm not sure how the message will be formatted
                    connect_to_agent(message.port)
                    break
            except asyncio.TimeoutError:
                # No message received within the timeout, continue with other tasks
                pass

            # Simulate sending data periodically
            message = {
                "PubNum": publisher_num,
                "Topic": topic,
                "Data": {"temperature": temperature_data, "title": title, "desc": desc}
            }

            await websocket.send(json.dumps(message))
            print(f"Sent message: {message}")

        # Cancel the ping response task when the connection is closed
        ping_response_task.cancel()

async def connect_to_agent(port):
    send_delay = 5

    uri = f"ws://127.0.0.1:{port}" 
    
    # Frontend controller logic
    partition = 1    
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        # Start a task to respond to ping frames
        ping_response_task = asyncio.create_task(respond_to_ping(websocket, b''))

        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1)
                if isinstance(message, bytes):
                    # If the received message is a ping frame, respond with a pong frame
                    await ping_response_task
                else:
                    print(f"Received message: {message}")
            except asyncio.TimeoutError:
                # No message received within the timeout, continue with other tasks
                pass

            # Simulate sending data periodically
            temperature_data += random.uniform(-5, 5)
            message = {
                "PubNum": publisher_num,
                "Topic": topic,
                "Partition": partition,
                "Data": {"temperature": temperature_data, "title": title, "desc": desc}
            }

            await websocket.send(json.dumps(message))
            print(f"Sent message: {message}")

            time.sleep(send_delay)

        # Cancel the ping response task when the connection is closed
        ping_response_task.cancel()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_frontend())