import asyncio
import websockets
import random
import time

async def connect_to_websocket():
    send_delay = 5
    temperature_data = 0

    port = input("type the port number of the agent")
    uri = f"ws://127.0.0.1:{port}"  # Replace with your WebSocket server URI
    publisher_num = input("type the publisher number")
    topic = input("what topic does this publisher belong to")

    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        while True:
            temperature_data += random.uniform(-5, 5)
            
            # if message.lower() == 'exit':
            #     break

            message = {"PubNum": publisher_num, "Topic": topic, "Data": temperature_data}

            await websocket.send(message)
            print(f"Sent message: {message}")

            response = await websocket.recv()
            print(f"Received response: {response}")

            time.sleep(send_delay)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_websocket())