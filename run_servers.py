import asyncio

from Backend.Agent.agent import main as agent_main
from Backend.Controller.backend_controller import main as backend_controller_main

async def run_all_servers():
    await agent_main()
    await backend_controller_main()

if __name__ == '__main__':
    asyncio.run(run_all_servers())