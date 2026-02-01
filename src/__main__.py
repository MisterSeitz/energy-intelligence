import asyncio

from .daily_power_actor import PowerIntelligence

async def main():
    actor = PowerIntelligence()
    await actor.run()

asyncio.run(main())
