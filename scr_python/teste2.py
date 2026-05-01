import asyncio
async def soma1():
    a = 1 + 2
    return a
    await asyncio.sleep(8)  # simula 3 segundos fazendo café
    print(a)

async def soma2(x):
    b = x + 1
    await asyncio.sleep(5)  # simula 2 segundos torrando pão
    print(b)

async def main():
    # executa os dois ao mesmo tempo
    await asyncio.gather(soma1(), soma2(a))

asyncio.run(main())