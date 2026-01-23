from fastapi import FastAPI, Request



app = FastAPI()


@app.get("/")
async def root(request: Request):
    return 'ok'

@app.post("/echo")
async def echo(request: Request):
    body = await request.body()
    return body.decode()
