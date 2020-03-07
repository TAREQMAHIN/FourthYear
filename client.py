import socketio

# asyncio
sio = socketio.AsyncClient()

@sio.event
def connect():
    print("I'm connected!")

@sio.event
def connect_error():
    print("The connection failed!")

@sio.event
def disconnect():
    print("I'm disconnected!")

await sio.connect('http://localhost:8000')
print('my sid is', sio.sid)