import socketio

# create a Socket.IO server
sio = socketio.AsyncServer(async_mode='asgi')

# wrap with ASGI application
app = socketio.ASGIApp(sio)

client_list = []

@sio.event
def connect(sid, environ):
    client_list.append(sid)
    print(client)
    print('connect ', sid)

@sio.event
def disconnect(sid):
    print('disconnect ', sid)
    client_list.erase(sid)
    print(client)

async def app(scope, receive, send):
    assert scope['type'] == 'http'
    await send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [
            [b'content-type', b'text/plain'],
        ]
    })
    await send({
        'type': 'http.response.body',
        'body': b'Hello, world!',
    })