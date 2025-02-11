import json
from uuid import uuid4
from typing import Any, Optional
from collections import OrderedDict

from pydantic import BaseModel, PrivateAttr


class Message(BaseModel):
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    data: dict[str, Any]


class Channel(BaseModel):
    name: str
    ready_messages: OrderedDict[str, Message] = OrderedDict()
    unacked_messages: OrderedDict[str, Message] = OrderedDict()


class Storage:
    def __init__(self):
        self.channels: dict[str, Channel] = {}

    async def register_channel(self, channel_name: str):
        if channel_name in self.channels:
            raise ValueError(f"Channel {channel_name} already exists")
        self.channels[channel_name] = Channel(name=channel_name)

    async def get_message(self, channel_name: str) -> Optional[Message]:
        if channel_name not in self.channels:
            return None
        if messages := self.channels[channel_name].ready_messages:
            _, message = messages.popitem(last=False)
            self.channels[channel_name].unacked_messages[message._id] = message
            return message

    async def put_message(self, channel_name: str, message: Message) -> None:
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages[message._id] = message
        else:
            raise ValueError(f"Channel {channel_name} does not exist")

    async def confirm_message(self, channel_name: str,
                              message_id: str) -> bool:
        if channel_name not in self.channels:
            return False
        return self.channels[channel_name].unacked_messages.pop(message_id,
                                                                False)

    async def purge_channel_messages(self, channel_name: str) -> None:
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages.clear()
            self.channels[channel_name].unacked_messages.clear()


class Broker:
    def __init__(self):
        self.storage = Storage()

    async def __call__(self, scope, receive, send):
        assert scope['type'] == 'http'

        result = await self.route(scope, receive)

        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ],
        })
        await send({
            'type': 'http.response.body',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ],
            'body': bytes(f"{dict(result=result)}", encoding="utf-8"),
        })

    async def route(self, scope, receive):
        body = await self.read_body(receive)

        if scope.get('path') == '/register':
            try:
                await self.storage.register_channel(body.get('channel'))
                return f'Channel {body.get('channel')} successfully registred'
            except ValueError as err:
                return str(err)

        if scope.get('path') == '/send':
            message = Message.model_validate(body)
            try:
                await self.storage.put_message(body.get('channel'), message)
                return {'data': message.data, 'message_id': message._id}
            except ValueError as err:
                return str(err)

        if scope.get('path') == '/read':
            message = await self.storage.get_message(body.get('channel'))
            if message:
                return {'data': message.data, 'message_id': message._id}
            else:
                return 'No messages in channel'

        if scope.get('path') == '/confirm':
            result = await self.storage.confirm_message(body.get('channel'),
                                                        body.get('message_id'))
            if result:
                return 'Message confirmed!'
            else:
                return 'No such message'

        if scope.get('path') == '/purge':
            await self.storage.purge_channel_messages(body.get('channel'))

    async def read_body(self, receive):
        body = b''
        more_body = True

        while more_body:
            message = await receive()
            body += message.get('body', b'')
            more_body = message.get('more_body', False)

        return json.loads(body)


app = Broker()
