import json
import logging
from typing import Any
from uuid import uuid4
from collections import OrderedDict

from pydantic import BaseModel, PrivateAttr


console_out = logging.StreamHandler()
logging.basicConfig(handlers=[console_out],
                    format="[%(levelname)s]: %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

AnyJSON = dict[str, Any]


class Message(BaseModel):
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    message: AnyJSON


class Channel:
    def __init__(self, name: str):
        self.name: str = name
        self.ready_messages: OrderedDict[str, Message] = OrderedDict()
        self.unacked_messages: OrderedDict[str, Message] = OrderedDict()

    def __repr__(self):
        return f'Channel(name={self.name}, ' \
               f'ready_messages={self.ready_messages}, ' \
               f'unacked_messages={self.unacked_messages})'


class Storage:
    def __init__(self):
        self.channels: dict[str, Channel] = {}

    async def get_message(self, channel_name: str):
        if channel_name not in self.channels:
            return None
        if messages := self.channels[channel_name].ready_messages:
            _, message = messages.popitem(last=False)
            self.channels[channel_name].unacked_messages[message._id] = message
            return message

    async def put_message(self, channel_name: str, message: Message):
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages[message._id] = message
        else:
            chan = Channel(name=channel_name)
            chan.ready_messages[message._id] = message
            self.channels[channel_name] = chan

    async def confirm_message(self, channel_name: str, message_id: str):
        if channel_name not in self.channels:
            return None
        if self.channels[channel_name].unacked_messages.get(message_id):
            del self.channels[channel_name].unacked_messages[message_id]

    async def purge_channel_messages(self, channel_name: str):
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages.clear()
            self.channels[channel_name].unacked_messages.clear()


class Broker:
    def __init__(self):
        self.storage = Storage()

    async def __call__(self, scope, receive, send):
        assert scope['type'] == 'http'

        result = await self.route(scope, receive, send)
        logger.info(self.storage.channels)

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

    async def route(self, scope, receive, send):
        body = await self.read_body(receive)

        if scope.get('path') == '/send':
            message = Message.model_validate(body)
            await self.storage.put_message(body.get('channel'), message)
            return {'message': message.message, 'message_id': message._id}

        if scope.get('path') == '/read':
            message = await self.storage.get_message(body.get('channel'))
            if message:
                return {'message': message.message, 'message_id': message._id}
            else:
                return 'No messages in channel'

        if scope.get('path') == '/confirm':
            await self.storage.confirm_message(body.get('channel'),
                                               body.get('message_id'))
            return 'Message confirmed!'

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
