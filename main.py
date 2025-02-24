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

    async def register_channel(self, channel_name: str) -> None:
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

    async def get_stats(self, channel_name: Optional[str]) -> dict[str, int]:
        stats_data = {}
        if channel_name is None:
            for channel in self.channels.values():
                stats_data[channel.name] = {
                    "ready_messages": len(channel.ready_messages),
                    "unacked_messages": len(channel.unacked_messages),
                    "total": len(channel.ready_messages) + len(channel.unacked_messages)
                }
        elif channel_name and channel_name in self.channels:
            channel = self.channels[channel_name]
            stats_data[channel_name] = {
                "ready_messages": len(channel.ready_messages),
                "unacked_messages": len(channel.unacked_messages),
                "total": len(channel.ready_messages) + len(channel.unacked_messages)
            }
        return stats_data


class Response(BaseModel):
    data: dict[str, Any] = {}
    message: str = ""
    error: str = ""
    message_id: str = ""


class Broker:
    def __init__(self):
        self.storage = Storage()

    async def __call__(self, scope, receive, send) -> None:
        assert scope['type'] == 'http'

        result = await self.route(scope, receive)

        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'Content-Type', b'application/json'],
            ],
        })
        await send({
            'type': 'http.response.body',
            'body': json.dumps(result.model_dump()).encode('utf-8'),
        })

    async def route(self, scope, receive):
        request = await receive()
        body = json.loads(request.get('body', b''))

        handlers = {
            '/register': self.register,
            '/send': self.send,
            '/read': self.read,
            '/confirm': self.confirm,
            '/purge': self.purge,
            '/stats': self.stats
        }

        path = scope.get('path')
        if path in handlers:
            return await handlers[path](body)
        else:
            return Response(error='Unknown path')

    async def register(self, body):
        try:
            await self.storage.register_channel(body.get('channel'))
            return Response(message=f'Channel {body.get("channel")} successfully registred')
        except ValueError as err:
            return Response(error=str(err))

    async def send(self, body):
        message = Message.model_validate(body)
        try:
            await self.storage.put_message(body.get('channel'), message)
            return Response(data=message.data, message_id=message._id)
        except ValueError as err:
            return Response(error=str(err))

    async def read(self, body):
        message = await self.storage.get_message(body.get('channel'))
        if message:
            return Response(data=message.data, message_id=message._id)
        else:
            return Response(message='No messages in channel')

    async def confirm(self, body):
        result = await self.storage.confirm_message(
            body.get('channel'), body.get('message_id')
        )
        if result:
            return Response(message='Message confirmed!')
        else:
            return Response(message='No such message')

    async def purge(self, body):
        await self.storage.purge_channel_messages(body.get('channel'))
        return Response(message='Channel purged')

    async def stats(self, body):
        stats = await self.storage.get_stats(body.get('channel'))
        return Response(data=stats)

app = Broker()
