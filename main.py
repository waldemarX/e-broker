import json
from uuid import uuid4
from typing import Any
from collections import OrderedDict

from pydantic import BaseModel, PrivateAttr


class Message(BaseModel):
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    data: dict[str, Any]


class Channel(BaseModel):
    name: str
    ready_messages: OrderedDict[str, Message] = OrderedDict()
    unacked_messages: OrderedDict[str, Message] = OrderedDict()


class Response(BaseModel):
    data: dict[str, Any] = {}
    message: str = ""
    error: str = ""
    message_id: str = ""


class Broker:
    def __init__(self):
        self.channels: dict[str, Channel] = {}

    async def __call__(self, scope, receive, send) -> None:
        try:
            result = await self.route(scope, receive)
        except Exception as err:
            result = Response(error=str(err))

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"application/json"],
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps(result.model_dump()).encode("utf-8"),
            }
        )

    async def route(self, scope, receive):
        request = await receive()

        handlers = {
            "/register": self.register,
            "/send": self.send,
            "/read": self.read,
            "/confirm": self.confirm,
            "/purge": self.purge,
            "/stats": self.stats,
        }

        path = scope.get("path")
        if path in handlers:
            body = json.loads(request.get("body", b""))
            return await handlers[path](body)
        else:
            return Response(error="Unknown path")

    async def register(self, body) -> Response:
        channel_name = body.get("channel")
        if channel_name in self.channels:
            return Response(error=f"Channel {channel_name} already exists")
        self.channels[channel_name] = Channel(name=channel_name)
        return Response(
            message=f"Channel {channel_name} successfully registred"
        )

    async def send(self, body) -> Response:
        message = Message.model_validate(body)
        channel_name = body.get("channel")
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages[message._id] = message
            return Response(data=message.data, message_id=message._id)
        else:
            return Response(error=f"Channel {channel_name} does not exist")

    async def read(self, body) -> Response:
        channel_name = body.get("channel")
        if channel_name not in self.channels:
            return Response(message=f"Channel {channel_name} does not exist")
        if messages := self.channels[channel_name].ready_messages:
            _, message = messages.popitem(last=False)
            self.channels[channel_name].unacked_messages[message._id] = message
            return Response(data=message.data, message_id=message._id)
        else:
            return Response(message="No messages in channel")

    async def confirm(self, body) -> Response:
        channel_name = body.get("channel")
        message_id = body.get("message_id")
        if channel_name not in self.channels:
            return Response(error=f"Channel {channel_name} does not exist")
        if self.channels[channel_name].unacked_messages.pop(message_id, False):
            return Response(
                message=f"Message confirmed!", message_id=message_id
            )
        else:
            return Response(
                error=f"Message does not exist", message_id=message_id
            )

    async def purge(self, body) -> Response:
        channel_name = body.get("channel")
        if channel_name in self.channels:
            self.channels[channel_name].ready_messages.clear()
            self.channels[channel_name].unacked_messages.clear()
            return Response(message=f"Channel {channel_name} purged")
        else:
            return Response(error=f"Channel {channel_name} does not exist")

    async def stats(self, body) -> Response:
        channel_name = body.get("channel")
        stats_data = {}
        if channel_name is None:
            for channel in self.channels.values():
                stats_data[channel.name] = {
                    "ready_messages": len(channel.ready_messages),
                    "unacked_messages": len(channel.unacked_messages),
                    "total": len(channel.ready_messages)
                    + len(channel.unacked_messages),
                }
        elif channel_name and channel_name in self.channels:
            channel = self.channels[channel_name]
            stats_data[channel_name] = {
                "ready_messages": len(channel.ready_messages),
                "unacked_messages": len(channel.unacked_messages),
                "total": len(channel.ready_messages)
                + len(channel.unacked_messages),
            }
        return Response(data=stats_data)


app = Broker()
