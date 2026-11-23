"""Own disposable HTTP read streams so restart does not wait for their keepalives."""

from __future__ import annotations

import asyncio


class ReadStreamOwner:
    """Track only GET event streams; writes and independent jobs are never registered."""

    def __init__(self):
        """Create one API-owned registry without starting work."""
        self.tasks: dict[asyncio.Task, asyncio.Task] = {}
        self.cancelled_tasks: set[asyncio.Task] = set()
        self.closing = False
        self.loop = None
        self.requests: dict[asyncio.Task, dict] = {}

    def start(self):
        """Reopen stream admission for a new API lifespan."""
        self.closing = False
        self.loop = asyncio.get_running_loop()

    def pending_operations(self):
        """Report static endpoint names only; never URLs, headers, bodies or query data."""
        counts = {}
        for scope in self.requests.values():
            endpoint = scope.get('endpoint')
            name = (getattr(endpoint, '__module__', '') + '.' +
                    getattr(endpoint, '__qualname__', 'unrouted')).strip('.')
            counts[name] = counts.get(name, 0) + 1
        return counts

    def cancel(self, task):
        """Cancel only the owned application child, never its caller's request task."""
        if task not in self.cancelled_tasks and not task.done():
            self.cancelled_tasks.add(task)
            task.cancel()

    async def close(self):
        """End owned read connections and await their subscription cleanup."""
        self.closing = True
        interrupted = False
        while True:
            requests = [(child, request) for child, request in self.tasks.items()
                        if child is not asyncio.current_task() and request is not asyncio.current_task()]
            if not requests:
                break
            for child, request in requests:
                self.cancel(child)
            try:
                # Cancelling the closer must never cascade into caller requests.
                await asyncio.wait({request for _, request in requests})
            except asyncio.CancelledError:
                interrupted = True
        if interrupted:
            raise asyncio.CancelledError



class ReadStreamMiddleware:
    """Release GET SSE connections before Uvicorn starts its graceful drain."""

    def __init__(self, app, owner):
        """Bind the response observer to the shared owner."""
        self.app = app
        self.owner = owner

    async def __call__(self, scope, receive, send):
        """Distinguish owned stream shutdown from external request cancellation."""
        if scope['type'] != 'http' or scope.get('method') != 'GET':
            await self.app(scope, receive, send)
            return
        if self.owner.closing:
            # Stop polling/reconnect traffic from replenishing the graceful drain.
            await send({'type': 'http.response.start', 'status': 503,
                        'headers': [(b'retry-after', b'1'), (b'content-length', b'0')]})
            await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
            return
        request = asyncio.current_task()
        self.owner.requests[request] = scope
        streaming = False
        started = False
        finished = False

        async def stream_send(message):
            """Register streams only after the actual response headers were accepted."""
            nonlocal streaming, started, finished
            if message['type'] == 'http.response.start':
                streaming = any(key.lower() == b'content-type' and
                                value.split(b';', 1)[0].strip().lower() == b'text/event-stream'
                                for key, value in message.get('headers', []))
            await send(message)
            if message['type'] == 'http.response.start':
                started = True
                if streaming:
                    self.owner.tasks[child] = request
            elif message['type'] == 'http.response.body' and not message.get('more_body', False):
                finished = True
            if streaming and self.owner.closing and not finished:
                self.owner.cancel(child)
                await asyncio.sleep(0)

        child = asyncio.create_task(self.app(scope, receive, stream_send), name='http-get-response')
        try:
            await asyncio.shield(child)
        except asyncio.CancelledError:
            # Cancelling a child raises here without cancelling this request. An
            # external timeout/disconnect cancels the request itself and must propagate.
            if child not in self.owner.cancelled_tasks or request.cancelling():
                raise
            if started and not finished:
                await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
        finally:
            interrupted = False
            if not child.done():
                child.cancel()
                completion = asyncio.gather(child, return_exceptions=True)
                while not completion.done():
                    try:
                        await asyncio.shield(completion)
                    except asyncio.CancelledError:
                        interrupted = True
            self.owner.cancelled_tasks.discard(child)
            self.owner.tasks.pop(child, None)
            self.owner.requests.pop(request, None)
            if interrupted:
                raise asyncio.CancelledError
