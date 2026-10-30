"""Ephemeral Python SDK host; credentials never enter the bounded JSON protocol."""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import uuid

from claude_agent_sdk import (
    ClaudeAgentOptions, ClaudeSDKClient, PermissionResultAllow, PermissionResultDeny,
    ResultMessage, SdkMcpTool, StreamEvent, SystemMessage, create_sdk_mcp_server,
)
from jsonschema import Draft202012Validator

MAX_LINE = 2 * 1024 * 1024
MESSAGES = {
    "subscription": "Claude requires a subscription login; API billing is disabled.",
    "model": "Claude changed the selected model.",
    "tools": "Claude enabled an unexpected tool.",
    "provider": "Claude request failed. Check account access and usage limits.",
    "incomplete": "Claude did not complete the reply.",
    "structured": "Claude did not return the required structured decision.",
    "protocol": "Claude runtime protocol failed.",
}


class WorkerError(RuntimeError):
    """Carry an allowlisted error code, never provider output or credentials."""


def send(value):
    """Publish one finite bounded JSON event on the parent-owned pipe."""
    line = json.dumps(value, allow_nan=False, separators=(",", ":")) + "\n"
    if len(line.encode()) > MAX_LINE:
        raise WorkerError("protocol")
    sys.stdout.write(line)
    sys.stdout.flush()


def decode(line):
    """Reject malformed/nonfinite messages before dispatching any work."""
    def invalid_constant(_value):
        """JSON constants such as NaN are not part of the protocol."""
        raise WorkerError("protocol")

    if not line or len(line) > MAX_LINE:
        raise WorkerError("protocol")
    value = json.loads(line, parse_constant=invalid_constant)
    if not isinstance(value, dict):
        raise WorkerError("protocol")
    json.dumps(value, allow_nan=False)
    return value


def count(value):
    """Accept genuine nonnegative integer telemetry, excluding booleans."""
    return type(value) is int and value >= 0


async def main(input_value, pending):
    """Use the SDK's native metadata, permission, MCP and structured-output APIs."""
    mode = input_value.get("mode")
    if mode not in {"models", "context", "chat"}:
        raise WorkerError("protocol")
    structured = "output_schema" in input_value
    validator = None
    if structured:
        output_schema = input_value["output_schema"]
        if not isinstance(output_schema, dict) or output_schema.get("type") != "object":
            raise WorkerError("structured")
        Draft202012Validator.check_schema(output_schema)
        validator = Draft202012Validator(output_schema)
    specs = input_value.get("tools") or []
    if not isinstance(specs, list) or len(specs) > 64:
        raise WorkerError("tools")
    names = set()
    tools = []
    for spec in specs:
        name = spec.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", name) or name in names:
            raise WorkerError("tools")
        names.add(name)
        schema = spec.get("input_schema") or {"type": "object", "properties": {}}
        Draft202012Validator.check_schema(schema)
        schema = {**schema, "properties": schema.get("properties", {})}

        def handler_for(tool_name, tool_schema):
            """Bind each advertised tool to its own immutable name and validator."""
            tool_validator = Draft202012Validator(tool_schema)

            async def handle(arguments):
                """Round-trip only validated capability arguments to the PBGui parent."""
                if not tool_validator.is_valid(arguments):
                    raise WorkerError("tools")
                identifier = str(uuid.uuid4())
                result = asyncio.get_running_loop().create_future()
                pending[identifier] = result
                try:
                    send({"type": "tool", "id": identifier, "name": tool_name, "arguments": arguments})
                    output = await result
                    return {"content": [{"type": "text", "text": json.dumps(output, allow_nan=False)}]}
                finally:
                    pending.pop(identifier, None)

            return handle

        tools.append(SdkMcpTool(name=name, description=spec.get("description") or "",
                                input_schema=schema, handler=handler_for(name, schema)))

    async def can_use_tool(name, arguments, _context):
        """Permit only the provided PBGui capabilities and the SDK output contract."""
        if (structured and name == "StructuredOutput"
                or name.startswith("mcp__pbgui__") and name[12:] in names):
            return PermissionResultAllow(updated_input=arguments)
        return PermissionResultDeny(message="Only the provided PBGui capabilities are available.")

    env = dict(os.environ)
    output_tokens = input_value.get("output_tokens")
    if type(output_tokens) is int and 0 < output_tokens <= 32768:
        env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = str(output_tokens)
    options = ClaudeAgentOptions(
        cli_path=input_value["executable"], cwd=os.getcwd(), env=env, tools=[],
        setting_sources=[], plugins=[], agents={}, strict_mcp_config=True,
        settings=json.dumps({"disableAllHooks": True, "autoMemoryEnabled": False, "fallbackModel": []}),
        extra_args={"no-session-persistence": None}, include_partial_messages=True,
        system_prompt=input_value.get("instructions") or "Reply to the user.",
        permission_mode="default", can_use_tool=can_use_tool, verbatim_prompts=True,
        mcp_servers={"pbgui": create_sdk_mcp_server(name="pbgui", version="1.0.0", tools=tools)} if tools else {},
        model=input_value.get("model") or None, effort=input_value.get("effort") or None,
        output_format={"type": "json_schema", "schema": input_value["output_schema"]} if structured else None,
    )
    async with ClaudeSDKClient(options=options) as client:
        info = await client.get_server_info() or {}
        account = info.get("account") or {}
        if (account.get("apiProvider") not in (None, "", "firstParty")
                or account.get("apiKeySource") not in (None, "", "none")):
            raise WorkerError("subscription")
        if mode == "models":
            models = info.get("models")
            if not isinstance(models, list):
                raise WorkerError("model")
            send({"type": "result", "result": models})
            return
        if mode == "context":
            context = await client.get_context_usage()
            if context.get("model") != input_value.get("model"):
                raise WorkerError("model")
            windows = [context.get(key) for key in ("maxTokens", "rawMaxTokens")
                       if type(context.get(key)) is int and context[key] > 0]
            if not windows:
                raise WorkerError("model")
            send({"type": "result", "result": {"model": context["model"], "context": min(windows)}})
            return
        await client.query("Conversation history (quoted user/assistant content):\n" +
                           json.dumps(input_value.get("history") or [], allow_nan=False))
        reply = None
        async for event in client.receive_response():
            if isinstance(event, SystemMessage):
                if event.subtype == "init":
                    if event.data.get("model") != input_value.get("model"):
                        raise WorkerError("model")
                    enabled = event.data.get("tools") or []
                    if any(name != "EndConversation" and not (structured and name == "StructuredOutput")
                           and not (name.startswith("mcp__pbgui__") and name[12:] in names) for name in enabled):
                        raise WorkerError("tools")
                if input_value.get("observe_runtime") and event.subtype == "compact_boundary":
                    send({"type": "runtime", "event": "compaction"})
            if isinstance(event, StreamEvent) and not structured:
                stream = event.event
                delta = stream.get("delta") or {}
                if stream.get("type") == "content_block_delta" and delta.get("type") == "text_delta":
                    send({"type": "text", "text": delta["text"]})
            if isinstance(event, ResultMessage):
                if event.subtype == "error_max_structured_output_retries":
                    raise WorkerError("structured")
                if event.is_error or event.subtype != "success":
                    raise WorkerError("provider")
                usage = event.model_usage or {}
                if any(name != input_value.get("model") and row.get("canonicalModel") != input_value.get("model")
                       for name, row in usage.items()):
                    raise WorkerError("model")
                if input_value.get("observe_runtime"):
                    rows = list(usage.values())
                    known = rows and all(all(count(row.get(key)) for key in (
                        "inputTokens", "cacheReadInputTokens", "cacheCreationInputTokens", "outputTokens")) for row in rows)
                    telemetry = {"type": "runtime", "event": "usage"}
                    if known:
                        telemetry.update(input_tokens=sum(row[key] for row in rows for key in (
                            "inputTokens", "cacheReadInputTokens", "cacheCreationInputTokens")),
                            output_tokens=sum(row["outputTokens"] for row in rows))
                    windows = [row["contextWindow"] for row in rows
                               if type(row.get("contextWindow")) is int and row["contextWindow"] > 0]
                    if windows:
                        telemetry["context_window"] = min(windows)
                    send(telemetry)
                if structured:
                    if not isinstance(event.structured_output, dict) or not validator.is_valid(event.structured_output):
                        raise WorkerError("structured")
                    reply = json.dumps(event.structured_output, allow_nan=False, separators=(",", ":"))
                else:
                    reply = event.result
        if not isinstance(reply, str) or not reply:
            raise WorkerError("incomplete")
        send({"type": "result", "result": reply})


async def run():
    """Own the input reader and pending calls for exactly one isolated SDK request."""
    reader = asyncio.StreamReader(limit=MAX_LINE)
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer)
    pending = {}
    input_task = None
    operation = None

    async def receive():
        """Accept only replies for currently owned tool calls; EOF cancels the request."""
        try:
            while line := await reader.readline():
                message = decode(line)
                future = pending.get(message.get("id"))
                if message.get("type") != "tool_result" or future is None or future.done():
                    raise WorkerError("protocol")
                future.set_result(message.get("result"))
            operation.cancel()
        except asyncio.CancelledError:
            raise
        except Exception:
            send({"type": "error", "message": MESSAGES["protocol"]})
            operation.cancel()

    try:
        value = decode(await reader.readline())
        operation = asyncio.create_task(main(value, pending), name="claude-sdk-request")
        input_task = asyncio.create_task(receive(), name="claude-parent-input")
        await operation
        return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc, WorkerError) else ""
        send({"type": "error", "message": MESSAGES.get(code, "Claude runtime failed. Check dependencies and subscription sign-in.")})
        return 1
    finally:
        for future in pending.values():
            future.cancel()
        if input_task:
            input_task.cancel()
            await asyncio.gather(input_task, return_exceptions=True)
        transport.close()


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(run()))
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.exit(1)
