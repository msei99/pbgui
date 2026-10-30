// Ephemeral SDK host. Native account credentials never enter the JSON protocol.
import readline from 'node:readline';
import { randomUUID } from 'node:crypto';
import { query, createSdkMcpServer, tool } from '@anthropic-ai/claude-agent-sdk';
import { z } from 'zod';

const send = value => process.stdout.write(JSON.stringify(value) + '\n');
const lines = readline.createInterface({ input: process.stdin });
const pending = new Map();
let started = false;
let active;
let releasePrompt;

function schema(value = {}) {
    if (value.enum?.length) return z.union(value.enum.map(item => z.literal(item)));
    if (value.anyOf || value.oneOf) return z.union((value.anyOf || value.oneOf).map(schema));
    if (Array.isArray(value.type)) return z.union(value.type.map(type => schema({ ...value, type })));
    if (value.type === 'string') return z.string();
    if (value.type === 'integer') return z.number().int();
    if (value.type === 'number') return z.number();
    if (value.type === 'boolean') return z.boolean();
    if (value.type === 'null') return z.null();
    if (value.type === 'array') return z.array(schema(value.items));
    if (value.type === 'object' || value.properties) {
        const fields = Object.fromEntries(Object.entries(value.properties || {}).map(([name, spec]) =>
            [name, (value.required || []).includes(name) ? schema(spec) : schema(spec).optional()]));
        return value.additionalProperties === false ? z.object(fields).strict() : z.object(fields).passthrough();
    }
    return z.unknown();
}

async function main(input) {
    const structured = input.output_schema !== undefined;
    if (structured && (!input.output_schema || input.output_schema.type !== 'object')) throw new Error('structured');
    const outputValidator = structured ? schema(input.output_schema) : null;
    const specs = input.tools || [];
    const names = new Set(specs.map(item => item.name));
    const tools = specs.map(spec => tool(spec.name, spec.description, schema(spec.input_schema).shape, async args => {
        const id = randomUUID();
        const answer = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
        send({ type: 'tool', id, name: spec.name, arguments: args });
        const result = await answer;
        return { content: [{ type: 'text', text: JSON.stringify(result) }] };
    }));
    const server = tools.length ? createSdkMcpServer({ name: 'pbgui', version: '1.0.0', tools }) : null;
    const options = {
        pathToClaudeCodeExecutable: input.executable,
        cwd: process.cwd(), env: { ...process.env,
            ...(Number.isInteger(input.output_tokens) && input.output_tokens > 0 && input.output_tokens <= 32768
                ? { CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(input.output_tokens) } : {}) }, tools: [],
        settingSources: [], plugins: [], agents: {}, strictMcpConfig: true,
        settings: { disableAllHooks: true, autoMemoryEnabled: false, fallbackModel: [] },
        persistSession: false, includePartialMessages: true,
        systemPrompt: input.instructions || 'Reply to the user.',
        permissionMode: 'default',
        canUseTool: async (name, args) => (structured && name === 'StructuredOutput') || (name.startsWith('mcp__pbgui__') && names.has(name.slice(12)))
            ? { behavior: 'allow', updatedInput: args }
            : { behavior: 'deny', message: 'Only the provided PBGui capabilities are available.' },
        mcpServers: server ? { pbgui: server } : {},
        ...(input.model ? { model: input.model } : {}),
        ...(input.effort ? { effort: input.effort } : {}),
        ...(structured ? { outputFormat: { type: 'json_schema', schema: input.output_schema } } : {}),
    };
    // Hold input until metadata is obtained; models mode never submits a prompt.
    async function* prompt() {
        await new Promise(resolve => { releasePrompt = resolve; });
        if (input.mode === 'chat') yield { type: 'user', message: { role: 'user', content:
            'Conversation history (quoted user/assistant content):\n' + JSON.stringify(input.history || []) }, parent_tool_use_id: null, session_id: '' };
    }
    active = query({ prompt: prompt(), options });
    const account = await active.accountInfo();
    if (account.apiProvider && account.apiProvider !== 'firstParty' || account.apiKeySource && account.apiKeySource !== 'none')
        throw new Error('subscription');
    if (input.mode === 'models') {
        send({ type: 'result', result: await active.supportedModels() });
        releasePrompt();
        return;
    }
    if (input.mode === 'context') {
        const context = await active.getContextUsage();
        if (context.model !== input.model) throw new Error('model');
        const windows = [context.maxTokens, context.rawMaxTokens].filter(value => Number.isSafeInteger(value) && value > 0);
        if (!windows.length) throw new Error('context');
        send({ type: 'result', result: { model: context.model, context: Math.min(...windows) } });
        releasePrompt();
        return;
    }
    releasePrompt();
    let reply = '';
    let complete = false;
    for await (const event of active) {
        if (event.type === 'system' && event.subtype === 'init') {
            if (event.model !== input.model) throw new Error('model');
            if ((event.tools || []).some(name => name !== 'EndConversation' && !(structured && name === 'StructuredOutput') && !(name.startsWith('mcp__pbgui__') && names.has(name.slice(12)))))
                throw new Error('tools');
        }
        if (!structured && event.type === 'stream_event' && event.event?.type === 'content_block_delta' && event.event.delta?.type === 'text_delta')
            send({ type: 'text', text: event.event.delta.text });
        if (input.observe_runtime && event.type === 'system' && event.subtype === 'compact_boundary')
            send({ type: 'runtime', event: 'compaction' });
        if (event.type === 'result') {
            if (event.subtype === 'error_max_structured_output_retries') throw new Error('structured');
            if (event.is_error || event.subtype !== 'success') throw new Error('provider');
            if (Object.entries(event.modelUsage || {}).some(([name, usage]) => name !== input.model && usage.canonicalModel !== input.model))
                throw new Error('model');
            if (input.observe_runtime) {
                const usage = Object.values(event.modelUsage || {});
                const count = key => usage.reduce((total, row) => total + (Number.isSafeInteger(row[key]) && row[key] >= 0 ? row[key] : 0), 0);
                const windows = usage.map(row => row.contextWindow).filter(value => Number.isSafeInteger(value) && value > 0);
                const known = usage.length > 0 && usage.every(row => ['inputTokens', 'cacheReadInputTokens', 'cacheCreationInputTokens', 'outputTokens']
                    .every(key => Number.isSafeInteger(row[key]) && row[key] >= 0));
                send({ type: 'runtime', event: 'usage', ...(known ? {
                    input_tokens: count('inputTokens') + count('cacheReadInputTokens') + count('cacheCreationInputTokens'),
                    output_tokens: count('outputTokens') } : {}),
                    ...(windows.length ? { context_window: Math.min(...windows) } : {}) });
            }
            if (structured) {
                if (!event.structured_output || typeof event.structured_output !== 'object' || Array.isArray(event.structured_output)
                    || !outputValidator.safeParse(event.structured_output).success)
                    throw new Error('structured');
                reply = JSON.stringify(event.structured_output, (_key, value) => {
                    if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('structured');
                    return value;
                });
            } else reply = event.result || '';
            complete = true;
        }
    }
    if (!complete || !reply) throw new Error('incomplete');
    send({ type: 'result', result: reply });
}

lines.on('line', line => {
    try {
        if (line.length > 2 * 1024 * 1024) throw new Error('protocol');
        const value = JSON.parse(line);
        if (!started) {
            started = true;
            main(value).catch(error => {
                const messages = { subscription: 'Claude requires a subscription login; API billing is disabled.', model: 'Claude changed the selected model.', tools: 'Claude enabled an unexpected tool.', provider: 'Claude request failed. Check account access and usage limits.', incomplete: 'Claude did not complete the reply.', structured: 'Claude did not return the required structured decision.' };
                send({ type: 'error', message: messages[error.message] || 'Claude runtime failed. Check dependencies and subscription sign-in.' });
                process.exitCode = 1;
            }).finally(async () => {
                releasePrompt?.();
                await active?.close();
                lines.close();
                process.stdin.destroy();
            });
        } else if (value.type === 'tool_result' && pending.has(value.id)) {
            pending.get(value.id).resolve(value.result);
            pending.delete(value.id);
        }
    } catch {
        send({ type: 'error', message: 'Claude runtime protocol failed.' });
        process.exitCode = 1;
        process.stdin.destroy();
    }
});
