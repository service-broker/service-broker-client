import { describe, expect, Expectation, objectHaving, oneOf, valueOfType } from "@service-broker/test-utils";
import assert from "assert";
import dotenv from "dotenv";
import * as rxjs from "rxjs";
import { PassThrough, Readable } from "stream";
import { connect } from "./index.js";
dotenv.config();
assert(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL");
const serviceBrokerUrl = process.env.SERVICE_BROKER_URL;
assert(process.env.AUTH_TOKEN, "Missing env AUTH_TOKEN");
const authToken = process.env.AUTH_TOKEN;
const localIp = oneOf(['::1', '127.0.0.1']);
function lvf(v$) {
    return rxjs.lastValueFrom(v$);
}
function sbConnect(url, queue, opts) {
    return connect(url, opts).pipe(rxjs.exhaustMap(sb => rxjs.concat(rxjs.of(sb), sb.event$.pipe(rxjs.tap(event => queue.push('ErrorEvent', event)), rxjs.ignoreElements())).pipe(rxjs.takeUntil(sb.close$), rxjs.finalize(() => sb.close()))), rxjs.repeat({ delay: 100 })).subscribe({
        next: sb => queue.push('ServiceBroker', sb),
        error: err => queue.push('Error', err)
    });
}
function makeQueue() {
    const queue = [];
    const waiters = [];
    return {
        push(type, value) {
            queue.push({ type, value });
            while (waiters.length && queue.length)
                waiters.shift()(queue.shift());
        },
        take(type) {
            assert(queue.length > 0, 'queueEmpty');
            const item = queue.shift();
            expect(item.type, type);
            return item.value;
        },
        async wait(type) {
            assert(queue.length == 0, '!queueEmpty');
            const item = await new Promise(f => waiters.push(f));
            expect(item.type, type);
            return item.value;
        }
    };
}
describe('config', ({ beforeEach, afterEach, test }) => {
    let subs;
    let queue;
    beforeEach(() => {
        subs = [];
        queue = makeQueue();
    });
    afterEach(() => {
        for (const sub of subs)
            sub.unsubscribe();
    });
    test('connect-fail', async () => {
        subs.push(sbConnect('ws://localhost:17038', queue));
        expect(await queue.wait('Error'), objectHaving({ code: 'ECONNREFUSED' }));
    });
    test('auth-token-fail', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const sb = await queue.wait('ServiceBroker');
        await lvf(sb.advertise({ name: 'echo' }, () => { }))
            .then(() => Promise.reject(new Error('!throwAsExpected')), err => expect(err.message, 'FORBIDDEN'));
    });
    test('reconnect-readvertise', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue, { authToken }));
        let sb = await queue.wait('ServiceBroker');
        await lvf(sb.advertise({ name: 'hello' }, req => ({ payload: 'Hello, ' + req.payload }))),
            sb.debug.con.close();
        sb = await queue.wait('ServiceBroker');
        expect(await lvf(sb.request({ name: 'hello' }, { payload: 'John' })), { header: valueOfType('object'), payload: 'Hello, John' });
    });
    test('wait-endpoint', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const client = await queue.wait('ServiceBroker');
        subs.push(sbConnect(serviceBrokerUrl, queue, { authToken }));
        const provider = await queue.wait('ServiceBroker');
        await lvf(provider.advertise({ name: 'hello' }, req => queue.push('Request', req)));
        await lvf(client.request({ name: 'hello' }, { payload: 'text' }));
        const req = queue.take('Request');
        expect(req.payload, 'text');
        const clientEndpointId = req.header.from;
        provider.waitEndpoint(clientEndpointId).subscribe(() => queue.push('disconnect', 0));
        await new Promise(f => setTimeout(f, 100));
        client.debug.con.close();
        await queue.wait('disconnect');
    });
    test('reconnect-wait-endpoint', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const client = await queue.wait('ServiceBroker');
        subs.push(sbConnect(serviceBrokerUrl, queue, { authToken, repeatConfig: {} }));
        let provider = await queue.wait('ServiceBroker');
        await lvf(provider.advertise({ name: 'hello' }, req => queue.push('Request', req)));
        await lvf(client.request({ name: 'hello' }, { payload: 'text' }));
        const req = queue.take('Request');
        expect(req.payload, 'text');
        const clientEndpointId = req.header.from;
        provider.waitEndpoint(clientEndpointId).subscribe(() => queue.push('disconnect', 0));
        await new Promise(f => setTimeout(f, 100));
        provider.debug.con.close();
        provider = await queue.wait('ServiceBroker');
        client.debug.con.close();
        await queue.wait('disconnect');
    });
    test('keep-alive', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue, { keepAlive: { pingInterval: 250, pongTimeout: 100 } }));
        const client = await queue.wait('ServiceBroker');
        expect(await queue.wait('ErrorEvent'), {
            type: 'keep-alive-error',
            error: new Expectation('instanceOf', 'TimeoutError', actual => assert(actual instanceof rxjs.TimeoutError))
        });
    });
    test('session-recovery', async () => {
        //TODO
    });
});
describe('pub-sub', ({ beforeEach, afterEach, test }) => {
    let subs;
    let queue;
    beforeEach(() => {
        subs = [];
        queue = makeQueue();
    });
    afterEach(() => {
        for (const sub of subs)
            sub.unsubscribe();
    });
    test("basic", async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const subscriber = await queue.wait('ServiceBroker');
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const publisher = await queue.wait('ServiceBroker');
        await lvf(subscriber.subscribe("test-log", msg => queue.push('Message', msg)));
        await lvf(publisher.publish("test-log", "what in the world"));
        expect(await queue.wait('Message'), "what in the world");
    });
});
describe("service", ({ beforeEach, afterEach, test }) => {
    let subs;
    let queue;
    beforeEach(() => {
        subs = [];
        queue = makeQueue();
    });
    afterEach(() => {
        for (const sub of subs)
            sub.unsubscribe();
    });
    test("request-response", async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const client = await queue.wait('ServiceBroker');
        subs.push(sbConnect(serviceBrokerUrl, queue, { authToken }));
        const provider = await queue.wait('ServiceBroker');
        //advertise
        await lvf(provider.advertise({ name: "test-tts", capabilities: ["v1", "v2"], priority: 1 }, req => new Promise(f => queue.push('Request', { req, reply: f }))));
        //request
        let promise = lvf(client.request({ name: "test-tts", capabilities: ["v1"] }, {
            header: { lang: "vi" },
            payload: "this is request payload"
        }));
        let { req, reply } = await queue.wait('Request');
        expect(req, {
            header: {
                from: valueOfType('string'),
                id: valueOfType('string'),
                ip: localIp,
                type: "ServiceRequest",
                service: {
                    name: "test-tts",
                    capabilities: ["v1"]
                },
                lang: "vi"
            },
            payload: "this is request payload"
        });
        //respond
        reply({
            header: { result: 1 },
            payload: Buffer.from("this is response payload")
        });
        let res = await promise;
        expect(res, {
            header: {
                from: valueOfType('string'),
                to: req.header.from,
                id: req.header.id,
                type: "ServiceResponse",
                result: 1
            },
            payload: Buffer.from("this is response payload")
        });
        const clientEndpointId = req.header.from;
        const providerEndpointId = res.header.from;
        //setServiceHandler
        provider.setServiceHandler("test-direct", req => new Promise(f => queue.push('Request', { req, reply: f })));
        //requestTo
        promise = lvf(client.requestTo(providerEndpointId, "test-direct", {
            header: { value: 100 },
            payload: "Direct request payload"
        }));
        ({ req, reply } = await queue.wait('Request'));
        expect(req, {
            header: {
                from: clientEndpointId,
                to: providerEndpointId,
                id: valueOfType('string'),
                type: "ServiceRequest",
                service: { name: "test-direct" },
                value: 100
            },
            payload: "Direct request payload"
        });
        //respond
        reply({
            header: { output: "crap" },
            payload: Buffer.from("Direct response payload")
        });
        expect(await promise, {
            header: {
                from: providerEndpointId,
                to: req.header.from,
                id: req.header.id,
                type: "ServiceResponse",
                output: "crap"
            },
            payload: Buffer.from("Direct response payload")
        });
        //notifyTo
        await lvf(client.notifyTo(providerEndpointId, "test-direct", {
            header: { value: 200 },
            payload: Buffer.from("Direct notify payload")
        }));
        ({ req, reply } = await queue.wait('Request'));
        expect(req, {
            header: {
                from: clientEndpointId,
                to: providerEndpointId,
                type: "ServiceRequest",
                service: { name: "test-direct" },
                value: 200
            },
            payload: Buffer.from("Direct notify payload")
        });
        reply({});
        //unadvertise
        await lvf(provider.unadvertise('test-tts'));
        //request fail no provider
        await lvf(client.request({ name: "test-tts", capabilities: ["v1"] }, {
            header: { lang: "en" },
            payload: "this is request payload"
        })).then(() => Promise.reject(new Error('!throwAsExpected')), err => expect(err.message, "NO_PROVIDER test-tts"));
    });
    test('streaming', async () => {
        subs.push(sbConnect(serviceBrokerUrl, queue));
        const client = await queue.wait('ServiceBroker');
        subs.push(sbConnect(serviceBrokerUrl, queue, { authToken, streamingChunkSize: 10 }));
        const provider = await queue.wait('ServiceBroker');
        await lvf(provider.advertise({ name: 'tts' }, msg => {
            assert(msg.payload == 'stream request');
            const stream = new PassThrough();
            const chunks = ['abcdefghijkl', 'mnop', 'qrstuvwxyz1234567890'];
            rxjs.from(chunks).pipe(rxjs.concatMap(chunk => rxjs.of(chunk).pipe(rxjs.concatWith(rxjs.timer(100).pipe(rxjs.ignoreElements()))))).subscribe({
                next: chunk => stream.write(Buffer.from(chunk)),
                complete: () => stream.end()
            });
            return { payload: stream };
        }));
        const res = await lvf(client.request({ name: 'tts' }, { payload: 'stream request' }));
        assert(res.payload instanceof Readable);
        expect(await lvf(rxjs.fromEvent(res.payload, 'data').pipe(rxjs.takeUntil(rxjs.fromEvent(res.payload, 'end')), rxjs.buffer(rxjs.NEVER))), [
            Buffer.from('abcdefghij'),
            Buffer.from('klmnopqrst'),
            Buffer.from('uvwxyz1234'),
            Buffer.from('567890')
        ]);
    });
});
//# sourceMappingURL=index.test.js.map