import { connect } from "@service-broker/websocket";
import assert from "assert";
import EventEmitter from "events";
import * as rxjs from "rxjs";
import { PassThrough, Transform } from "stream";
;
const reservedFields = {
    from: undefined,
    to: undefined,
    id: undefined,
    type: undefined,
    error: undefined,
    service: undefined,
    part: undefined
};
function assertRecord(obj) {
}
export class ServiceBroker extends EventEmitter {
    opts;
    connection$;
    providers;
    pending;
    pendingIdGen;
    shutdown$;
    constructor(opts) {
        super();
        this.opts = opts;
        this.providers = new Map();
        this.pending = new Map();
        this.pendingIdGen = 0;
        this.shutdown$ = new rxjs.ReplaySubject(1);
        this.connection$ = connect(opts.url, opts.websocketOptions).pipe(rxjs.tap({
            next: () => this.emit('connect'),
            error: err => this.emit('error', new Error('Fail to connect to service broker', { cause: err }))
        }), !opts.retryConfig ? rxjs.identity : rxjs.retry(opts.retryConfig), rxjs.exhaustMap(conn => {
            if (this.providers.size) {
                conn.send(JSON.stringify({
                    authToken: this.opts.authToken,
                    type: "SbAdvertiseRequest",
                }) + '\n' +
                    JSON.stringify(Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)));
            }
            for (const endpointId of this.waitPromises.keys()) {
                conn.send(JSON.stringify({
                    type: 'SbEndpointWaitRequest',
                    endpointId
                }));
            }
            return rxjs.merge(conn.message$.pipe(rxjs.tap(event => {
                try {
                    this.onMessage(event.data);
                }
                catch (err) {
                    if (err instanceof Error)
                        err.cause = event.data;
                    this.emit('error', new Error('Fail to handle message', { cause: err }));
                }
            })), conn.error$.pipe(rxjs.tap(event => this.emit('error', new Error('Connection error', { cause: event.error })))), !opts.keepAlive ? rxjs.EMPTY : conn.keepAlive(opts.keepAlive.pingInterval, opts.keepAlive.pongTimeout).pipe(rxjs.catchError(err => {
                this.emit('error', new Error('Fail to keep alive', { cause: err }));
                conn.terminate();
                return rxjs.EMPTY;
            }))).pipe(rxjs.takeUntil(conn.close$.pipe(rxjs.tap(event => {
                this.emit('close', event.code, event.reason);
            }))), rxjs.finalize(() => conn.close()), rxjs.ignoreElements(), rxjs.startWith(conn), rxjs.endWith(null));
        }), !opts.repeatConfig ? rxjs.identity : rxjs.repeat(opts.repeatConfig), rxjs.takeUntil(this.shutdown$), rxjs.shareReplay(1));
    }
    onMessage(data) {
        let msg;
        if (typeof data == "string")
            msg = this.messageFromString(data);
        else if (Buffer.isBuffer(data))
            msg = this.messageFromBuffer(data);
        else
            throw new Error("Message is not a string or Buffer");
        if (msg.header.type == "SbEndpointWaitResponse")
            this.onEndpointWaitResponse(msg);
        else if (msg.header.service)
            this.onServiceRequest(msg);
        else
            this.onServiceResponse(msg);
    }
    async onServiceRequest(msg) {
        try {
            assert(typeof msg.header.service == 'object' && msg.header.service != null, 'BAD_ARGS service');
            assertRecord(msg.header.service);
            assert(typeof msg.header.service.name == 'string', 'BAD_ARGS service.name');
            const provider = this.providers.get(msg.header.service.name);
            assert(provider, 'NO_SERVICE ' + msg.header.service.name);
            const res = await provider.handler(msg) || {};
            if (msg.header.id) {
                const header = {
                    to: msg.header.from,
                    id: msg.header.id,
                    type: "ServiceResponse"
                };
                await this.send(Object.assign({}, res.header, reservedFields, header), res.payload);
            }
        }
        catch (err) {
            if (msg.header.id) {
                await this.send({
                    to: msg.header.from,
                    id: msg.header.id,
                    type: "ServiceResponse",
                    error: err instanceof Error ? err.message : err
                });
            }
            else {
                this.emit('error', new Error('Unhandled error thrown by notification handler', { cause: err }));
            }
        }
    }
    onServiceResponse(msg) {
        assert(typeof msg.header.id == 'string', 'BAD_ARGS id');
        const pending = this.pending.get(msg.header.id);
        assert(pending, 'Stray serviceResponse');
        pending.next(msg);
    }
    onEndpointWaitResponse(msg) {
        assert(typeof msg.header.endpointId == 'string', 'BAD_ARGS endpointId');
        const waiter = this.waitPromises.get(msg.header.endpointId);
        assert(waiter, 'Stray endpointWaitResponse');
        waiter.next(msg.header.error);
        waiter.complete();
    }
    messageFromString(str) {
        assert(str[0] == "{", "Message doesn't have JSON header");
        const index = str.indexOf("\n");
        const headerStr = (index != -1) ? str.slice(0, index) : str;
        const payload = (index != -1) ? str.slice(index + 1) : undefined;
        return {
            header: JSON.parse(headerStr),
            payload
        };
    }
    messageFromBuffer(buf) {
        assert(buf[0] == 123, "Message doesn't have JSON header");
        const index = buf.indexOf("\n");
        const headerStr = (index != -1) ? buf.slice(0, index).toString() : buf.toString();
        const payload = (index != -1) ? buf.slice(index + 1) : undefined;
        return {
            header: JSON.parse(headerStr),
            payload
        };
    }
    async send(header, payload) {
        const ws = await rxjs.firstValueFrom(this.connection$);
        assert(ws, "No connection");
        const headerStr = JSON.stringify(header);
        if (payload) {
            if (typeof payload == "string") {
                ws.send(headerStr + "\n" + payload);
            }
            else if (Buffer.isBuffer(payload)) {
                const headerLen = Buffer.byteLength(headerStr);
                const tmp = Buffer.allocUnsafe(headerLen + 1 + payload.length);
                tmp.write(headerStr);
                tmp[headerLen] = 10;
                payload.copy(tmp, headerLen + 1);
                ws.send(tmp);
            }
            else if (payload.pipe) {
                const stream = this.packetizer(this.opts.streamingChunkSize || 64_000);
                stream.on("data", data => this.send(Object.assign({}, header, { part: true }), data));
                stream.on("end", () => this.send(header));
                payload.pipe(stream);
            }
            else
                throw new Error("Unexpected");
        }
        else
            ws.send(headerStr);
    }
    packetizer(size) {
        let buf;
        let pos;
        return new Transform({
            transform: function (chunk, encoding, callback) {
                while (chunk.length) {
                    if (!buf) {
                        buf = Buffer.alloc(size);
                        pos = 0;
                    }
                    const count = chunk.copy(buf, pos);
                    pos += count;
                    if (pos >= buf.length) {
                        this.push(buf);
                        buf = null;
                    }
                    chunk = chunk.slice(count);
                }
                callback();
            },
            flush: function (callback) {
                if (buf) {
                    this.push(buf.slice(0, pos));
                    buf = null;
                }
                callback();
            }
        });
    }
    async advertise(service, handler) {
        assert(!this.providers.has(service.name), `${service.name} provider already exists`);
        this.providers.set(service.name, {
            service,
            handler,
            advertise: true
        });
        const id = String(++this.pendingIdGen);
        if (process.env.OLD_ADVERTISE) {
            await this.send({
                id,
                authToken: this.opts.authToken,
                type: "SbAdvertiseRequest",
                services: Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)
            });
        }
        else {
            await this.send({
                id,
                authToken: this.opts.authToken,
                type: "SbAdvertiseRequest",
            }, JSON.stringify(Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)));
        }
        return await this.pendingResponse(id);
    }
    async unadvertise(serviceName) {
        assert(this.providers.delete(serviceName), `${serviceName} provider not exists`);
        const id = String(++this.pendingIdGen);
        await this.send({
            id,
            authToken: this.opts.authToken,
            type: "SbAdvertiseRequest",
        }, JSON.stringify(Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)));
        return await this.pendingResponse(id);
    }
    setServiceHandler(serviceName, handler) {
        assert(!this.providers.has(serviceName), `${serviceName} provider already exists`);
        this.providers.set(serviceName, {
            service: { name: serviceName },
            handler,
            advertise: false
        });
    }
    async request(service, req, timeout) {
        const id = String(++this.pendingIdGen);
        const header = {
            id,
            type: "ServiceRequest",
            service
        };
        await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
        return await this.pendingResponse(id, timeout);
    }
    async notify(service, msg) {
        const header = {
            type: "ServiceRequest",
            service
        };
        await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
    }
    async requestTo(endpointId, serviceName, req, timeout) {
        const id = String(++this.pendingIdGen);
        const header = {
            to: endpointId,
            id,
            type: "ServiceRequest",
            service: { name: serviceName }
        };
        await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
        return await this.pendingResponse(id, timeout);
    }
    async notifyTo(endpointId, serviceName, msg) {
        const header = {
            to: endpointId,
            type: "ServiceRequest",
            service: { name: serviceName }
        };
        await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
    }
    async pendingResponse(id, timeout = 30_000) {
        const subject = new rxjs.Subject();
        this.pending.set(id, subject);
        try {
            return await rxjs.firstValueFrom(subject.pipe(rxjs.first(), timeout == 0 || timeout == Infinity ? rxjs.identity : rxjs.timeout(timeout), rxjs.exhaustMap(first => {
                if (first.header.error)
                    throw first.header.error;
                if (!first.header.part)
                    return rxjs.of(first);
                const stream = new PassThrough();
                return subject.pipe(rxjs.timeout(30_000), rxjs.takeWhile(res => !!res.header.part, true), rxjs.startWith(first), rxjs.concatMap(res => rxjs.iif(() => res.payload != undefined, new rxjs.Observable(subscriber => {
                    stream.write(res.payload, err => err ? subscriber.error(err) : subscriber.complete());
                }), rxjs.EMPTY)), rxjs.finalize(() => stream.end()), rxjs.startWith({ header: first.header, payload: stream }));
            }), rxjs.finalize(() => this.pending.delete(id))));
        }
        catch (err) {
            throw typeof err == 'string' ? new Error(err) : err;
        }
    }
    async publish(topic, text) {
        await this.send({
            type: "ServiceRequest",
            service: { name: "#" + topic }
        }, text);
    }
    async subscribe(topic, handler) {
        await this.advertise({ name: "#" + topic }, (msg) => handler(msg.payload));
    }
    async unsubscribe(topic) {
        await this.unadvertise("#" + topic);
    }
    async status() {
        const id = String(++this.pendingIdGen);
        await this.send({
            id,
            type: "SbStatusRequest"
        });
        const res = await this.pendingResponse(id);
        return JSON.parse(res.payload);
    }
    async cleanup() {
        await this.send({
            type: "SbCleanupRequest"
        });
    }
    waitPromises = new Map();
    async waitEndpoint(endpointId) {
        let waiter = this.waitPromises.get(endpointId);
        if (!waiter) {
            await this.send({
                type: "SbEndpointWaitRequest",
                endpointId
            });
            this.waitPromises.set(endpointId, waiter = new rxjs.ReplaySubject());
            waiter.subscribe().add(() => this.waitPromises.delete(endpointId));
        }
        return await rxjs.firstValueFrom(waiter);
    }
    shutdown() {
        this.shutdown$.next();
    }
    async debugGetConnection() {
        return await rxjs.firstValueFrom(this.connection$);
    }
}
//# sourceMappingURL=index.js.map