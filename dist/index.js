"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ServiceBroker = void 0;
const assert_1 = __importDefault(require("assert"));
const rxjs = __importStar(require("rxjs"));
const stream_1 = require("stream");
const websocket_1 = require("./websocket");
const reservedFields = {
    from: undefined,
    to: undefined,
    id: undefined,
    type: undefined,
    error: undefined,
    service: undefined,
    part: undefined
};
function pTimeout(promise, millis) {
    if (millis == Infinity)
        return promise;
    let timer;
    return Promise.race([
        promise
            .finally(() => clearTimeout(timer)),
        new Promise(f => timer = setTimeout(f, millis))
            .then(() => Promise.reject(new Error("Timeout")))
    ]);
}
class ServiceBroker {
    constructor(opts) {
        this.opts = opts;
        this.waitPromises = new Map();
        this.providers = {};
        this.pending = {};
        this.pendingIdGen = 0;
        this.logger = opts.logger ?? console;
        this.shutdown$ = new rxjs.ReplaySubject(1);
        this.connection$ = (0, websocket_1.connect)(opts.url).pipe(rxjs.tap({
            error: err => {
                this.logger.info("Failed to connect to service broker,", String(err));
            }
        }), opts.disableReconnect ? rxjs.identity : rxjs.retry({ delay: 15000 }), rxjs.exhaustMap(conn => {
            this.logger.info("Service broker connection established");
            conn.send(JSON.stringify({
                authToken: this.opts.authToken,
                type: "SbAdvertiseRequest",
                services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
            }));
            this.opts.onConnect?.();
            return rxjs.merge(conn.message$.pipe(rxjs.tap(event => {
                try {
                    this.onMessage(event.data);
                }
                catch (err) {
                    this.logger.error(err);
                }
            })), conn.error$.pipe(rxjs.tap(event => this.logger.error(event.error)))).pipe(rxjs.ignoreElements(), rxjs.startWith(conn), rxjs.endWith(null), rxjs.takeUntil(rxjs.merge(conn.close$.pipe(rxjs.tap(event => this.logger.info("Service broker connection lost,", event.code, event.reason))), conn.keepAlive((opts.keepAliveIntervalSeconds ?? 30) * 1000, 3000).pipe(rxjs.catchError(err => {
                if (!(err instanceof rxjs.TimeoutError))
                    this.logger.error(err);
                this.logger.info("Service broker connection lost, keep-alive timeout");
                return rxjs.of(0);
            })))), rxjs.finalize(() => conn.close()));
        }), opts.disableReconnect ? rxjs.identity : rxjs.repeat({ delay: 1000 }), rxjs.takeUntil(this.shutdown$), rxjs.shareReplay(1));
    }
    onMessage(data) {
        let msg;
        try {
            if (typeof data == "string")
                msg = this.messageFromString(data);
            else if (Buffer.isBuffer(data))
                msg = this.messageFromBuffer(data);
            else
                throw new Error("Message is not a string or Buffer");
        }
        catch (err) {
            this.logger.error(String(err));
            return;
        }
        if (msg.header.type == "ServiceRequest")
            this.onServiceRequest(msg);
        else if (msg.header.type == "ServiceResponse")
            this.onServiceResponse(msg);
        else if (msg.header.type == "SbStatusResponse")
            this.onServiceResponse(msg);
        else if (msg.header.type == "SbEndpointWaitResponse")
            this.onServiceResponse(msg);
        else if (msg.header.error)
            this.onServiceResponse(msg);
        else if (msg.header.service)
            this.onServiceRequest(msg);
        else
            this.logger.error("Don't know what to do with message:", msg.header);
    }
    async onServiceRequest(msg) {
        try {
            if (this.providers[msg.header.service.name]) {
                const res = await this.providers[msg.header.service.name].handler(msg) || {};
                if (msg.header.id) {
                    const header = {
                        to: msg.header.from,
                        id: msg.header.id,
                        type: "ServiceResponse"
                    };
                    await this.send(Object.assign({}, res.header, reservedFields, header), res.payload);
                }
            }
            else
                throw new Error("No provider for service " + msg.header.service.name);
        }
        catch (err) {
            if (msg.header.id) {
                await this.send({
                    to: msg.header.from,
                    id: msg.header.id,
                    type: "ServiceResponse",
                    error: err instanceof Error ? err.message : String(err)
                });
            }
            else
                this.logger.error(String(err), msg.header);
        }
    }
    onServiceResponse(msg) {
        if (this.pending[msg.header.id])
            this.pending[msg.header.id].process(msg);
        else
            this.logger.error("Response received but no pending request:", msg.header);
    }
    messageFromString(str) {
        if (str[0] != "{")
            throw new Error("Message doesn't have JSON header");
        const index = str.indexOf("\n");
        const headerStr = (index != -1) ? str.slice(0, index) : str;
        const payload = (index != -1) ? str.slice(index + 1) : undefined;
        let header;
        try {
            header = JSON.parse(headerStr);
        }
        catch (err) {
            throw new Error("Failed to parse message header");
        }
        return { header, payload };
    }
    messageFromBuffer(buf) {
        if (buf[0] != 123)
            throw new Error("Message doesn't have JSON header");
        const index = buf.indexOf("\n");
        const headerStr = (index != -1) ? buf.slice(0, index).toString() : buf.toString();
        const payload = (index != -1) ? buf.slice(index + 1) : undefined;
        let header;
        try {
            header = JSON.parse(headerStr);
        }
        catch (err) {
            throw new Error("Failed to parse message header");
        }
        return { header, payload };
    }
    async send(header, payload) {
        const ws = await rxjs.firstValueFrom(this.connection$);
        if (!ws)
            throw new Error("No connection");
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
                const stream = this.packetizer(64 * 1000);
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
        return new stream_1.Transform({
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
        (0, assert_1.default)(service && service.name && handler, "Missing args");
        (0, assert_1.default)(!this.providers[service.name], `${service.name} provider already exists`);
        this.providers[service.name] = {
            service,
            handler,
            advertise: true
        };
        await this.send({
            authToken: this.opts.authToken,
            type: "SbAdvertiseRequest",
            services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
        });
    }
    async unadvertise(serviceName) {
        (0, assert_1.default)(serviceName, "Missing args");
        (0, assert_1.default)(this.providers[serviceName], `${serviceName} provider not exists`);
        delete this.providers[serviceName];
        await this.send({
            authToken: this.opts.authToken,
            type: "SbAdvertiseRequest",
            services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
        });
    }
    setServiceHandler(serviceName, handler) {
        (0, assert_1.default)(serviceName && handler, "Missing args");
        (0, assert_1.default)(!this.providers[serviceName], `${serviceName} provider already exists`);
        this.providers[serviceName] = {
            service: { name: serviceName },
            handler,
            advertise: false
        };
    }
    async request(service, req, timeout) {
        (0, assert_1.default)(service && service.name && req, "Missing args");
        const id = String(++this.pendingIdGen);
        const promise = this.pendingResponse(id, timeout);
        const header = {
            id,
            type: "ServiceRequest",
            service
        };
        await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
        return promise;
    }
    async notify(service, msg) {
        (0, assert_1.default)(service && service.name && msg, "Missing args");
        const header = {
            type: "ServiceRequest",
            service
        };
        await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
    }
    async requestTo(endpointId, serviceName, req, timeout) {
        (0, assert_1.default)(endpointId && serviceName && req, "Missing args");
        const id = String(++this.pendingIdGen);
        const promise = this.pendingResponse(id, timeout);
        const header = {
            to: endpointId,
            id,
            type: "ServiceRequest",
            service: { name: serviceName }
        };
        await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
        return promise;
    }
    async notifyTo(endpointId, serviceName, msg) {
        (0, assert_1.default)(endpointId && serviceName && msg, "Missing args");
        const header = {
            to: endpointId,
            type: "ServiceRequest",
            service: { name: serviceName }
        };
        await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
    }
    pendingResponse(id, timeout) {
        const promise = new Promise((fulfill, reject) => {
            let stream;
            this.pending[id] = {
                process: res => {
                    if (res.header.error)
                        reject(new Error(res.header.error));
                    else {
                        if (res.header.part) {
                            if (!stream)
                                fulfill({ header: res.header, payload: stream = new stream_1.PassThrough() });
                            stream.write(res.payload);
                        }
                        else {
                            delete this.pending[id];
                            if (stream)
                                stream.end(res.payload);
                            else
                                fulfill(res);
                        }
                    }
                }
            };
        });
        return pTimeout(promise, timeout || 30 * 1000)
            .catch(err => {
            delete this.pending[id];
            throw err;
        });
    }
    async publish(topic, text) {
        (0, assert_1.default)(topic && text, "Missing args");
        await this.send({
            type: "ServiceRequest",
            service: { name: "#" + topic }
        }, text);
    }
    async subscribe(topic, handler) {
        (0, assert_1.default)(topic && handler, "Missing args");
        await this.advertise({ name: "#" + topic }, (msg) => handler(msg.payload));
    }
    async unsubscribe(topic) {
        (0, assert_1.default)(topic, "Missing args");
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
    async wait(endpointId) {
        const id = String(++this.pendingIdGen);
        await this.send({
            id,
            type: "SbEndpointWaitRequest",
            endpointId
        });
        await this.pendingResponse(id, Infinity);
    }
    waitEndpoint(endpointId) {
        let promise = this.waitPromises.get(endpointId);
        if (!promise)
            this.waitPromises.set(endpointId, promise = this.wait(endpointId).finally(() => this.waitPromises.delete(endpointId)));
        return promise;
    }
    shutdown() {
        this.shutdown$.next();
    }
}
exports.ServiceBroker = ServiceBroker;
