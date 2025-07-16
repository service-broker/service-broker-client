import { connect as connectWebSocket } from "@service-broker/websocket";
import assert from "assert";
import * as rxjs from "rxjs";
import { PassThrough } from "stream";
import { deserialize, serialize } from "./serialize.js";
const reservedFields = {
    from: undefined,
    to: undefined,
    id: undefined,
    type: undefined,
    error: undefined,
    service: undefined,
    part: undefined
};
function* makeIdGen() {
    let i = 0;
    while (true)
        yield String(++i);
}
function assertRecord(value) {
}
export function connect(url, opts = {}) {
    const providers = new Map();
    const waitEndpoints = new Map();
    return connectWebSocket(url).pipe(rxjs.map(con => makeClient(con, providers, waitEndpoints, opts)));
}
function makeClient(con, providers, waitEndpoints, { authToken, keepAlive, streamingChunkSize }) {
    const transmit = rxjs.bindNodeCallback(con.send).bind(con);
    const sendSubject = new rxjs.Subject();
    const pendingIdGen = makeIdGen();
    const pendingResponses = new Map();
    return {
        event$: rxjs.merge(
        //WARNING: this must come before the others!
        sendSubject.pipe(rxjs.concatMap(({ msg, subscriber }) => rxjs.defer(() => {
            const data = serialize(msg, streamingChunkSize);
            if (rxjs.isObservable(data)) {
                return data.pipe(rxjs.concatMap(chunk => transmit(chunk, {})), rxjs.ignoreElements(), rxjs.endWith(undefined));
            }
            else {
                return transmit(data, {});
            }
        }).pipe(rxjs.tap(subscriber), rxjs.ignoreElements(), rxjs.catchError(err => rxjs.of({
            type: 'send-error',
            message: msg,
            error: err
        }))))), 
        //readvertise services on reconnect
        rxjs.defer(() => {
            const services = Array.from(providers.values())
                .filter(x => x.advertise)
                .map(x => x.service);
            return rxjs.iif(() => services.length > 0, sendAdvertisement(services).pipe(rxjs.ignoreElements(), rxjs.catchError(err => rxjs.of({
                type: 'send-error',
                message: { header: { type: 'SbAdvertiseRequest', services } },
                error: err
            }))), rxjs.EMPTY);
        }), 
        //resend endpointWaitRequests on reconnect
        rxjs.defer(() => rxjs.from(waitEndpoints.keys()).pipe(rxjs.concatMap(endpointId => send({
            header: {
                type: "SbEndpointWaitRequest",
                endpointId
            }
        })), rxjs.ignoreElements(), rxjs.catchError(err => rxjs.of({
            type: 'send-error',
            message: { header: { type: 'SbEndpointWaitRequest' } },
            error: err
        })))), con.message$.pipe(rxjs.mergeMap(event => processMessage(event.data).pipe(rxjs.ignoreElements(), rxjs.catchError(err => rxjs.of({
            type: 'receive-error',
            message: event.data,
            error: err
        }))))), con.error$.pipe(rxjs.map(event => ({
            type: 'socket-error',
            error: event.error
        }))), !keepAlive
            ? rxjs.EMPTY
            : con.keepAlive(keepAlive.pingInterval, keepAlive.pongTimeout).pipe(rxjs.ignoreElements(), rxjs.catchError(err => {
                con.terminate();
                return rxjs.of({
                    type: 'keep-alive-error',
                    error: err
                });
            }))),
        close$: con.close$,
        close: con.close.bind(con),
        debug: {
            con
        },
        advertise(service, handler) {
            assert(!providers.has(service.name), `${service.name} provider already exists`);
            return sendAdvertisement(Array.from(providers.values())
                .filter(x => x.advertise)
                .map(x => x.service)
                .concat(service)).pipe(rxjs.tap(() => {
                providers.set(service.name, {
                    service,
                    handler,
                    advertise: true
                });
            }));
        },
        unadvertise(serviceName) {
            assert(providers.has(serviceName), `${serviceName} provider not exists`);
            return sendAdvertisement(Array.from(providers.values())
                .filter(x => x.advertise && x.service.name != serviceName)
                .map(x => x.service)).pipe(rxjs.tap(() => {
                providers.delete(serviceName);
            }));
        },
        setServiceHandler(serviceName, handler) {
            assert(!providers.has(serviceName), `${serviceName} provider already exists`);
            providers.set(serviceName, {
                service: { name: serviceName },
                handler,
                advertise: false
            });
        },
        request(service, req, timeout) {
            return sendReq({
                header: {
                    ...req.header,
                    ...reservedFields,
                    type: "ServiceRequest",
                    service
                },
                payload: req.payload
            }, timeout);
        },
        notify(service, msg) {
            return send({
                header: {
                    ...msg.header,
                    ...reservedFields,
                    type: "ServiceRequest",
                    service
                },
                payload: msg.payload
            });
        },
        requestTo(endpointId, serviceName, req, timeout) {
            return sendReq({
                header: {
                    ...req.header,
                    ...reservedFields,
                    to: endpointId,
                    type: "ServiceRequest",
                    service: { name: serviceName }
                },
                payload: req.payload
            }, timeout);
        },
        notifyTo(endpointId, serviceName, msg) {
            return send({
                header: {
                    ...msg.header,
                    ...reservedFields,
                    to: endpointId,
                    type: "ServiceRequest",
                    service: { name: serviceName }
                },
                payload: msg.payload
            });
        },
        publish(topic, text) {
            return send({
                header: {
                    type: "ServiceRequest",
                    service: { name: "#" + topic }
                },
                payload: text
            });
        },
        subscribe(topic, handler) {
            return this.advertise({ name: "#" + topic }, msg => handler(msg.payload));
        },
        unsubscribe(topic) {
            return this.unadvertise("#" + topic);
        },
        status() {
            return sendReq({
                header: { type: "SbStatusRequest" }
            }).pipe(rxjs.map(res => JSON.parse(res.payload)));
        },
        cleanup() {
            return send({
                header: {
                    type: "SbCleanupRequest"
                }
            });
        },
        waitEndpoint(endpointId) {
            let waiter = waitEndpoints.get(endpointId);
            if (!waiter) {
                const closeSubject = new rxjs.ReplaySubject();
                waitEndpoints.set(endpointId, waiter = {
                    closeSubject,
                    close$: send({
                        header: {
                            type: "SbEndpointWaitRequest",
                            endpointId
                        }
                    }).pipe(rxjs.exhaustMap(() => closeSubject), rxjs.finalize(() => waitEndpoints.delete(endpointId)))
                });
            }
            return waiter.close$;
        }
    };
    function sendAdvertisement(services) {
        return sendReq({
            header: {
                authToken,
                type: "SbAdvertiseRequest"
            },
            payload: JSON.stringify(services)
        });
    }
    function processMessage(data) {
        const msg = deserialize(data);
        if (msg.header.type == "ServiceRequest")
            return onServiceRequest(msg);
        else if (msg.header.type == "ServiceResponse")
            return onServiceResponse(msg);
        else if (msg.header.type == "SbAdvertiseResponse")
            return onServiceResponse(msg);
        else if (msg.header.type == "SbStatusResponse")
            return onServiceResponse(msg);
        else if (msg.header.type == "SbEndpointWaitResponse")
            return onEndpointWaitResponse(msg);
        else if (msg.header.error)
            return onServiceResponse(msg);
        else if (msg.header.service)
            return onServiceRequest(msg);
        else
            throw new Error("Don't know what to do with message");
    }
    function onServiceRequest(req) {
        return rxjs.defer(() => {
            assert(typeof req.header.service == 'object' && req.header.service != null, 'BAD_REQUEST');
            assertRecord(req.header.service);
            assert(typeof req.header.service.name == 'string', 'BAD_REQUEST');
            const provider = providers.get(req.header.service.name);
            assert(provider, "NO_PROVIDER " + req.header.service.name);
            return Promise.resolve(provider.handler(req));
        }).pipe(rxjs.map(res => res ?? {}), rxjs.exhaustMap(res => rxjs.iif(() => req.header.id != null, send({
            header: {
                ...res.header,
                ...reservedFields,
                to: req.header.from,
                id: req.header.id,
                type: "ServiceResponse"
            },
            payload: res.payload
        }), rxjs.EMPTY)), rxjs.catchError(err => rxjs.iif(() => req.header.id != null, send({
            header: {
                to: req.header.from,
                id: req.header.id,
                type: "ServiceResponse",
                error: err instanceof Error ? err.message : String(err)
            }
        }), rxjs.throwError(() => new Error('Potentially silent error thrown by notification handler', { cause: err })))));
    }
    function onServiceResponse(msg) {
        const pending = pendingResponses.get(msg.header.id);
        if (pending) {
            pending.next(msg);
            return rxjs.EMPTY;
        }
        else {
            throw new Error("Response received but no pending request");
        }
    }
    function onEndpointWaitResponse(msg) {
        const waiter = waitEndpoints.get(msg.header.endpointId);
        if (waiter) {
            waiter.closeSubject.next();
            waiter.closeSubject.complete();
            return rxjs.EMPTY;
        }
        else {
            throw new Error("Stray EndpointWaitResponse");
        }
    }
    function send(msg) {
        return new rxjs.Observable(subscriber => {
            sendSubject.next({ msg, subscriber });
        });
    }
    function sendReq(msg, timeout = 30000) {
        return rxjs.defer(() => {
            const id = pendingIdGen.next().value;
            msg.header.id = id;
            return send(msg).pipe(rxjs.exhaustMap(() => new rxjs.Observable(subscriber => {
                pendingResponses.set(id, subscriber);
                return () => pendingResponses.delete(id);
            })), rxjs.share(), src$ => src$.pipe(rxjs.first(), timeout == Infinity ? rxjs.identity : rxjs.timeout(timeout), rxjs.exhaustMap(res => rxjs.iif(() => !!res.header.part, rxjs.defer(() => {
                const stream = new PassThrough();
                const streamWrite = rxjs.bindNodeCallback(stream.write).bind(stream);
                return src$.pipe(rxjs.startWith(res), rxjs.takeWhile(msg => !!msg.header.part, true), rxjs.concatMap(msg => msg.payload ? streamWrite(msg.payload, 'utf8') : rxjs.EMPTY), rxjs.finalize(() => stream.end()), rxjs.ignoreElements(), rxjs.startWith({ header: res.header, payload: stream }));
            }), rxjs.iif(() => !!res.header.error, rxjs.throwError(() => typeof res.header.error == 'string' ? new Error(res.header.error) : res.header.error), rxjs.of(res)))), rxjs.share({ resetOnRefCountZero: false })));
        });
    }
}
//# sourceMappingURL=index.js.map