import { Connection, connect as connectWebSocket } from "@service-broker/websocket";
import assert from "assert";
import * as rxjs from "rxjs";
import { PassThrough, Readable } from "stream";
import WebSocket from "ws";
import { deserialize, serialize } from "./serialize.js";


export interface Message {
  header?: Record<string, unknown>
  payload?: string | Buffer | Readable;
}

export interface MessageWithHeader extends Message {
  header: Record<string, unknown>
}

export type ServiceBroker = ReturnType<typeof makeClient>

interface ProvidedService {
  name: string
  capabilities?: string[]
  priority?: number
}

interface Provider {
  service: ProvidedService
  handler(req: MessageWithHeader): Message | void | Promise<Message | void>
  advertise: boolean
}

const reservedFields: Record<string, void> = {
  from: undefined,
  to: undefined,
  id: undefined,
  type: undefined,
  error: undefined,
  service: undefined,
  part: undefined
};

function* makeIdGen(): Generator<string, never> {
  let i = 0
  while (true) yield String(++i)
}

function assertRecord(value: object): asserts value is Record<string, unknown> {
}



export interface ConnectOptions {
  authToken?: string
  keepAlive?: {
    pingInterval: number
    pongTimeout: number
  },
  streamingChunkSize?: number
}

type ErrorEvent = {
  type: 'send-error'
  message?: MessageWithHeader
  error: unknown
} | {
  type: 'receive-error'
  message: WebSocket.Data
  error: unknown
} | {
  type: 'keep-alive-error'
  error: unknown
} | {
  type: 'socket-error'
  error: unknown
}

export function connect(url: string, opts: ConnectOptions = {}) {
  const providers = new Map<string, Provider>()
  const waitEndpoints = new Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>()
  return connectWebSocket(url).pipe(
    rxjs.map(con => makeClient(con, providers, waitEndpoints, opts))
  )
}

function makeClient(
  con: Connection,
  providers: Map<string, Provider>,
  waitEndpoints: Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>,
  { authToken, keepAlive, streamingChunkSize }: ConnectOptions
) {
  const transmit = rxjs.bindNodeCallback(con.send).bind(con)
  const sendSubject = new rxjs.Subject<{ msg: MessageWithHeader, subscriber: rxjs.Subscriber<void> }>()
  const pendingIdGen = makeIdGen()
  const pendingResponses = new Map<string, rxjs.Subscriber<MessageWithHeader>>()

  return {
    event$: rxjs.merge<ErrorEvent[]>(
      //WARNING: this must come before the others!
      sendSubject.pipe(
        rxjs.concatMap(({ msg, subscriber }) =>
          rxjs.defer(() => {
            const data = serialize(msg, streamingChunkSize)
            if (rxjs.isObservable(data)) {
              return data.pipe(
                rxjs.concatMap(chunk => transmit(chunk, {})),
                rxjs.ignoreElements(),
                rxjs.endWith(undefined)
              )
            } else {
              return transmit(data, {})
            }
          }).pipe(
            rxjs.tap(subscriber),
            rxjs.ignoreElements(),
            rxjs.catchError(err =>
              rxjs.of<ErrorEvent>({
                type: 'send-error',
                message: msg,
                error: err
              })
            )
          )
        )
      ),
      //readvertise services on reconnect
      rxjs.defer(() => {
        const services = Array.from(providers.values())
          .filter(x => x.advertise)
          .map(x => x.service)
        return rxjs.iif(
          () => services.length > 0,
          sendAdvertisement(services).pipe(
            rxjs.ignoreElements(),
            rxjs.catchError(err =>
              rxjs.of<ErrorEvent>({
                type: 'send-error',
                message: { header: { type: 'SbAdvertiseRequest', services } },
                error: err
              })
            )
          ),
          rxjs.EMPTY
        )
      }),
      //resend endpointWaitRequests on reconnect
      rxjs.defer(() =>
        rxjs.from(waitEndpoints.keys()).pipe(
          rxjs.concatMap(endpointId =>
            send({
              header: {
                type: "SbEndpointWaitRequest",
                endpointId
              }
            })
          ),
          rxjs.ignoreElements(),
          rxjs.catchError(err =>
            rxjs.of<ErrorEvent>({
              type: 'send-error',
              message: { header: { type: 'SbEndpointWaitRequest' } },
              error: err
            })
          )
        )
      ),
      con.message$.pipe(
        rxjs.mergeMap(event =>
          processMessage(event.data).pipe(
            rxjs.ignoreElements(),
            rxjs.catchError(err =>
              rxjs.of<ErrorEvent>({
                type: 'receive-error',
                message: event.data,
                error: err
              })
            )
          )
        )
      ),
      con.error$.pipe(
        rxjs.map(event => ({
          type: 'socket-error',
          error: event.error
        }))
      ),
      !keepAlive
        ? rxjs.EMPTY
        : con.keepAlive(keepAlive.pingInterval, keepAlive.pongTimeout).pipe(
          rxjs.ignoreElements(),
          rxjs.catchError(err => {
            con.terminate()
            return rxjs.of<ErrorEvent>({
              type: 'keep-alive-error',
              error: err
            })
          })
        )
    ),

    close$: con.close$,
    close: con.close.bind(con),
    debug: {
      con
    },

    advertise(
      service: { name: string, capabilities?: string[], priority?: number },
      handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>
    ) {
      assert(!providers.has(service.name), `${service.name} provider already exists`)
      return sendAdvertisement(
        Array.from(providers.values())
          .filter(x => x.advertise)
          .map(x => x.service)
          .concat(service)
      ).pipe(
        rxjs.tap(() => {
          providers.set(service.name, {
            service,
            handler,
            advertise: true
          })
        })
      )
    },

    unadvertise(serviceName: string) {
      assert(providers.has(serviceName), `${serviceName} provider not exists`)
      return sendAdvertisement(
        Array.from(providers.values())
          .filter(x => x.advertise && x.service.name != serviceName)
          .map(x => x.service)
      ).pipe(
        rxjs.tap(() => {
          providers.delete(serviceName)
        })
      )
    },

    setServiceHandler(
      serviceName: string,
      handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>
    ) {
      assert(!providers.has(serviceName), `${serviceName} provider already exists`)
      providers.set(serviceName, {
        service: { name: serviceName },
        handler,
        advertise: false
      })
    },

    request(
      service: { name: string, capabilities?: string[] },
      req: Message,
      timeout?: number
    ) {
      return sendReq({
        header: {
          ...req.header,
          ...reservedFields,
          type: "ServiceRequest",
          service
        },
        payload: req.payload
      }, timeout)
    },

    notify(
      service: { name: string, capabilities?: string[] },
      msg: Message
    ) {
      return send({
        header: {
          ...msg.header,
          ...reservedFields,
          type: "ServiceRequest",
          service
        },
        payload: msg.payload
      })
    },

    requestTo(
      endpointId: string,
      serviceName: string,
      req: Message,
      timeout?: number
    ) {
      return sendReq({
        header: {
          ...req.header,
          ...reservedFields,
          to: endpointId,
          type: "ServiceRequest",
          service: { name: serviceName }
        },
        payload: req.payload
      }, timeout)
    },

    notifyTo(
      endpointId: string,
      serviceName: string,
      msg: Message
    ) {
      return send({
        header: {
          ...msg.header,
          ...reservedFields,
          to: endpointId,
          type: "ServiceRequest",
          service: { name: serviceName }
        },
        payload: msg.payload
      })
    },

    publish(topic: string, text: string) {
      return send({
        header: {
          type: "ServiceRequest",
          service: { name: "#" + topic }
        },
        payload: text
      })
    },

    subscribe(topic: string, handler: (text: string) => void) {
      return this.advertise(
        { name: "#" + topic },
        msg => handler(msg.payload as string)
      )
    },

    unsubscribe(topic: string) {
      return this.unadvertise("#" + topic)
    },

    status() {
      return sendReq({
        header: { type: "SbStatusRequest" }
      }).pipe(
        rxjs.map(res => JSON.parse(res.payload as string))
      )
    },

    cleanup() {
      return send({
        header: {
          type: "SbCleanupRequest"
        }
      })
    },

    waitEndpoint(endpointId: string) {
      let waiter = waitEndpoints.get(endpointId)
      if (!waiter) {
        const closeSubject = new rxjs.ReplaySubject<void>()
        waitEndpoints.set(endpointId, waiter = {
          closeSubject,
          close$: send({
            header: {
              type: "SbEndpointWaitRequest",
              endpointId
            }
          }).pipe(
            rxjs.exhaustMap(() => closeSubject),
            rxjs.finalize(() => waitEndpoints.delete(endpointId))
          )
        })
      }
      return waiter.close$
    }
  }



  function sendAdvertisement(services: ProvidedService[]) {
    return sendReq({
      header: {
        authToken,
        type: "SbAdvertiseRequest"
      },
      payload: JSON.stringify(services)
    })
  }

  function processMessage(data: unknown): rxjs.Observable<void> {
    const msg = deserialize(data)
    if (msg.header.type == "ServiceRequest") return onServiceRequest(msg)
    else if (msg.header.type == "ServiceResponse") return onServiceResponse(msg)
    else if (msg.header.type == "SbAdvertiseResponse") return onServiceResponse(msg)
    else if (msg.header.type == "SbStatusResponse") return onServiceResponse(msg)
    else if (msg.header.type == "SbEndpointWaitResponse") return onEndpointWaitResponse(msg)
    else if (msg.header.error) return onServiceResponse(msg)
    else if (msg.header.service) return onServiceRequest(msg)
    else throw new Error("Don't know what to do with message")
  }

  function onServiceRequest(req: MessageWithHeader): rxjs.Observable<void> {
    return rxjs.defer(() => {
      assert(typeof req.header.service == 'object' && req.header.service != null, 'BAD_REQUEST')
      assertRecord(req.header.service)
      assert(typeof req.header.service.name == 'string', 'BAD_REQUEST')
      const provider = providers.get(req.header.service.name)
      assert(provider, "NO_PROVIDER " + req.header.service.name)
      return Promise.resolve(provider.handler(req))
    }).pipe(
      rxjs.map(res => res ?? {}),
      rxjs.exhaustMap(res =>
        rxjs.iif(
          () => req.header.id != null,
          send({
            header: {
              ...res.header,
              ...reservedFields,
              to: req.header.from,
              id: req.header.id,
              type: "ServiceResponse"
            },
            payload: res.payload
          }),
          rxjs.EMPTY
        )
      ),
      rxjs.catchError(err =>
        rxjs.iif(
          () => req.header.id != null,
          send({
            header: {
              to: req.header.from,
              id: req.header.id,
              type: "ServiceResponse",
              error: err instanceof Error ? err.message : String(err)
            }
          }),
          rxjs.throwError(() =>
            new Error('Potentially silent error thrown by notification handler', { cause: err })
          )
        )
      )
    )
  }

  function onServiceResponse(msg: MessageWithHeader): rxjs.Observable<void> {
    const pending = pendingResponses.get(msg.header.id as string)
    if (pending) {
      pending.next(msg)
      return rxjs.EMPTY
    } else {
      throw new Error("Response received but no pending request")
    }
  }

  function onEndpointWaitResponse(msg: MessageWithHeader): rxjs.Observable<void> {
    const waiter = waitEndpoints.get(msg.header.endpointId as string)
    if (waiter) {
      waiter.closeSubject.next()
      waiter.closeSubject.complete()
      return rxjs.EMPTY
    } else {
      throw new Error("Stray EndpointWaitResponse")
    }
  }

  function send(msg: MessageWithHeader): rxjs.Observable<void> {
    return new rxjs.Observable(subscriber => {
      sendSubject.next({ msg, subscriber })
    })
  }

  function sendReq(msg: MessageWithHeader, timeout = 30000): rxjs.Observable<MessageWithHeader> {
    return rxjs.defer(() => {
      const id = pendingIdGen.next().value
      msg.header.id = id
      return send(msg).pipe(
        rxjs.exhaustMap(() =>
          new rxjs.Observable<MessageWithHeader>(subscriber => {
            pendingResponses.set(id, subscriber)
            return () => pendingResponses.delete(id)
          })
        ),
        rxjs.share(),
        src$ => src$.pipe(
          rxjs.first(),
          timeout == Infinity ? rxjs.identity : rxjs.timeout(timeout),
          rxjs.exhaustMap(res =>
            rxjs.iif(
              () => !!res.header.part,
              rxjs.defer(() => {
                const stream = new PassThrough()
                const streamWrite = rxjs.bindNodeCallback(stream.write).bind(stream)
                return src$.pipe(
                  rxjs.startWith(res),
                  rxjs.takeWhile(msg => !!msg.header.part, true),
                  rxjs.concatMap(msg => msg.payload ? streamWrite(msg.payload, 'utf8') : rxjs.EMPTY),
                  rxjs.finalize(() => stream.end()),
                  rxjs.ignoreElements(),
                  rxjs.startWith({ header: res.header, payload: stream })
                )
              }),
              rxjs.iif(
                () => !!res.header.error,
                rxjs.throwError(() => typeof res.header.error == 'string' ? new Error(res.header.error) : res.header.error),
                rxjs.of(res)
              )
            )
          ),
          rxjs.share({ resetOnRefCountZero: false })
        )
      )
    })
  }
}
