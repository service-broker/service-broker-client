import { Connection, connect as connectWebSocket } from "@service-broker/websocket";
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

const reservedFields: Record<string, void> = {
  from: undefined,
  to: undefined,
  id: undefined,
  type: undefined,
  error: undefined,
  service: undefined,
  part: undefined
};



export interface ClientOptions {
  keepAlive?: {
    pingInterval: number
    pongTimeout: number
  },
  streamingChunkSize?: number
}

export interface Client {
  request$: rxjs.Observable<ServiceEvent>
  error$: rxjs.Observable<ErrorEvent>
  close$: rxjs.Observable<WebSocket.CloseEvent>
  close: WebSocket['close']
  advertise(opts: {
    services: { name: string, capabilities?: string[], priority?: number }[],
    topics: { name: string, capabilities?: string[] }[],
    authToken?: string
  }): rxjs.Observable<MessageWithHeader>
  request(
    service: { name: string, capabilities?: string[] },
    req: Message,
    timeout?: number
  ): rxjs.Observable<MessageWithHeader>
  notify(
    service: { name: string, capabilities?: string[] },
    msg: Message
  ): rxjs.Observable<void>
  requestTo(
    endpointId: string,
    serviceName: string,
    req: Message,
    timeout?: number
  ): rxjs.Observable<MessageWithHeader>
  notifyTo(
    endpointId: string,
    serviceName: string,
    msg: Message
  ): rxjs.Observable<void>
  publish(topic: string, text: string): rxjs.Observable<void>
  status(): rxjs.Observable<unknown>
  cleanup(): rxjs.Observable<void>
  waitEndpoint(endpointId: string): rxjs.Observable<void>
}

export interface ServiceEvent {
  type: 'service'
  request: MessageWithHeader
  responseSubject: rxjs.Subject<Message | void>
}

export interface ErrorEvent {
  type: 'error'
  error: unknown
  detail: unknown
}



export function connect(url: string, opts: ClientOptions = {}) {
  const waitEndpoints = new Map<string, rxjs.Subject<void>>()
  return connectWebSocket(url).pipe(
    rxjs.map(con => makeClient(con, waitEndpoints, opts))
  )
}

function makeClient(
  con: Connection,
  waitEndpoints: Map<string, rxjs.Subject<void>>,
  opts: ClientOptions
): Client {
  const transmit = rxjs.bindNodeCallback(con.send).bind(con)
  const sendSubject = new rxjs.Subject<{ msg: MessageWithHeader, subscriber: rxjs.Subscriber<void> }>()
  const pendingIdGen = (function*() { let i = 0; while (true) yield String(++i) })()
  const pendingResponses = new Map<string, rxjs.Subscriber<MessageWithHeader>>()

  //background activities
  const send$ = sendSubject.pipe(
    rxjs.concatMap(({ msg, subscriber }) =>
      rxjs.defer(() => {
        const data = serialize(msg, opts.streamingChunkSize)
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
            type: 'error',
            error: err,
            detail: { method: 'send', message: msg }
          })
        )
      )
    ),
    rxjs.share()
  )

  const doOnce$ = rxjs.merge(
    //resend endpointWaitRequests on reconnect
    rxjs.defer(() => rxjs.from(waitEndpoints.keys())).pipe(
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
          type: 'error',
          error: err,
          detail: { method: 'waitEndpoint' }
        })
      )
    )
  ).pipe(
    rxjs.share()
  )

  const receive$ = con.message$.pipe(
    rxjs.mergeMap(event =>
      processMessage(event.data).pipe(
        rxjs.catchError(err =>
          rxjs.of<ErrorEvent>({
            type: 'error',
            error: err,
            detail: { method: 'receive', data: event.data }
          })
        )
      )
    ),
    rxjs.share()
  )

  const error$ = con.error$.pipe(
    rxjs.map((event): ErrorEvent => ({
      type: 'error',
      error: event.error,
      detail: { method: 'socketEvent' }
    })),
    rxjs.share()
  )

  const keepAlive$ = rxjs.defer(() => {
    if (opts.keepAlive) {
      return con.keepAlive(opts.keepAlive.pingInterval, opts.keepAlive.pongTimeout).pipe(
        rxjs.ignoreElements(),
        rxjs.catchError(err => {
          con.terminate()
          return rxjs.of<ErrorEvent>({
            type: 'error',
            error: err,
            detail: { method: 'keepAlive' }
          })
        })
      )
    } else {
      return rxjs.EMPTY
    }
  }).pipe(
    rxjs.share()
  )

  rxjs.merge(
    send$,    //this must come first so that it's subscribed before any send() call
    receive$,
    error$,
    doOnce$,
    keepAlive$,
  ).pipe(
    rxjs.takeUntil(con.close$)
  ).subscribe()

  //return the API
  return {
    request$: receive$.pipe(
      rxjs.filter(event => event.type == 'service')
    ),

    error$: rxjs.merge(
      send$,
      receive$.pipe(
        rxjs.filter(event => event.type == 'error')
      ),
      error$,
      doOnce$,
      keepAlive$,
    ),

    close$: con.close$,

    close: con.close.bind(con),

    advertise({ services, topics, authToken }) {
      return sendReq({
        header: {
          type: "SbAdvertiseRequest",
          authToken
        },
        payload: JSON.stringify(
          services.concat(
            topics.map(({ name, capabilities }) => ({ name: '#' + name, capabilities }))
          )
        )
      })
    },

    request(service, req, timeout) {
      return sendReq({
        header: {
          ...req.header,
          ...reservedFields,
          service
        },
        payload: req.payload
      }, timeout)
    },

    notify(service, msg) {
      return send({
        header: {
          ...msg.header,
          ...reservedFields,
          service
        },
        payload: msg.payload
      })
    },

    requestTo(endpointId, serviceName, req, timeout) {
      return sendReq({
        header: {
          ...req.header,
          ...reservedFields,
          to: endpointId,
          service: { name: serviceName }
        },
        payload: req.payload
      }, timeout)
    },

    notifyTo(endpointId, serviceName, msg) {
      return send({
        header: {
          ...msg.header,
          ...reservedFields,
          to: endpointId,
          service: { name: serviceName }
        },
        payload: msg.payload
      })
    },

    publish(topic, text) {
      return send({
        header: {
          service: { name: "#" + topic }
        },
        payload: text
      })
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

    waitEndpoint(endpointId) {
      return rxjs.defer(() => {
        let waiter = waitEndpoints.get(endpointId)
        if (!waiter) {
          waitEndpoints.set(endpointId, waiter = new rxjs.ReplaySubject<void>())
          send({
            header: {
              type: "SbEndpointWaitRequest",
              endpointId
            }
          }).pipe(
            rxjs.exhaustMap(() => waiter!),
            rxjs.finalize(() => waitEndpoints.delete(endpointId))
          ).subscribe()
        }
        return waiter
      })
    }
  }



  function processMessage(data: unknown): rxjs.Observable<ServiceEvent> {
    const msg = deserialize(data)
    if (msg.header.type == "SbEndpointWaitResponse") return onEndpointWaitResponse(msg)
    else if (msg.header.service) return onServiceRequest(msg)
    else return onServiceResponse(msg)
  }

  function onServiceRequest(req: MessageWithHeader): rxjs.Observable<ServiceEvent> {
    const responseSubject = new rxjs.Subject<Message | void>()
    return rxjs.merge(
      //this must come first to ensure it is subscribed before the ServiceEvent is emitted
      responseSubject.pipe(
        rxjs.first(null, undefined),
        rxjs.timeout(60_000),
        rxjs.exhaustMap(res =>
          rxjs.iif(
            () => req.header.id != null,
            send({
              header: {
                ...res?.header,
                ...reservedFields,
                to: req.header.from,
                id: req.header.id,
                type: "ServiceResponse"
              },
              payload: res?.payload
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
              new Error('Unhandled error thrown by notification handler', { cause: err })
            )
          )
        ),
        rxjs.ignoreElements()
      ),
      rxjs.of<ServiceEvent>({ type: 'service', request: req, responseSubject })
    )
  }

  function onServiceResponse(msg: MessageWithHeader): rxjs.Observable<never> {
    const pending = pendingResponses.get(msg.header.id as string)
    if (pending) {
      pending.next(msg)
      return rxjs.EMPTY
    } else {
      throw new Error("Stray ServiceResponse")
    }
  }

  function onEndpointWaitResponse(msg: MessageWithHeader): rxjs.Observable<never> {
    const waiter = waitEndpoints.get(msg.header.endpointId as string)
    if (waiter) {
      waiter.next()
      waiter.complete()
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

  function sendReq(msg: MessageWithHeader, timeout = 30_000): rxjs.Observable<MessageWithHeader> {
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
                  rxjs.timeout(60_000),
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
