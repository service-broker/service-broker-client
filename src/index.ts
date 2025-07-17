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

export type ServiceBroker = ReturnType<typeof makeClient>

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



export interface ConnectOptions {
  keepAlive?: {
    pingInterval: number
    pongTimeout: number
  },
  streamingChunkSize?: number
}

export type ServiceEvent = {
  type: 'service-request'
  request: MessageWithHeader
  responseSubject: rxjs.Subject<Message | void>
}

export type ErrorEvent = {
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
  const waitEndpoints = new Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>()
  return connectWebSocket(url).pipe(
    rxjs.map(con => makeClient(con, waitEndpoints, opts))
  )
}

function makeClient(
  con: Connection,
  waitEndpoints: Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>,
  opts: ConnectOptions
) {
  const transmit = rxjs.bindNodeCallback(con.send).bind(con)
  const sendSubject = new rxjs.Subject<{ msg: MessageWithHeader, subscriber: rxjs.Subscriber<void> }>()
  const pendingIdGen = makeIdGen()
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
            type: 'send-error',
            message: msg,
            error: err
          })
        )
      )
    ),
    rxjs.share()
  )

  const doOnce$ = rxjs.merge(
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
    )
  ).pipe(
    rxjs.share()
  )

  const receive$ = con.message$.pipe(
    rxjs.mergeMap(event =>
      processMessage(event.data).pipe(
        rxjs.catchError(err =>
          rxjs.of<ErrorEvent>({
            type: 'receive-error',
            message: event.data,
            error: err
          })
        )
      )
    ),
    rxjs.share()
  )

  const keepAlive$ = rxjs.defer(() => {
    if (opts.keepAlive) {
      return con.keepAlive(opts.keepAlive.pingInterval, opts.keepAlive.pongTimeout).pipe(
        rxjs.ignoreElements(),
        rxjs.catchError(err => {
          con.terminate()
          return rxjs.of<ErrorEvent>({
            type: 'keep-alive-error',
            error: err
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
    doOnce$,
    keepAlive$,
  ).pipe(
    rxjs.takeUntil(con.close$)
  ).subscribe()

  //return the API
  return {
    _debug: {
      con
    },

    request$: receive$.pipe(
      rxjs.filter(event => event?.type == 'service-request')
    ),

    error$: rxjs.merge(
      send$,
      receive$.pipe(
        rxjs.filter(event => event?.type == 'receive-error')
      ),
      doOnce$,
      keepAlive$,
      con.error$.pipe(
        rxjs.map(event => ({
          type: 'socket-error',
          error: event.error
        }) as ErrorEvent)
      )
    ),

    close$: con.close$,

    close: con.close.bind(con),

    advertise({ services, topics, authToken }: {
      services: { name: string, capabilities?: string[], priority?: number }[],
      topics: { name: string, capabilities?: string[] }[],
      authToken?: string
    }) {
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

    request(
      service: { name: string, capabilities?: string[] },
      req: Message,
      timeout?: number
    ) {
      return sendReq({
        header: {
          ...req.header,
          ...reservedFields,
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
          service: { name: serviceName }
        },
        payload: msg.payload
      })
    },

    publish(topic: string, text: string) {
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



  function processMessage(data: unknown): rxjs.Observable<void | ServiceEvent> {
    const msg = deserialize(data)
    if (msg.header.type == "SbEndpointWaitResponse") return onEndpointWaitResponse(msg)
    else if (msg.header.service) return onServiceRequest(msg)
    else return onServiceResponse(msg)
  }

  function onServiceRequest(req: MessageWithHeader): rxjs.Observable<ServiceEvent> {
    const responseSubject = new rxjs.Subject<Message | void>()
    return rxjs.merge(
      rxjs.of({ type: 'service-request', request: req, responseSubject } as ServiceEvent),
      responseSubject.pipe(
        rxjs.first(null, undefined),
        rxjs.timeout(60*1000),
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
              new Error('Potentially silent error thrown by notification handler', { cause: err })
            )
          )
        ),
        rxjs.ignoreElements()
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
