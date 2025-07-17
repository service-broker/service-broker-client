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
  authToken?: string
  keepAlive?: {
    pingInterval: number
    pongTimeout: number
  },
  streamingChunkSize?: number
  handle?: (request: MessageWithHeader) => void | Message | rxjs.Observable<void | Message>
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
  const waitEndpoints = new Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>()
  return connectWebSocket(url).pipe(
    rxjs.map(con => makeClient(con, waitEndpoints, opts))
  )
}

function makeClient(
  con: Connection,
  waitEndpoints: Map<string, { closeSubject: rxjs.Subject<void>, close$: rxjs.Observable<void> }>,
  { authToken, keepAlive, streamingChunkSize, handle }: ConnectOptions
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

    advertise({ services, topics }: {
      services: { name: string, capabilities?: string[], priority?: number }[],
      topics: { name: string, capabilities?: string[] }[],
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



  function processMessage(data: unknown): rxjs.Observable<void> {
    const msg = deserialize(data)
    if (msg.header.type == "SbEndpointWaitResponse") return onEndpointWaitResponse(msg)
    else if (msg.header.service) return onServiceRequest(msg)
    else return onServiceResponse(msg)
  }

  function onServiceRequest(req: MessageWithHeader): rxjs.Observable<void> {
    return rxjs.defer(() => {
      if (handle) {
        const result = handle(req)
        return rxjs.isObservable(result) ? result.pipe(rxjs.first(null, undefined)) : rxjs.of(result)
      } else {
        return rxjs.throwError(() => 'No service')
      }
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
