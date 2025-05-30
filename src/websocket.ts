import * as rxjs from "rxjs"
import WebSocket, { CloseEvent, ErrorEvent, MessageEvent } from "ws"

export interface Connection {
  message$: rxjs.Observable<MessageEvent>
  error$: rxjs.Observable<ErrorEvent>
  close$: rxjs.Observable<CloseEvent>
  send: WebSocket['send']
  close: WebSocket['close']
  keepAlive(interval: number, timeout: number): rxjs.Observable<never>
}

export function connect(url: string) {
  return rxjs.defer(() => {
    const ws = new WebSocket(url)
    return rxjs.race(
      rxjs.fromEvent(ws, 'error', (event: ErrorEvent) => event).pipe(
        rxjs.map(event => { throw event.error })
      ),
      rxjs.fromEvent(ws, 'open').pipe(
        rxjs.take(1),
        rxjs.map(() => makeConnection(ws))
      )
    )
  })
}

function makeConnection(ws: WebSocket): Connection {
  return {
    message$: rxjs.fromEvent(ws, 'message', (event: MessageEvent) => event),
    error$: rxjs.fromEvent(ws, 'error', (event: ErrorEvent) => event),
    close$: rxjs.fromEvent(ws, 'close', (event: CloseEvent) => event),
    send: ws.send.bind(ws),
    close: ws.close.bind(ws),
    keepAlive: (interval, timeout) => rxjs.interval(interval).pipe(
      rxjs.switchMap(() => {
        ws.ping()
        return rxjs.fromEventPattern(h => ws.on('pong', h), h => ws.off('pong', h)).pipe(
          rxjs.timeout(timeout),
          rxjs.take(1),
          rxjs.ignoreElements()
        )
      })
    )
  }
}
