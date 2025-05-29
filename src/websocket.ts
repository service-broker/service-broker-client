import * as rxjs from "rxjs"
import WebSocket, { CloseEvent, ErrorEvent, MessageEvent } from "ws"

export type Connection = Pick<WebSocket, 'send'>


export function connect(
  url: string,
  keepAlive?: { interval: number, timeout: number }
) {
  return rxjs.defer(() => {
    const ws = new WebSocket(url)
    return rxjs.race(
      rxjs.fromEvent(ws, 'error', (event: ErrorEvent) => event).pipe(
        rxjs.map(event => { throw event.error })
      ),
      rxjs.fromEvent(ws, 'open').pipe(
        rxjs.take(1)
      )
    ).pipe(
      rxjs.map(() => ({
        type: 'open' as const,
        connection: ws as Connection
      })),
      rxjs.mergeWith(
        rxjs.fromEvent(ws, 'message', (event: MessageEvent) => ({
          type: 'message' as const,
          data: event.data
        })),
        rxjs.fromEvent(ws, 'error', (event: ErrorEvent) => ({
          type: 'error' as const,
          error: event.error
        })),
        rxjs.fromEvent(ws, 'close', (event: CloseEvent) => ({
          type: 'close' as const,
          code: event.code,
          reason: event.reason
        })),
        keepAlive ? rxjs.interval(keepAlive.interval).pipe(
          rxjs.switchMap(() => {
            ws.ping()
            return rxjs.fromEventPattern(
              h => ws.on('pong', h),
              h => ws.off('pong', h)
            ).pipe(
              rxjs.take(1),
              rxjs.ignoreElements(),
              rxjs.timeout({
                each: keepAlive.timeout,
                with: () => rxjs.of({
                  type: 'close' as const,
                  code: 1006,
                  reason: 'Keep alive timeout'
                })
              })
            )
          })
        ) : rxjs.EMPTY
      ),
      rxjs.takeWhile(event => event.type !== 'close', true),
      rxjs.finalize(() => ws.close())
    )
  })
}
