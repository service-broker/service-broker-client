import * as rxjs from "rxjs";
import WebSocket from "ws";
export function connect(address, options) {
    return rxjs.defer(() => {
        const ws = new WebSocket(address, options);
        return rxjs.race(rxjs.fromEvent(ws, 'error', (event) => event).pipe(rxjs.map(event => { throw event.error; })), rxjs.fromEvent(ws, 'open').pipe(rxjs.take(1), rxjs.map(() => makeConnection(ws))));
    });
}
function makeConnection(ws) {
    return {
        message$: rxjs.fromEvent(ws, 'message', (event) => event),
        error$: rxjs.fromEvent(ws, 'error', (event) => event),
        close$: rxjs.fromEvent(ws, 'close', (event) => event),
        send: ws.send.bind(ws),
        close: ws.close.bind(ws),
        keepAlive: (interval, timeout) => rxjs.interval(interval).pipe(rxjs.switchMap(() => {
            ws.ping();
            return rxjs.fromEventPattern(h => ws.on('pong', h), h => ws.off('pong', h)).pipe(rxjs.timeout(timeout), rxjs.take(1), rxjs.ignoreElements());
        }))
    };
}
