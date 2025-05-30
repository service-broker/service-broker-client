import { ClientRequestArgs } from "http";
import * as rxjs from "rxjs";
import WebSocket, { CloseEvent, ErrorEvent, MessageEvent } from "ws";
export interface Connection {
    message$: rxjs.Observable<MessageEvent>;
    error$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<CloseEvent>;
    send: WebSocket['send'];
    close: WebSocket['close'];
    keepAlive(interval: number, timeout: number): rxjs.Observable<never>;
}
export declare function connect(address: string | URL, options?: WebSocket.ClientOptions | ClientRequestArgs): rxjs.Observable<Connection>;
