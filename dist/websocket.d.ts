import * as rxjs from "rxjs";
import WebSocket from "ws";
export type Connection = Pick<WebSocket, 'send'>;
export declare function connect(url: string, keepAlive?: {
    interval: number;
    timeout: number;
}): rxjs.Observable<{
    type: "open";
    connection: Connection;
} | {
    type: "message";
    data: WebSocket.Data;
} | {
    type: "error";
    error: any;
} | {
    type: "close";
    code: number;
    reason: string;
} | {
    type: "close";
    code: number;
    reason: string;
}>;
