import { ClientRequestArgs } from "http";
import * as rxjs from "rxjs";
import { Readable } from "stream";
import WebSocket from "ws";
export interface Message {
    header?: Record<string, unknown>;
    payload?: string | Buffer | Readable;
}
export interface MessageWithHeader extends Message {
    header: Record<string, unknown>;
}
export interface ClientOptions {
    websocketOptions?: WebSocket.ClientOptions | ClientRequestArgs;
    keepAlive?: {
        pingInterval: number;
        pongTimeout: number;
    };
    streamingChunkSize?: number;
    handle?: (request$: rxjs.Observable<MessageWithHeader>) => rxjs.Observable<RespondAction | ErrorEvent>;
}
export interface Client {
    error$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<WebSocket.CloseEvent>;
    close: WebSocket['close'];
    advertise(opts: {
        services: {
            name: string;
            capabilities?: string[];
            priority?: number;
        }[];
        topics: {
            name: string;
            capabilities?: string[];
        }[];
        authToken?: string;
    }): rxjs.Observable<MessageWithHeader>;
    request(service: {
        name: string;
        capabilities?: string[];
    }, req: Message, timeout?: number): rxjs.Observable<MessageWithHeader>;
    notify(service: {
        name: string;
        capabilities?: string[];
    }, msg: Message): rxjs.Observable<void>;
    requestTo(endpointId: string, serviceName: string, req: Message, timeout?: number): rxjs.Observable<MessageWithHeader>;
    notifyTo(endpointId: string, serviceName: string, msg: Message): rxjs.Observable<void>;
    publish(topic: string, text: string): rxjs.Observable<void>;
    status(): rxjs.Observable<unknown>;
    cleanup(): rxjs.Observable<void>;
    waitEndpoint(endpointId: string): rxjs.Observable<void>;
}
export interface RespondAction {
    type: 'respond';
    request: MessageWithHeader;
    response: Message;
}
export interface ErrorEvent {
    type: 'error';
    error: unknown;
    detail: unknown;
}
export declare function connect(url: string, opts?: ClientOptions): rxjs.Observable<Client>;
