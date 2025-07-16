import { Connection } from "@service-broker/websocket";
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
export type ServiceBroker = ReturnType<typeof makeClient>;
interface ProvidedService {
    name: string;
    capabilities?: string[];
    priority?: number;
}
interface Provider {
    service: ProvidedService;
    handler(req: MessageWithHeader): Message | void | Promise<Message | void>;
    advertise: boolean;
}
export interface ConnectOptions {
    authToken?: string;
    keepAlive?: {
        pingInterval: number;
        pongTimeout: number;
    };
    streamingChunkSize?: number;
}
type ErrorEvent = {
    type: 'send-error';
    message?: MessageWithHeader;
    error: unknown;
} | {
    type: 'receive-error';
    message: WebSocket.Data;
    error: unknown;
} | {
    type: 'keep-alive-error';
    error: unknown;
} | {
    type: 'socket-error';
    error: unknown;
};
export declare function connect(url: string, opts?: ConnectOptions): rxjs.Observable<{
    event$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<WebSocket.CloseEvent>;
    close: (code?: number, data?: string | Buffer) => void;
    debug: {
        con: Connection;
    };
    advertise(service: {
        name: string;
        capabilities?: string[];
        priority?: number;
    }, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): rxjs.Observable<MessageWithHeader>;
    unadvertise(serviceName: string): rxjs.Observable<MessageWithHeader>;
    setServiceHandler(serviceName: string, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): void;
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
    subscribe(topic: string, handler: (text: string) => void): rxjs.Observable<MessageWithHeader>;
    unsubscribe(topic: string): rxjs.Observable<MessageWithHeader>;
    status(): rxjs.Observable<any>;
    cleanup(): rxjs.Observable<void>;
    waitEndpoint(endpointId: string): rxjs.Observable<void>;
}>;
declare function makeClient(con: Connection, providers: Map<string, Provider>, waitEndpoints: Map<string, {
    closeSubject: rxjs.Subject<void>;
    close$: rxjs.Observable<void>;
}>, { authToken, keepAlive, streamingChunkSize }: ConnectOptions): {
    event$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<WebSocket.CloseEvent>;
    close: (code?: number, data?: string | Buffer) => void;
    debug: {
        con: Connection;
    };
    advertise(service: {
        name: string;
        capabilities?: string[];
        priority?: number;
    }, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): rxjs.Observable<MessageWithHeader>;
    unadvertise(serviceName: string): rxjs.Observable<MessageWithHeader>;
    setServiceHandler(serviceName: string, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): void;
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
    subscribe(topic: string, handler: (text: string) => void): rxjs.Observable<MessageWithHeader>;
    unsubscribe(topic: string): rxjs.Observable<MessageWithHeader>;
    status(): rxjs.Observable<any>;
    cleanup(): rxjs.Observable<void>;
    waitEndpoint(endpointId: string): rxjs.Observable<void>;
};
export {};
