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
export interface ConnectOptions {
    keepAlive?: {
        pingInterval: number;
        pongTimeout: number;
    };
    streamingChunkSize?: number;
}
export type ServiceEvent = {
    type: 'service-request';
    request: MessageWithHeader;
    responseSubject: rxjs.Subject<Message | void>;
};
export type ErrorEvent = {
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
    _debug: {
        con: Connection;
    };
    request$: rxjs.Observable<ServiceEvent>;
    error$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<WebSocket.CloseEvent>;
    close: (code?: number, data?: string | Buffer) => void;
    advertise({ services, topics, authToken }: {
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
    status(): rxjs.Observable<any>;
    cleanup(): rxjs.Observable<void>;
    waitEndpoint(endpointId: string): rxjs.Observable<void>;
}>;
declare function makeClient(con: Connection, waitEndpoints: Map<string, {
    closeSubject: rxjs.Subject<void>;
    close$: rxjs.Observable<void>;
}>, opts: ConnectOptions): {
    _debug: {
        con: Connection;
    };
    request$: rxjs.Observable<ServiceEvent>;
    error$: rxjs.Observable<ErrorEvent>;
    close$: rxjs.Observable<WebSocket.CloseEvent>;
    close: (code?: number, data?: string | Buffer) => void;
    advertise({ services, topics, authToken }: {
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
    status(): rxjs.Observable<any>;
    cleanup(): rxjs.Observable<void>;
    waitEndpoint(endpointId: string): rxjs.Observable<void>;
};
export {};
