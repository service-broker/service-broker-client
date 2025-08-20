import { Connection } from "@service-broker/websocket";
import EventEmitter from "events";
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
interface EventMap {
    connect: [];
    close: [number, string];
    error: [unknown];
}
export declare class ServiceBroker extends EventEmitter<EventMap> {
    private opts;
    private readonly connection$;
    private readonly providers;
    private readonly pending;
    private pendingIdGen;
    private readonly shutdown$;
    constructor(opts: {
        url: string;
        websocketOptions?: WebSocket.ClientOptions | ClientRequestArgs;
        authToken?: string;
        retryConfig?: rxjs.RetryConfig;
        repeatConfig?: rxjs.RepeatConfig;
        keepAlive?: {
            pingInterval: number;
            pongTimeout: number;
        };
        streamingChunkSize?: number;
    });
    private onMessage;
    private onServiceRequest;
    private onServiceResponse;
    private onEndpointWaitResponse;
    private messageFromString;
    private messageFromBuffer;
    private send;
    private packetizer;
    advertise(service: {
        name: string;
        capabilities?: string[];
        priority?: number;
    }, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): Promise<Message>;
    unadvertise(serviceName: string): Promise<Message>;
    setServiceHandler(serviceName: string, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): void;
    request(service: {
        name: string;
        capabilities?: string[];
    }, req: Message, timeout?: number): Promise<Message>;
    notify(service: {
        name: string;
        capabilities?: string[];
    }, msg: Message): Promise<void>;
    requestTo(endpointId: string, serviceName: string, req: Message, timeout?: number): Promise<Message>;
    notifyTo(endpointId: string, serviceName: string, msg: Message): Promise<void>;
    private pendingResponse;
    publish(topic: string, text: string): Promise<void>;
    subscribe(topic: string, handler: (text: string) => void): Promise<void>;
    unsubscribe(topic: string): Promise<void>;
    status(): Promise<any>;
    cleanup(): Promise<void>;
    private readonly waitPromises;
    waitEndpoint(endpointId: string): Promise<unknown>;
    shutdown(): void;
    debugGetConnection(): Promise<Connection | null>;
}
export {};
