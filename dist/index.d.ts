import { Readable } from "stream";
export interface Message {
    header?: {
        [key: string]: any;
    };
    payload?: string | Buffer | Readable;
}
export interface MessageWithHeader extends Message {
    header: {
        [key: string]: any;
    };
}
interface Logger {
    info: Console["info"];
    error: Console["error"];
}
export declare class ServiceBroker {
    private opts;
    private readonly providers;
    private readonly pending;
    private pendingIdGen;
    private conProvider?;
    private shutdownFlag;
    private logger;
    constructor(opts: {
        url: string;
        logger?: Logger;
        keepAliveIntervalSeconds?: number;
        onConnect?: () => void;
        adminSecret?: string;
        disableReconnect?: boolean;
    });
    private getConnection;
    private connect;
    private onMessage;
    private onServiceRequest;
    private onServiceResponse;
    private messageFromString;
    private messageFromBuffer;
    private send;
    private packetizer;
    advertise(service: {
        name: string;
        capabilities?: string[];
        priority?: number;
    }, handler: (msg: MessageWithHeader) => Message | void | Promise<Message | void>): Promise<void>;
    unadvertise(serviceName: string): Promise<void>;
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
    private wait;
    waitEndpoint(endpointId: string): Promise<void>;
    shutdown(): Promise<void>;
}
export {};
