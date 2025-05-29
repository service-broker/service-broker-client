import assert from "assert";
import * as rxjs from "rxjs";
import { PassThrough, Readable, Transform } from "stream";
import { connect, Connection } from "./websocket";


export interface Message {
  header?: {[key: string]: any};
  payload?: string|Buffer|Readable;
}

export interface MessageWithHeader extends Message {
  header: {[key: string]: any};
}

type Provider = {
  service: {
    name: string;
    capabilities?: string[];
    priority?: number;
  };
  handler: (msg: MessageWithHeader) => Message|void|Promise<Message|void>;
  advertise: boolean;
};

type PendingResponse = {
  process: (msg: MessageWithHeader) => void;
};

const reservedFields: {[key: string]: void} = {
  from: undefined,
  to: undefined,
  id: undefined,
  type: undefined,
  error: undefined,
  service: undefined,
  part: undefined
};

interface Logger {
  info: Console["info"],
  error: Console["error"]
}

function pTimeout<T>(promise: Promise<T>, millis: number): Promise<T> {
  if (millis == Infinity) return promise
  let timer: NodeJS.Timeout
  return Promise.race([
    promise
      .finally(() => clearTimeout(timer)),
    new Promise(f => timer = setTimeout(f, millis))
      .then(() => Promise.reject(new Error("Timeout")))
  ])
}


export class ServiceBroker {
  private readonly connection$: rxjs.Observable<Connection | null>
  private readonly providers: {[key: string]: Provider};
  private readonly pending: {[key: string]: PendingResponse};
  private pendingIdGen: number;
  private logger: Logger;
  private readonly shutdown$: rxjs.Subject<void>

  constructor(private opts: {
    url: string,
    logger?: Logger,
    keepAliveIntervalSeconds?: number,
    onConnect?: () => void,
    authToken?: string,
    disableReconnect?: boolean,
  }) {
    this.providers = {};
    this.pending = {};
    this.pendingIdGen = 0;
    this.logger = opts.logger ?? console
    this.shutdown$ = new rxjs.ReplaySubject(1)

    this.connection$ = connect(opts.url, {
      interval: (opts.keepAliveIntervalSeconds ?? 30) * 1000,
      timeout: 3000
    }).pipe(
      rxjs.concatMap(event => {
        switch (event.type) {
          case 'open':
            this.logger.info("Service broker connection established");
            event.connection.send(
              JSON.stringify({
                authToken: this.opts.authToken,
                type: "SbAdvertiseRequest",
                services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
              })
            )
            this.opts.onConnect?.()
            return rxjs.of(event.connection)
          case 'message':
            try {
              this.onMessage(event.data)
            } catch (err) {
              this.logger.error(err)
            }
            return rxjs.EMPTY
          case 'error':
            this.logger.error(event.error)
            return rxjs.EMPTY
          case 'close':
            this.logger.info("Service broker connection lost,", event.code, event.reason)
            return rxjs.of(null)
        }
      }),
      rxjs.tap({
        error: err => {
          this.logger.info("Failed to connect to service broker,", String(err))
        }
      }),
      opts.disableReconnect ? rxjs.identity : rxjs.retry({ delay: 15000 }),
      opts.disableReconnect ? rxjs.identity : rxjs.repeat({ delay: 1000 }),
      rxjs.takeUntil(this.shutdown$),
      rxjs.shareReplay(1)
    )
  }

  private onMessage(data: unknown) {
    let msg: MessageWithHeader;
    try {
      if (typeof data == "string") msg = this.messageFromString(data);
      else if (Buffer.isBuffer(data)) msg = this.messageFromBuffer(data);
      else throw new Error("Message is not a string or Buffer");
    }
    catch (err) {
      this.logger.error(String(err));
      return;
    }
    if (msg.header.type == "ServiceRequest") this.onServiceRequest(msg);
    else if (msg.header.type == "ServiceResponse") this.onServiceResponse(msg);
    else if (msg.header.type == "SbStatusResponse") this.onServiceResponse(msg);
    else if (msg.header.type == "SbEndpointWaitResponse") this.onServiceResponse(msg);
    else if (msg.header.error) this.onServiceResponse(msg);
    else if (msg.header.service) this.onServiceRequest(msg);
    else this.logger.error("Don't know what to do with message:", msg.header);
  }

  private async onServiceRequest(msg: MessageWithHeader) {
    try {
      if (this.providers[msg.header.service.name]) {
        const res = await this.providers[msg.header.service.name].handler(msg) || {};
        if (msg.header.id) {
          const header = {
            to: msg.header.from,
            id: msg.header.id,
            type: "ServiceResponse"
          };
          await this.send(Object.assign({}, res.header, reservedFields, header), res.payload);
        }
      }
      else throw new Error("No provider for service " + msg.header.service.name);
    }
    catch (err) {
      if (msg.header.id) {
        await this.send({
          to: msg.header.from,
          id: msg.header.id,
          type: "ServiceResponse",
          error: err instanceof Error ? err.message : String(err)
        });
      }
      else this.logger.error(String(err), msg.header);
    }
  }

  private onServiceResponse(msg: MessageWithHeader) {
    if (this.pending[msg.header.id]) this.pending[msg.header.id].process(msg);
    else this.logger.error("Response received but no pending request:", msg.header);
  }

  private messageFromString(str: string): MessageWithHeader {
    if (str[0] != "{") throw new Error("Message doesn't have JSON header");
    const index = str.indexOf("\n");
    const headerStr = (index != -1) ? str.slice(0,index) : str;
    const payload = (index != -1) ? str.slice(index+1) : undefined;
    let header;
    try {
      header = JSON.parse(headerStr);
    }
    catch (err) {
      throw new Error("Failed to parse message header");
    }
    return {header, payload};
  }

  private messageFromBuffer(buf: Buffer): MessageWithHeader {
    if (buf[0] != 123) throw new Error("Message doesn't have JSON header");
    const index = buf.indexOf("\n");
    const headerStr = (index != -1) ? buf.slice(0,index).toString() : buf.toString();
    const payload = (index != -1) ? buf.slice(index+1) : undefined;
    let header;
    try {
      header = JSON.parse(headerStr);
    }
    catch (err) {
      throw new Error("Failed to parse message header");
    }
    return {header, payload};
  }

  private async send(header: {[key: string]: any}, payload?: string|Buffer|Readable) {
    const ws = await rxjs.firstValueFrom(this.connection$)
    if (!ws) throw new Error("No connection")
    const headerStr = JSON.stringify(header);
    if (payload) {
      if (typeof payload == "string") {
        ws.send(headerStr + "\n" + payload);
      }
      else if (Buffer.isBuffer(payload)) {
        const headerLen = Buffer.byteLength(headerStr);
        const tmp = Buffer.allocUnsafe(headerLen +1 +payload.length);
        tmp.write(headerStr);
        tmp[headerLen] = 10;
        payload.copy(tmp, headerLen+1);
        ws.send(tmp);
      }
      else if (payload.pipe) {
        const stream = this.packetizer(64*1000);
        stream.on("data", data => this.send(Object.assign({}, header, {part: true}), data));
        stream.on("end", () => this.send(header));
        payload.pipe(stream);
      }
      else throw new Error("Unexpected");
    }
    else ws.send(headerStr);
  }

  private packetizer(size: number): Transform {
    let buf: Buffer|null;
    let pos: number;
    return new Transform({
      transform: function(chunk: Buffer, encoding, callback) {
        while (chunk.length) {
          if (!buf) {
            buf = Buffer.alloc(size);
            pos = 0;
          }
          const count = chunk.copy(buf, pos);
          pos += count;
          if (pos >= buf.length) {
            this.push(buf);
            buf = null;
          }
          chunk = chunk.slice(count);
        }
        callback();
      },
      flush: function(callback) {
        if (buf) {
          this.push(buf.slice(0, pos));
          buf = null;
        }
        callback();
      }
    });
  }




  async advertise(service: {name: string, capabilities?: string[], priority?: number}, handler: (msg: MessageWithHeader) => Message|void|Promise<Message|void>) {
    assert(service && service.name && handler, "Missing args");
    assert(!this.providers[service.name], `${service.name} provider already exists`);
    this.providers[service.name] = {
      service,
      handler,
      advertise: true
    };
    await this.send({
      authToken: this.opts.authToken,
      type: "SbAdvertiseRequest",
      services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
    });
  }

  async unadvertise(serviceName: string) {
    assert(serviceName, "Missing args");
    assert(this.providers[serviceName], `${serviceName} provider not exists`);
    delete this.providers[serviceName];
    await this.send({
      authToken: this.opts.authToken,
      type: "SbAdvertiseRequest",
      services: Object.values(this.providers).filter(x => x.advertise).map(x => x.service)
    });
  }

  setServiceHandler(serviceName: string, handler: (msg: MessageWithHeader) => Message|void|Promise<Message|void>) {
    assert(serviceName && handler, "Missing args");
    assert(!this.providers[serviceName], `${serviceName} provider already exists`);
    this.providers[serviceName] = {
      service: {name: serviceName},
      handler,
      advertise: false
    };
  }



  async request(service: {name: string, capabilities?: string[]}, req: Message, timeout?: number): Promise<Message> {
    assert(service && service.name && req, "Missing args");
    const id = String(++this.pendingIdGen);
    const promise = this.pendingResponse(id, timeout);
    const header = {
      id,
      type: "ServiceRequest",
      service
    };
    await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
    return promise;
  }

  async notify(service: {name: string, capabilities?: string[]}, msg: Message): Promise<void> {
    assert(service && service.name && msg, "Missing args");
    const header = {
      type: "ServiceRequest",
      service
    };
    await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
  }

  async requestTo(endpointId: string, serviceName: string, req: Message, timeout?: number): Promise<Message> {
    assert(endpointId && serviceName && req, "Missing args");
    const id = String(++this.pendingIdGen);
    const promise = this.pendingResponse(id, timeout);
    const header = {
      to: endpointId,
      id,
      type: "ServiceRequest",
      service: {name: serviceName}
    }
    await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
    return promise;
  }

  async notifyTo(endpointId: string, serviceName: string, msg: Message): Promise<void> {
    assert(endpointId && serviceName && msg, "Missing args");
    const header = {
      to: endpointId,
      type: "ServiceRequest",
      service: {name: serviceName}
    }
    await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
  }

  private pendingResponse(id: string, timeout?: number): Promise<Message> {
    const promise: Promise<Message> = new Promise((fulfill, reject) => {
      let stream: PassThrough;
      this.pending[id] = {
        process: res => {
          if (res.header.error) reject(new Error(res.header.error));
          else {
            if (res.header.part) {
              if (!stream) fulfill({header: res.header, payload: stream = new PassThrough()});
              stream.write(res.payload);
            }
            else {
              delete this.pending[id];
              if (stream) stream.end(res.payload);
              else fulfill(res);
            }
          }
        }
      };
    });
    return pTimeout(promise, timeout || 30*1000)
      .catch(err => {
        delete this.pending[id];
        throw err;
      });
  }




  async publish(topic: string, text: string) {
    assert(topic && text, "Missing args");
    await this.send({
      type: "ServiceRequest",
      service: {name: "#"+topic}
    },
    text);
  }

  async subscribe(topic: string, handler: (text: string) => void) {
    assert(topic && handler, "Missing args");
    await this.advertise({name: "#"+topic}, (msg: Message) => handler(msg.payload as string));
  }

  async unsubscribe(topic: string) {
    assert(topic, "Missing args");
    await this.unadvertise("#"+topic);
  }




  async status() {
    const id = String(++this.pendingIdGen);
    await this.send({
      id,
      type: "SbStatusRequest"
    })
    const res = await this.pendingResponse(id);
    return JSON.parse(res.payload as string);
  }

  async cleanup() {
    await this.send({
      type: "SbCleanupRequest"
    })
  }

  private readonly waitPromises = new Map<string, Promise<void>>()

  private async wait(endpointId: string) {
    const id = String(++this.pendingIdGen);
    await this.send({
      id,
      type: "SbEndpointWaitRequest",
      endpointId
    })
    await this.pendingResponse(id, Infinity);
  }

  waitEndpoint(endpointId: string) {
    let promise = this.waitPromises.get(endpointId)
    if (!promise) this.waitPromises.set(endpointId, promise = this.wait(endpointId).finally(() => this.waitPromises.delete(endpointId)))
    return promise
  }

  shutdown() {
    this.shutdown$.next()
  }
}
