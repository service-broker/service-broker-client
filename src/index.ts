import { connect, Connection } from "@service-broker/websocket";
import assert from "assert";
import EventEmitter from "events";
import { ClientRequestArgs } from "http";
import * as rxjs from "rxjs";
import { PassThrough, Readable, Transform } from "stream";
import WebSocket from "ws";


export interface Message {
  header?: Record<string, unknown>
  payload?: string|Buffer|Readable;
}

export interface MessageWithHeader extends Message {
  header: Record<string, unknown>
}

interface Provider {
  service: {
    name: string;
    capabilities?: string[];
    priority?: number;
  };
  handler: (msg: MessageWithHeader) => Message|void|Promise<Message|void>;
  advertise: boolean;
};

const reservedFields: Record<string, void> = {
  from: undefined,
  to: undefined,
  id: undefined,
  type: undefined,
  error: undefined,
  service: undefined,
  part: undefined
};

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

function assertRecord(obj: object): asserts obj is Record<string, unknown> {
}


interface EventMap {
  connect: []
  close: [number, string]
  error: [unknown]
}

export class ServiceBroker extends EventEmitter<EventMap> {
  private readonly connection$: rxjs.Observable<Connection | null>
  private readonly providers: Map<string, Provider>
  private readonly pending: Map<string, (msg: MessageWithHeader) => void>
  private pendingIdGen: number;
  private readonly shutdown$: rxjs.Subject<void>

  constructor(private opts: {
    url: string,
    websocketOptions?: WebSocket.ClientOptions | ClientRequestArgs
    authToken?: string,
    retryConfig?: rxjs.RetryConfig,
    repeatConfig?: rxjs.RepeatConfig,
    keepAlive?: { pingInterval: number, pongTimeout: number },
    streamingChunkSize?: number,
  }) {
    super()
    this.providers = new Map()
    this.pending = new Map()
    this.pendingIdGen = 0;
    this.shutdown$ = new rxjs.ReplaySubject(1)

    this.connection$ = connect(opts.url, opts.websocketOptions).pipe(
      rxjs.tap({
        next: () => this.emit('connect'),
        error: err => this.emit('error', new Error('Fail to connect to service broker', { cause: err }))
      }),
      !opts.retryConfig ? rxjs.identity : rxjs.retry(opts.retryConfig),
      rxjs.exhaustMap(conn => {
        if (this.providers.size) {
          conn.send(
            JSON.stringify({
              authToken: this.opts.authToken,
              type: "SbAdvertiseRequest",
            }) + '\n' +
            JSON.stringify(
              Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)
            )
          )
        }
        for (const endpointId of this.waitPromises.keys()) {
          conn.send(
            JSON.stringify({
              type: 'SbEndpointWaitRequest',
              endpointId
            })
          )
        }
        return rxjs.merge(
          conn.message$.pipe(
            rxjs.tap(event => {
              try {
                this.onMessage(event.data)
              } catch (err) {
                if (err instanceof Error) err.cause = event.data
                this.emit('error', new Error('Fail to handle message', { cause: err }))
              }
            })
          ),
          conn.error$.pipe(
            rxjs.tap(event => this.emit('error', new Error('Connection error', { cause: event.error })))
          ),
          !opts.keepAlive ? rxjs.EMPTY : conn.keepAlive(opts.keepAlive.pingInterval, opts.keepAlive.pongTimeout).pipe(
            rxjs.catchError(err => {
              this.emit('error', new Error('Fail to keep alive', { cause: err }))
              conn.terminate()
              return rxjs.EMPTY
            })
          )
        ).pipe(
          rxjs.takeUntil(
            conn.close$.pipe(
              rxjs.tap(event => {
                this.emit('close', event.code, event.reason)
              })
            )
          ),
          rxjs.finalize(() => conn.close()),
          rxjs.ignoreElements(),
          rxjs.startWith(conn),
          rxjs.endWith(null)
        )
      }),
      !opts.repeatConfig ? rxjs.identity : rxjs.repeat(opts.repeatConfig),
      rxjs.takeUntil(this.shutdown$),
      rxjs.shareReplay(1)
    )
  }

  private onMessage(data: unknown) {
    let msg: MessageWithHeader;
    if (typeof data == "string") msg = this.messageFromString(data);
    else if (Buffer.isBuffer(data)) msg = this.messageFromBuffer(data);
    else throw new Error("Message is not a string or Buffer");
    if (msg.header.type == "SbEndpointWaitResponse") this.onEndpointWaitResponse(msg)
    else if (msg.header.service) this.onServiceRequest(msg);
    else this.onServiceResponse(msg)
  }

  private async onServiceRequest(msg: MessageWithHeader) {
    try {
      assert(typeof msg.header.service == 'object' && msg.header.service != null, 'BAD_ARGS service')
      assertRecord(msg.header.service)
      assert(typeof msg.header.service.name == 'string', 'BAD_ARGS service.name')
      const provider = this.providers.get(msg.header.service.name)
      assert(provider, 'NO_SERVICE ' + msg.header.service.name)
      const res = await provider.handler(msg) || {}
      if (msg.header.id) {
        const header = {
          to: msg.header.from,
          id: msg.header.id,
          type: "ServiceResponse"
        };
        await this.send(Object.assign({}, res.header, reservedFields, header), res.payload);
      }
    }
    catch (err) {
      if (msg.header.id) {
        await this.send({
          to: msg.header.from,
          id: msg.header.id,
          type: "ServiceResponse",
          error: err instanceof Error ? err.message : err
        });
      }
      else {
        this.emit('error', new Error('Unhandled error thrown by notification handler', { cause: err }))
      }
    }
  }

  private onServiceResponse(msg: MessageWithHeader) {
    assert(typeof msg.header.id == 'string', 'BAD_ARGS id')
    const pending = this.pending.get(msg.header.id)
    assert(pending, 'Stray serviceResponse')
    pending(msg)
  }

  private onEndpointWaitResponse(msg: MessageWithHeader) {
    assert(typeof msg.header.endpointId == 'string', 'BAD_ARGS endpointId')
    const waiter = this.waitPromises.get(msg.header.endpointId)
    assert(waiter, 'Stray endpointWaitResponse')
    waiter.next(msg.header.error)
    waiter.complete()
  }

  private messageFromString(str: string): MessageWithHeader {
    assert(str[0] == "{", "Message doesn't have JSON header")
    const index = str.indexOf("\n");
    const headerStr = (index != -1) ? str.slice(0,index) : str;
    const payload = (index != -1) ? str.slice(index+1) : undefined;
    return {
      header: JSON.parse(headerStr),
      payload
    }
  }

  private messageFromBuffer(buf: Buffer): MessageWithHeader {
    assert(buf[0] == 123, "Message doesn't have JSON header")
    const index = buf.indexOf("\n");
    const headerStr = (index != -1) ? buf.slice(0,index).toString() : buf.toString();
    const payload = (index != -1) ? buf.slice(index+1) : undefined;
    return {
      header: JSON.parse(headerStr),
      payload
    }
  }

  private async send(header: Record<string, unknown>, payload?: string|Buffer|Readable) {
    const ws = await rxjs.firstValueFrom(this.connection$)
    assert(ws, "No connection")
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
        const stream = this.packetizer(this.opts.streamingChunkSize || 64_000)
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
    assert(!this.providers.has(service.name), `${service.name} provider already exists`)
    this.providers.set(service.name, {
      service,
      handler,
      advertise: true
    })
    const id = String(++this.pendingIdGen)
    await this.send({
      id,
      authToken: this.opts.authToken,
      type: "SbAdvertiseRequest",
    }, JSON.stringify(
      Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)
    ))
    return this.pendingResponse(id)
  }

  async unadvertise(serviceName: string) {
    assert(this.providers.delete(serviceName), `${serviceName} provider not exists`)
    const id = String(++this.pendingIdGen)
    await this.send({
      id,
      authToken: this.opts.authToken,
      type: "SbAdvertiseRequest",
    }, JSON.stringify(
      Array.from(this.providers.values()).filter(x => x.advertise).map(x => x.service)
    ))
    return this.pendingResponse(id)
  }

  setServiceHandler(serviceName: string, handler: (msg: MessageWithHeader) => Message|void|Promise<Message|void>) {
    assert(!this.providers.has(serviceName), `${serviceName} provider already exists`)
    this.providers.set(serviceName, {
      service: {name: serviceName},
      handler,
      advertise: false
    })
  }



  async request(service: {name: string, capabilities?: string[]}, req: Message, timeout?: number): Promise<Message> {
    const id = String(++this.pendingIdGen);
    const header = {
      id,
      type: "ServiceRequest",
      service
    };
    await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
    return this.pendingResponse(id, timeout)
  }

  async notify(service: {name: string, capabilities?: string[]}, msg: Message): Promise<void> {
    const header = {
      type: "ServiceRequest",
      service
    };
    await this.send(Object.assign({}, msg.header, reservedFields, header), msg.payload);
  }

  async requestTo(endpointId: string, serviceName: string, req: Message, timeout?: number): Promise<Message> {
    const id = String(++this.pendingIdGen);
    const header = {
      to: endpointId,
      id,
      type: "ServiceRequest",
      service: {name: serviceName}
    }
    await this.send(Object.assign({}, req.header, reservedFields, header), req.payload);
    return this.pendingResponse(id, timeout)
  }

  async notifyTo(endpointId: string, serviceName: string, msg: Message): Promise<void> {
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
      this.pending.set(id, res => {
        if (res.header.error) {
          reject(typeof res.header.error == 'string' ? new Error(res.header.error) : res.header.error)
        }
        else {
          if (res.header.part) {
            if (!stream) fulfill({header: res.header, payload: stream = new PassThrough()});
            stream.write(res.payload);
          }
          else {
            this.pending.delete(id)
            if (stream) stream.end(res.payload);
            else fulfill(res);
          }
        }
      })
    });
    return pTimeout(promise, timeout || 30_000)
      .catch(err => {
        this.pending.delete(id)
        throw err;
      });
  }




  async publish(topic: string, text: string) {
    await this.send({
      type: "ServiceRequest",
      service: {name: "#"+topic}
    },
    text);
  }

  async subscribe(topic: string, handler: (text: string) => void) {
    await this.advertise({name: "#"+topic}, (msg: Message) => handler(msg.payload as string));
  }

  async unsubscribe(topic: string) {
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

  private readonly waitPromises = new Map<string, rxjs.Subject<unknown>>()

  async waitEndpoint(endpointId: string) {
    let waiter = this.waitPromises.get(endpointId)
    if (!waiter) {
      await this.send({
        type: "SbEndpointWaitRequest",
        endpointId
      })
      this.waitPromises.set(endpointId, waiter = new rxjs.ReplaySubject())
      waiter.subscribe().add(() => this.waitPromises.delete(endpointId))
    }
    return rxjs.firstValueFrom(waiter)
  }

  shutdown() {
    this.shutdown$.next()
  }

  debugGetConnection() {
    return rxjs.firstValueFrom(this.connection$)
  }
}
