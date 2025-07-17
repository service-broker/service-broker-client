import { describe, expect, Expectation, objectHaving, oneOf, valueOfType } from "@service-broker/test-utils"
import assert from "assert"
import dotenv from "dotenv"
import * as rxjs from "rxjs"
import { PassThrough, Readable } from "stream"
import { connect, ConnectOptions, Message, MessageWithHeader, ServiceBroker } from "./index.js"

dotenv.config()

assert(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL")
const serviceBrokerUrl = process.env.SERVICE_BROKER_URL

assert(process.env.AUTH_TOKEN, "Missing env AUTH_TOKEN")
const authToken = process.env.AUTH_TOKEN

const localIp = oneOf(['::1', '127.0.0.1'])

function lvf<T>(v$: rxjs.Observable<T>) {
  return rxjs.lastValueFrom(v$)
}

function sbConnect(url: string, queue: ReturnType<typeof makeQueue>, opts?: ConnectOptions) {
  return connect(url, opts).pipe(
    rxjs.exhaustMap(sb =>
      rxjs.concat(
        rxjs.of(sb),
        sb.event$.pipe(
          rxjs.tap(event => queue.push('ErrorEvent', event)),
          rxjs.ignoreElements()
        )
      ).pipe(
        rxjs.takeUntil(sb.close$),
        rxjs.finalize(() => sb.close())
      )
    ),
    rxjs.repeat({ delay: 100 }),
  ).subscribe({
    next: sb => queue.push('ServiceBroker', sb),
    error: err => queue.push('Error', err)
  })
}

function makeQueue() {
  interface Item { type: string, value: unknown }
  const queue: Item[] = []
  const waiters: ((item: Item) => void)[] = []
  return {
    push(type: string, value: unknown) {
      queue.push({ type, value })
      while (waiters.length && queue.length)
        waiters.shift()!(queue.shift()!)
    },
    take<T>(type: string) {
      assert(queue.length > 0, 'queueEmpty')
      const item = queue.shift()!
      try {
        expect(item.type, type)
      } catch (err) {
        console.log(item.value)
        throw err
      }
      return item.value as T
    },
    async wait<T>(type: string) {
      assert(queue.length == 0, '!queueEmpty')
      const item = await new Promise<Item>(f => waiters.push(f))
      try {
        expect(item.type, type)
      } catch (err) {
        console.log(item.value)
        throw err
      }
      return item.value as T
    }
  }
}



describe('config', ({ beforeEach, afterEach, test }) => {
  let subs: rxjs.Subscription[]
  let queue: ReturnType<typeof makeQueue>

  beforeEach(() => {
    subs = []
    queue = makeQueue()
  })

  afterEach(() => {
    for (const sub of subs) sub.unsubscribe()
  })

  test('connect-fail', async () => {
    subs.push(sbConnect('ws://localhost:17038', queue))
    expect(await queue.wait('Error'), objectHaving({ code: 'ECONNREFUSED' }))
  })

  test('auth-token-fail', async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const sb = await queue.wait<ServiceBroker>('ServiceBroker')
    await lvf(sb.advertise({ services: [{ name: 'echo' }], topics: [] }))
      .then(
        () => Promise.reject(new Error('!throwAsExpected')),
        err => expect(err.message, 'FORBIDDEN')
      )
  })

  test('wait-endpoint', async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const client = await queue.wait<ServiceBroker>('ServiceBroker')
    subs.push(sbConnect(serviceBrokerUrl, queue, { authToken, handle: req => queue.push('Request', req) }))
    const provider = await queue.wait<ServiceBroker>('ServiceBroker')

    await lvf(provider.advertise({ services: [{ name: 'hello' }], topics: [] }))
    await lvf(client.request({ name: 'hello' }, { payload: 'text' }))
    const req = queue.take<MessageWithHeader>('Request')
    expect(req.payload, 'text')
    const clientEndpointId = req.header.from as string
    provider.waitEndpoint(clientEndpointId).subscribe(() => queue.push('disconnect', 0))

    await new Promise(f => setTimeout(f, 100))
    client.debug.con.close()
    await queue.wait('disconnect')
  })

  test('reconnect-wait-endpoint', async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const client = await queue.wait<ServiceBroker>('ServiceBroker')
    subs.push(sbConnect(serviceBrokerUrl, queue, { authToken, handle: req => queue.push('Request', req) }))
    let provider = await queue.wait<ServiceBroker>('ServiceBroker')

    await lvf(provider.advertise({ services: [{ name: 'hello' }], topics: [] }))
    await lvf(client.request({ name: 'hello' }, { payload: 'text' }))
    const req = queue.take<MessageWithHeader>('Request')
    expect(req.payload, 'text')
    const clientEndpointId = req.header.from as string
    provider.waitEndpoint(clientEndpointId).subscribe(() => queue.push('disconnect', 0))

    await new Promise(f => setTimeout(f, 100))
    provider.debug.con.close()
    provider = await queue.wait<ServiceBroker>('ServiceBroker')
    client.debug.con.close()
    await queue.wait('disconnect')
  })

  test('keep-alive', async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue, { keepAlive: { pingInterval: 250, pongTimeout: 100 } }))
    await queue.wait<ServiceBroker>('ServiceBroker')
    console.log('This test requires autoPong to be disabled on the Service Broker')
    expect(await queue.wait('ErrorEvent'), {
      type: 'keep-alive-error',
      error: new Expectation('instanceOf', 'TimeoutError', actual => assert(actual instanceof rxjs.TimeoutError))
    })
  })

  test('session-recovery', async () => {
    //TODO
  })
})



describe('pub-sub', ({ beforeEach, afterEach, test }) => {
  let subs: rxjs.Subscription[]
  let queue: ReturnType<typeof makeQueue>

  beforeEach(() => {
    subs = []
    queue = makeQueue()
  })

  afterEach(() => {
    for (const sub of subs) sub.unsubscribe()
  })

  test("basic", async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue, { handle: req => queue.push('Message', req) }))
    const subscriber = await queue.wait<ServiceBroker>('ServiceBroker')
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const publisher = await queue.wait<ServiceBroker>('ServiceBroker')

    await lvf(subscriber.advertise({ services: [], topics: [{ name: "test-log" }] }))
    await lvf(publisher.publish("test-log", "what in the world"))
    expect(await queue.wait('Message'), {
      header: {
        from: valueOfType('string'),
        ip: localIp,
        service: { name: '#test-log' }
      },
      payload: "what in the world"
    })
  })
})



describe("service", ({ beforeEach, afterEach, test }) => {
  let subs: rxjs.Subscription[]
  let queue: ReturnType<typeof makeQueue>

  beforeEach(() => {
    subs = []
    queue = makeQueue()
  })

  afterEach(() => {
    for (const sub of subs) sub.unsubscribe()
  })

  test("request-response", async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const client = await queue.wait<ServiceBroker>('ServiceBroker')
    subs.push(sbConnect(serviceBrokerUrl, queue, {
      authToken,
      handle: req => new rxjs.Observable(sub => queue.push('Request', { req, sub }))
    }))
    const provider = await queue.wait<ServiceBroker>('ServiceBroker')

    //advertise
    await lvf(provider.advertise({
      services: [{ name: "test-tts", capabilities: ["v1", "v2"], priority: 1 }],
      topics: []
    }))

    //request
    let promise = lvf(client.request({ name: "test-tts", capabilities: ["v1"] }, {
      header: { lang: "vi" },
      payload: "this is request payload"
    }))

    let { req, sub } = await queue.wait<{ req: MessageWithHeader, sub: rxjs.Subscriber<Message> }>('Request')
    expect(req, {
      header: {
        from: valueOfType('string'),
        id: valueOfType('string'),
        ip: localIp,
        service: {
          name: "test-tts",
          capabilities: ["v1"]
        },
        lang: "vi"
      },
      payload: "this is request payload"
    })

    //respond
    sub.next({
      header: { result: 1 },
      payload: Buffer.from("this is response payload")
    })

    let res = await promise
    expect(res, {
      header: {
        from: valueOfType('string'),
        to: req.header.from,
        id: req.header.id,
        type: "ServiceResponse",
        result: 1
      },
      payload: Buffer.from("this is response payload")
    })

    const clientEndpointId = req.header.from
    const providerEndpointId = res.header!.from as string

    //requestTo
    promise = lvf(client.requestTo(providerEndpointId, "test-direct", {
      header: { value: 100 },
      payload: "Direct request payload"
    }));

    ({ req, sub } = await queue.wait<{ req: MessageWithHeader, sub: rxjs.Subscriber<Message> }>('Request'))
    expect(req, {
      header: {
        from: clientEndpointId,
        to: providerEndpointId,
        id: valueOfType('string'),
        service: { name: "test-direct" },
        value: 100
      },
      payload: "Direct request payload"
    })

    //respond
    sub.next({
      header: { output: "crap" },
      payload: Buffer.from("Direct response payload")
    })

    expect(await promise, {
      header: {
        from: providerEndpointId,
        to: req.header.from,
        id: req.header.id,
        type: "ServiceResponse",
        output: "crap"
      },
      payload: Buffer.from("Direct response payload")
    })

    //notifyTo
    await lvf(client.notifyTo(providerEndpointId, "test-direct", {
      header: { value: 200 },
      payload: Buffer.from("Direct notify payload")
    }));

    ({ req, sub } = await queue.wait<{ req: MessageWithHeader, sub: rxjs.Subscriber<Message> }>('Request'))
    expect(req, {
      header: {
        from: clientEndpointId,
        to: providerEndpointId,
        service: { name: "test-direct" },
        value: 200
      },
      payload: Buffer.from("Direct notify payload")
    })

    sub.next({})

    //unadvertise
    await lvf(provider.advertise({ services: [], topics: [] }))

    //request fail no provider
    await lvf(client.request({ name: "test-tts", capabilities: ["v1"] }, {
      header: { lang: "en" },
      payload: "this is request payload"
    })).then(
      () => Promise.reject(new Error('!throwAsExpected')),
      err => expect(err.message, "NO_PROVIDER test-tts")
    )
  })

  test('streaming', async () => {
    subs.push(sbConnect(serviceBrokerUrl, queue))
    const client = await queue.wait<ServiceBroker>('ServiceBroker')
    subs.push(sbConnect(serviceBrokerUrl, queue, {
      authToken,
      streamingChunkSize: 10,
      handle(msg) {
        assert(msg.payload == 'stream request')
        const stream = new PassThrough()
        const chunks = ['abcdefghijkl', 'mnop', 'qrstuvwxyz1234567890']
        rxjs.from(chunks).pipe(
          rxjs.concatMap(chunk =>
            rxjs.of(chunk).pipe(
              rxjs.concatWith(
                rxjs.timer(100).pipe(
                  rxjs.ignoreElements()
                )
              )
            )
          )
        ).subscribe({
          next: chunk => stream.write(Buffer.from(chunk)),
          complete: () => stream.end()
        })
        return { payload: stream }
      }
    }))
    const provider = await queue.wait<ServiceBroker>('ServiceBroker')
    await lvf(provider.advertise({ services: [{ name: 'tts' }], topics: [] }))

    const res = await lvf(client.request({ name: 'tts' }, { payload: 'stream request' }))
    assert(res.payload instanceof Readable)
    expect(
      await lvf(
        rxjs.fromEvent(res.payload, 'data').pipe(
          rxjs.takeUntil(
            rxjs.fromEvent(res.payload, 'end')
          ),
          rxjs.buffer(rxjs.NEVER)
        )
      ), [
        Buffer.from('abcdefghij'),
        Buffer.from('klmnopqrst'),
        Buffer.from('uvwxyz1234'),
        Buffer.from('567890')
      ]
    )
  })
})
