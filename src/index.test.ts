import { describe, expect, objectHaving, oneOf, valueOfType } from "@service-broker/test-utils"
import assert from "assert"
import dotenv from "dotenv"
import * as rxjs from "rxjs"
import { PassThrough, Readable } from "stream"
import { MessageWithHeader, ServiceBroker } from "./index.js"

dotenv.config({ quiet: true })

assert(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL")
assert(process.env.AUTH_TOKEN, "Missing env AUTH_TOKEN")

const serviceBrokerUrl: string = process.env.SERVICE_BROKER_URL
const authToken: string = process.env.AUTH_TOKEN
const localIp = oneOf(['::1', '127.0.0.1'])


describe('config', ({ afterEach, test }) => {
  let sb: ServiceBroker|undefined
  let clientSb: ServiceBroker|undefined

  afterEach(() => {
    sb?.shutdown()
    clientSb?.shutdown()
  })

  test('connect-fail', async () => {
    const queue = makeQueue()
    sb = new ServiceBroker({ url: 'ws://localhost:17038' })
    sb.on('error', err => queue.push(err))
    await sb.status()
      .then(
        () => Promise.reject(new Error('!throwAsExpected')),
        err => expect(err.code, 'ECONNREFUSED')
      )
    expect(queue.take(), objectHaving({ message: 'Fail to connect to service broker' }))
    expect(queue.items, [])
  })

  test('auth-token-fail', async () => {
    sb = new ServiceBroker({ url: serviceBrokerUrl })
    await sb.advertise({ name: 'echo' }, () => { })
      .then(
        () => Promise.reject(new Error('!throwAsExpected')),
        err => expect(err.message, 'FORBIDDEN')
      )
  })

  test('reconnect', async () => {
    const queue = makeQueue()
    sb = new ServiceBroker({ url: serviceBrokerUrl, authToken, repeatConfig: { delay: 250 } })
    sb.on('connect', () => queue.push('connected'))
    await sb.advertise({ name: 'hello' }, req => ({ payload: 'Hello, ' + req.payload }))
    expect(queue.take(), 'connected')
    await sb.debugGetConnection().then(con => con!.close())
    await waitMillis(100)

    //failed request during downtime
    await sb.request({ name: 'hello' }, { payload: 'Jack' }).then(
      () => Promise.reject(new Error("!throwAsExpected")),
      err => expect(err.message, 'No connection')
    )

    //successful request after reconnect
    expect(await queue.wait(), 'connected')
    expect(await sb.request({ name: 'hello' }, { payload: 'John' }), objectHaving({ payload: 'Hello, John' }))
    expect(queue.items, [])
  })

  test('wait-endpoint', async () => {
    const queue = makeQueue()
    sb = new ServiceBroker({ url: serviceBrokerUrl, authToken, repeatConfig: { delay: 250 } })
    sb.on('connect', () => queue.push('connect'))
    await sb.advertise({ name: 'hello' }, req => queue.push(req))
    expect(queue.take(), 'connect')

    //test client disconnect
    clientSb = new ServiceBroker({ url: serviceBrokerUrl })
    await clientSb.request({ name: 'hello' }, { payload: 'text' })
    let req = queue.take<MessageWithHeader>()
    expect(req, objectHaving({ payload: 'text' }))
    sb.waitEndpoint(req.header.from as string).then(err => queue.push(['endpoint-close', err]))
    await waitMillis(100)
    clientSb.shutdown()
    expect(await queue.wait(), ['endpoint-close', undefined])
    expect(queue.items, [])

    //test client disconnect during downtime
    clientSb = new ServiceBroker({ url: serviceBrokerUrl })
    await clientSb.request({ name: 'hello' }, { payload: 'text' })
    req = queue.take<MessageWithHeader>()
    expect(req, objectHaving({ payload: 'text' }))
    sb.waitEndpoint(req.header.from as string).then(err => queue.push(['endpoint-close', err]))
    await waitMillis(100)
    await sb.debugGetConnection().then(con => con!.close())
    clientSb.shutdown()
    expect(await queue.wait(), 'connect')
    expect(await queue.wait(), ['endpoint-close', 'ENDPOINT_NOT_FOUND'])
    expect(queue.items, [])

    //test client disconnect after downtime
    clientSb = new ServiceBroker({ url: serviceBrokerUrl })
    await clientSb.request({ name: 'hello' }, { payload: 'text' })
    req = queue.take<MessageWithHeader>()
    expect(req, objectHaving({ payload: 'text' }))
    sb.waitEndpoint(req.header.from as string).then(err => queue.push(['endpoint-close', err]))
    await waitMillis(100)
    await sb.debugGetConnection().then(con => con!.close())
    expect(await queue.wait(), 'connect')
    clientSb.shutdown()
    expect(await queue.wait(), ['endpoint-close', undefined])
    expect(queue.items, [])
  })

  test('keep-alive', async () => {
    console.info("This test requires autoPong be disabled on the Service Broker")
    const queue = makeQueue()
    sb = new ServiceBroker({ url: serviceBrokerUrl, keepAlive: { pingInterval: 250, pongTimeout: 100 }})
    sb.on('error', err => queue.push(err))
    sb.on('close', (code, reason) => queue.push({ code, reason }))
    sb.debugGetConnection()
    expect(await queue.wait(), objectHaving({ message: 'Fail to keep alive' }))
    expect(await queue.wait(), { code: 1006, reason: '' })
  })
})


describe('pub-sub', ({ beforeAll, afterAll, test }) => {
  let sb: ServiceBroker

  beforeAll(() => {
    sb = new ServiceBroker({ url: serviceBrokerUrl })
  })

  afterAll(() => {
    sb.shutdown()
  })

  test("basic", async () => {
    const queue = makeQueue();
    await sb.subscribe("test-log", msg => queue.push(msg));
    await sb.publish("test-log", "what in the world");
    expect(await queue.wait(), "what in the world");
  });
})


describe("service", ({ beforeAll, afterAll, test }) => {
  let sb: ServiceBroker

  beforeAll(() => {
    sb = new ServiceBroker({ url: serviceBrokerUrl, authToken, streamingChunkSize: 10 })
  })

  afterAll(() => {
    sb.shutdown()
  })

  test("request/response", async () => {
    const queue = makeQueue();

    //advertise
    await sb.advertise({name:"test-tts", capabilities:["v1","v2"], priority:1}, msg => {
      queue.push(msg);
      return {
        header: {result:1},
        payload: Buffer.from("this is response payload")
      };
    });

    //request
    let promise = sb.request({name:"test-tts", capabilities:["v1"]}, {
      header: {lang:"vi"},
      payload: "this is request payload"
    });
    expect(await queue.wait(), {
      header: {
        from: valueOfType('string'),
        ip: localIp,
        id: valueOfType('string'),
        type: "ServiceRequest",
        service: {
          name: "test-tts",
          capabilities: ["v1"]
        },
        lang: "vi"
      },
      payload: "this is request payload"
    })
    let res = await promise;
    expect(res, {
      header: {
        from: valueOfType('string'),
        to: valueOfType('string'),
        id: valueOfType('string'),
        type: "ServiceResponse",
        result: 1
      },
      payload: Buffer.from("this is response payload")
    })

    //setServiceHandler
    const endpointId = res.header!.from as string;
    sb.setServiceHandler("test-direct", msg => {
      queue.push(msg);
      return {
        header: {output: "crap"},
        payload: Buffer.from("Direct response payload")
      };
    });

    //requestTo
    promise = sb.requestTo(endpointId, "test-direct", {
      header: {value: 100},
      payload: "Direct request payload"
    });
    expect(await queue.wait(), {
      header: {
        from: valueOfType('string'),
        to: endpointId,
        id: valueOfType('string'),
        type: "ServiceRequest",
        service: {name: "test-direct"},
        value: 100
      },
      payload: "Direct request payload"
    })
    expect(await promise, {
      header: {
        from: valueOfType('string'),
        to: valueOfType('string'),
        id: valueOfType('string'),
        type: "ServiceResponse",
        output: "crap"
      },
      payload: Buffer.from("Direct response payload")
    })

    //notifyTo
    await sb.notifyTo(endpointId, "test-direct", {
      header: {value: 200},
      payload: Buffer.from("Direct notify payload")
    });
    expect(await queue.wait(), {
      header: {
        from: valueOfType('string'),
        to: endpointId,
        type: "ServiceRequest",
        service: {name: "test-direct"},
        value: 200
      },
      payload: Buffer.from("Direct notify payload")
    })

    //unadvertise
    await sb.unadvertise('test-tts')

    //test no-provider
    try {
      await sb.request({name:"test-tts", capabilities:["v1"]}, {
        header: {lang:"en"},
        payload: "this is request payload"
      });
    }
    catch (err: any) {
      expect(err.message, "NO_PROVIDER test-tts");
    }
  });

  test('streaming', async () => {
    await sb.advertise({ name: 'tts' }, msg => {
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
    })

    const res = await sb.request({ name: 'tts' }, { payload: 'stream request' })
    assert(res.payload instanceof Readable)
    expect(
      await rxjs.lastValueFrom(
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



function makeQueue() {
  const items: unknown[] = []
  const waiters: Function[] = []
  return {
    items,
    push(value: unknown) {
      items.push(value)
      while (items.length && waiters.length) waiters.shift()!(items.shift())
    },
    take<T>(): T {
      assert(items.length, 'Queue empty')
      return items.shift() as T
    },
    wait<T>(): Promise<T> {
      assert(!items.length, 'Queue not empty')
      return new Promise(f => waiters.push(f))
    }
  }
}

function waitMillis(ms: number) {
  return new Promise(f => setTimeout(f, ms))
}
