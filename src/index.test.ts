import assert from "assert"
import dotenv from "dotenv"
import { ServiceBroker } from "./index"
import { describe, expect, runAll } from "./test-utils"

dotenv.config()

assert(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL")

const sb = new ServiceBroker({url: process.env.SERVICE_BROKER_URL})


describe("main", ({test}) => {

  test("pub/sub", async () => {
    const queue = new Queue();
    sb.subscribe("test-log", msg => queue.push(msg));

    sb.publish("test-log", "what in the world");
    expect(await queue.shift()).toBe("what in the world");
  });


  test("request/response", async () => {
    const queue = new Queue();
    sb.advertise({name:"test-tts", capabilities:["v1","v2"], priority:1}, msg => {
      queue.push(msg);
      return {
        header: {result:1},
        payload: Buffer.from("this is response payload")
      };
    });

    let promise = sb.request({name:"test-tts", capabilities:["v1"]}, {
      header: {lang:"vi"},
      payload: "this is request payload"
    });
    expectMessage(await queue.shift(), {
      header: {
        type: "ServiceRequest",
        service: {
          name: "test-tts",
          capabilities: ["v1"]
        },
        lang: "vi"
      },
      payload: "this is request payload"
    }, {
      to: false,
      ip: true,
      id: true
    });
    let res = await promise;
    expectMessage(res, {
      header: {
        type: "ServiceResponse",
        result: 1
      },
      payload: Buffer.from("this is response payload")
    }, {
      to: true,
      ip: false,
      id: true
    });

    //test setServiceHandler, requestTo, notifyTo
    const endpointId = res.header!.from;
    sb.setServiceHandler("test-direct", msg => {
      queue.push(msg);
      return {
        header: {output: "crap"},
        payload: Buffer.from("Direct response payload")
      };
    });
    promise = sb.requestTo(endpointId, "test-direct", {
      header: {value: 100},
      payload: "Direct request payload"
    });
    expectMessage(await queue.shift(), {
      header: {
        to: endpointId,
        type: "ServiceRequest",
        service: {name: "test-direct"},
        value: 100
      },
      payload: "Direct request payload"
    }, {
      to: true,
      ip: false,
      id: true
    });
    expectMessage(await promise, {
      header: {
        type: "ServiceResponse",
        output: "crap"
      },
      payload: Buffer.from("Direct response payload")
    }, {
      to: true,
      ip: false,
      id: true
    });
    sb.notifyTo(endpointId, "test-direct", {
      header: {value: 200},
      payload: Buffer.from("Direct notify payload")
    });
    expectMessage(await queue.shift(), {
      header: {
        to: endpointId,
        type: "ServiceRequest",
        service: {name: "test-direct"},
        value: 200
      },
      payload: Buffer.from("Direct notify payload")
    }, {
      to: true,
      ip: false,
      id: false
    });

    //test no-provider
    try {
      await sb.request({name:"test-tts", capabilities:["v3"]}, {
        header: {lang:"en"},
        payload: "this is request payload"
      });
    }
    catch (err: any) {
      expect(err.message).toBe("No provider test-tts");
    }
  });
})



class Queue<T> {
  items: T[];
  waiters: Array<{fulfill: (item: T) => void}>;
  constructor() {
    this.items = [];
    this.waiters = [];
  }
  push(value: T) {
    this.items.push(value);
    while (this.items.length && this.waiters.length) this.waiters.shift()!.fulfill(this.items.shift()!);
  }
  shift(): T|Promise<T> {
    if (this.items.length) return this.items.shift()!;
    else return new Promise(fulfill => this.waiters.push({fulfill}));
  }
}

function expectMessage(
  a: any,
  b: {header: Record<string, unknown>, payload: unknown},
  opts: {to: boolean, ip: boolean, id: boolean}
) {
  assert(typeof a == "object" && a)
  assert(typeof a.header == "object" && a.header)
  assert(typeof a.header.from == "string")
  assert(typeof a.header.to == (opts.to ? "string" : "undefined"))
  assert(typeof a.header.ip == (opts.ip ? "string" : "undefined"))
  assert(typeof a.header.id == (opts.id ? "string" : "undefined"))
  for (const p in b.header) expect(a.header[p]).toEqual(b.header[p])
  expect(a.payload).toEqual(b.payload)
}



runAll()
  .catch(console.error)
  .finally(() => sb.shutdown())
