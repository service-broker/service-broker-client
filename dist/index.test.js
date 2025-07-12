import { describe, expect } from "@service-broker/test-utils";
import assert from "assert";
import dotenv from "dotenv";
import { ServiceBroker } from "./index.js";
dotenv.config();
assert(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL");
const serviceBrokerUrl = process.env.SERVICE_BROKER_URL;
assert(process.env.AUTH_TOKEN, "Missing env AUTH_TOKEN");
const authToken = process.env.AUTH_TOKEN;
describe("main", ({ beforeAll, afterAll, test }) => {
    let sb;
    beforeAll(() => {
        sb = new ServiceBroker({ url: serviceBrokerUrl, authToken });
    });
    afterAll(() => {
        sb.shutdown();
    });
    test("pub/sub", async () => {
        const queue = new Queue();
        sb.subscribe("test-log", msg => queue.push(msg));
        sb.publish("test-log", "what in the world");
        expect(await queue.shift(), "what in the world");
    });
    test("request/response", async () => {
        const queue = new Queue();
        sb.advertise({ name: "test-tts", capabilities: ["v1", "v2"], priority: 1 }, msg => {
            queue.push(msg);
            return {
                header: { result: 1 },
                payload: Buffer.from("this is response payload")
            };
        });
        let promise = sb.request({ name: "test-tts", capabilities: ["v1"] }, {
            header: { lang: "vi" },
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
        const endpointId = res.header.from;
        sb.setServiceHandler("test-direct", msg => {
            queue.push(msg);
            return {
                header: { output: "crap" },
                payload: Buffer.from("Direct response payload")
            };
        });
        promise = sb.requestTo(endpointId, "test-direct", {
            header: { value: 100 },
            payload: "Direct request payload"
        });
        expectMessage(await queue.shift(), {
            header: {
                to: endpointId,
                type: "ServiceRequest",
                service: { name: "test-direct" },
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
            header: { value: 200 },
            payload: Buffer.from("Direct notify payload")
        });
        expectMessage(await queue.shift(), {
            header: {
                to: endpointId,
                type: "ServiceRequest",
                service: { name: "test-direct" },
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
            await sb.request({ name: "test-tts", capabilities: ["v3"] }, {
                header: { lang: "en" },
                payload: "this is request payload"
            });
        }
        catch (err) {
            expect(err.message, "NO_PROVIDER test-tts");
        }
    });
});
class Queue {
    constructor() {
        this.items = [];
        this.waiters = [];
    }
    push(value) {
        this.items.push(value);
        while (this.items.length && this.waiters.length)
            this.waiters.shift().fulfill(this.items.shift());
    }
    shift() {
        if (this.items.length)
            return this.items.shift();
        else
            return new Promise(fulfill => this.waiters.push({ fulfill }));
    }
}
function expectMessage(a, b, opts) {
    assert(typeof a == "object" && a);
    assert(typeof a.header == "object" && a.header);
    assert(typeof a.header.from == "string");
    assert(typeof a.header.to == (opts.to ? "string" : "undefined"));
    assert(typeof a.header.ip == (opts.ip ? "string" : "undefined"));
    assert(typeof a.header.id == (opts.id ? "string" : "undefined"));
    for (const p in b.header)
        expect(a.header[p], b.header[p]);
    expect(a.payload, b.payload);
}
//# sourceMappingURL=index.test.js.map