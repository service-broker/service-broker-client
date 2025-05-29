"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert_1 = __importDefault(require("assert"));
const dotenv_1 = __importDefault(require("dotenv"));
const index_1 = require("./index");
const test_utils_1 = require("./test-utils");
dotenv_1.default.config();
(0, assert_1.default)(process.env.SERVICE_BROKER_URL, "Missing env SERVICE_BROKER_URL");
const sb = new index_1.ServiceBroker({ url: process.env.SERVICE_BROKER_URL });
(0, test_utils_1.describe)("main", ({ test }) => {
    test("pub/sub", async () => {
        const queue = new Queue();
        sb.subscribe("test-log", msg => queue.push(msg));
        sb.publish("test-log", "what in the world");
        (0, test_utils_1.expect)(await queue.shift()).toBe("what in the world");
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
            (0, test_utils_1.expect)(err.message).toBe("NO_PROVIDER test-tts");
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
    (0, assert_1.default)(typeof a == "object" && a);
    (0, assert_1.default)(typeof a.header == "object" && a.header);
    (0, assert_1.default)(typeof a.header.from == "string");
    (0, assert_1.default)(typeof a.header.to == (opts.to ? "string" : "undefined"));
    (0, assert_1.default)(typeof a.header.ip == (opts.ip ? "string" : "undefined"));
    (0, assert_1.default)(typeof a.header.id == (opts.id ? "string" : "undefined"));
    for (const p in b.header)
        (0, test_utils_1.expect)(a.header[p]).toEqual(b.header[p]);
    (0, test_utils_1.expect)(a.payload).toEqual(b.payload);
}
(0, test_utils_1.runAll)()
    .catch(console.error)
    .finally(() => sb.shutdown());
