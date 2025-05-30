"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.connect = connect;
const rxjs = __importStar(require("rxjs"));
const ws_1 = __importDefault(require("ws"));
function connect(address, options) {
    return rxjs.defer(() => {
        const ws = new ws_1.default(address, options);
        return rxjs.race(rxjs.fromEvent(ws, 'error', (event) => event).pipe(rxjs.map(event => { throw event.error; })), rxjs.fromEvent(ws, 'open').pipe(rxjs.take(1), rxjs.map(() => makeConnection(ws))));
    });
}
function makeConnection(ws) {
    return {
        message$: rxjs.fromEvent(ws, 'message', (event) => event),
        error$: rxjs.fromEvent(ws, 'error', (event) => event),
        close$: rxjs.fromEvent(ws, 'close', (event) => event),
        send: ws.send.bind(ws),
        close: ws.close.bind(ws),
        keepAlive: (interval, timeout) => rxjs.interval(interval).pipe(rxjs.switchMap(() => {
            ws.ping();
            return rxjs.fromEventPattern(h => ws.on('pong', h), h => ws.off('pong', h)).pipe(rxjs.timeout(timeout), rxjs.take(1), rxjs.ignoreElements());
        }))
    };
}
