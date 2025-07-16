import { describe, expect, Expectation } from "@service-broker/test-utils";
import assert from "assert";
import * as rxjs from "rxjs";
import { Readable } from "stream";
import { deserialize, serialize } from "./serialize.js";
function toThrow(expectedErr) {
    return new Expectation('throws', expectedErr, actual => {
        assert(typeof actual == 'function', '!isFunction');
        let throws = true;
        try {
            actual();
            throws = false;
        }
        catch (err) {
            expect(err, expectedErr);
        }
        assert(throws, '!throwsAsExpected');
    });
}
describe('serialize-deserialize', ({ test }) => {
    test('text', () => {
        expect(serialize({ header: { a: 1 }, payload: 'text' }), '{"a":1}\ntext');
        expect(serialize({ header: { b: 2 }, payload: '\r\nsome\ntext' }), '{"b":2}\n\r\nsome\ntext');
        expect(serialize({ header: { c: 3 }, payload: '' }), '{"c":3}\n');
        expect(serialize({ header: { d: 4 } }), '{"d":4}');
        expect(deserialize('{"e":5}\ntext'), { header: { e: 5 }, payload: 'text' });
        expect(deserialize('{"f":6}\n\r\nsome\ntext'), { header: { f: 6 }, payload: '\r\nsome\ntext' });
        expect(deserialize('{"g":7}\n'), { header: { g: 7 }, payload: '' });
        expect(deserialize('{"h":8}'), { header: { h: 8 } });
        expect(() => deserialize('{"i":9}c\ntext'), toThrow({ message: 'Failed to parse message header' }));
    });
    test('binary', () => {
        expect(serialize({ header: { a: 1 }, payload: Buffer.from('binary') }), Buffer.from('{"a":1}\nbinary'));
        expect(serialize({ header: { b: 2 }, payload: Buffer.from('\r\nbin\ndata') }), Buffer.from('{"b":2}\n\r\nbin\ndata'));
        expect(serialize({ header: { c: 3 }, payload: Buffer.from([]) }), Buffer.from('{"c":3}\n'));
        expect(deserialize(Buffer.from('{"e":5}\nbinary')), { header: { e: 5 }, payload: Buffer.from('binary') });
        expect(deserialize(Buffer.from('{"f":6}\n\r\nsome\ntext')), { header: { f: 6 }, payload: Buffer.from('\r\nsome\ntext') });
        expect(deserialize(Buffer.from('{"g":7}\n')), { header: { g: 7 }, payload: Buffer.from([]) });
        expect(() => deserialize(Buffer.from('{"i":9}c\ntext')), toThrow({ message: 'Failed to parse message header' }));
    });
    test('stream', async () => {
        const inputStream = new Readable();
        inputStream.push(Buffer.from('abcdefghijkl'));
        inputStream.push(Buffer.from('mnop'));
        inputStream.push(Buffer.from('qrstuvwxyz1234567890'));
        inputStream.push(null);
        const packet$ = serialize({ header: { a: 1 }, payload: inputStream }, 10);
        assert(rxjs.isObservable(packet$));
        expect(await rxjs.firstValueFrom(packet$.pipe(rxjs.buffer(rxjs.NEVER))), [
            Buffer.from('{"a":1,"part":true}\nabcdefghij'),
            Buffer.from('{"a":1,"part":true}\nklmnopqrst'),
            Buffer.from('{"a":1,"part":true}\nuvwxyz1234'),
            Buffer.from('{"a":1,"part":true}\n567890'),
            '{"a":1}'
        ]);
    });
});
//# sourceMappingURL=serialize.test.js.map