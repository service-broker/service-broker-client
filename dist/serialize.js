import { Readable, Transform } from "stream";
import * as rxjs from "rxjs";
export function serialize({ header, payload }, chunkSize = 64000) {
    if (payload instanceof Readable) {
        return rxjs.defer(() => {
            const stream = payload.pipe(makePacketizer(chunkSize));
            return rxjs.fromEvent(stream, 'data', (chunk) => chunk).pipe(rxjs.map(chunk => serializeSingle({ ...header, part: true }, chunk)), rxjs.takeUntil(rxjs.fromEvent(stream, 'end')), rxjs.endWith(serializeSingle(header)));
        });
    }
    else {
        return serializeSingle(header, payload);
    }
}
function serializeSingle(header, payload) {
    const headerStr = JSON.stringify(header);
    if (typeof payload == 'undefined') {
        return headerStr;
    }
    else if (typeof payload == "string") {
        return headerStr + "\n" + payload;
    }
    else {
        const headerLen = Buffer.byteLength(headerStr);
        const buffer = Buffer.allocUnsafe(headerLen + 1 + payload.length);
        buffer.write(headerStr);
        buffer[headerLen] = 10;
        payload.copy(buffer, headerLen + 1);
        return buffer;
    }
}
function makePacketizer(size) {
    let buf;
    let pos;
    return new Transform({
        transform(chunk, encoding, callback) {
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
                chunk = chunk.subarray(count);
            }
            callback();
        },
        flush(callback) {
            if (buf) {
                this.push(buf.subarray(0, pos));
                buf = null;
            }
            callback();
        }
    });
}
export function deserialize(data) {
    if (typeof data == "string")
        return messageFromString(data);
    else if (Buffer.isBuffer(data))
        return messageFromBuffer(data);
    else
        throw new Error("Message is not a string or Buffer");
}
function messageFromString(str) {
    if (str[0] != "{")
        throw new Error("Message doesn't have JSON header");
    const index = str.indexOf("\n");
    try {
        if (index != -1) {
            return {
                header: JSON.parse(str.slice(0, index)),
                payload: str.slice(index + 1)
            };
        }
        else {
            return {
                header: JSON.parse(str)
            };
        }
    }
    catch (err) {
        throw new Error("Failed to parse message header");
    }
}
function messageFromBuffer(buf) {
    if (buf[0] != 123)
        throw new Error("Message doesn't have JSON header");
    const index = buf.indexOf("\n");
    try {
        if (index != -1) {
            return {
                header: JSON.parse(buf.subarray(0, index).toString()),
                payload: buf.subarray(index + 1)
            };
        }
        else {
            return {
                header: JSON.parse(buf.toString())
            };
        }
    }
    catch (err) {
        throw new Error("Failed to parse message header");
    }
}
//# sourceMappingURL=serialize.js.map