import { MessageWithHeader } from "./index.js";
import * as rxjs from "rxjs";
export declare function serialize({ header, payload }: MessageWithHeader, chunkSize?: number): string | Buffer | rxjs.Observable<string | Buffer>;
export declare function deserialize(data: unknown): MessageWithHeader;
