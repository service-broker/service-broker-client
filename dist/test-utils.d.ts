import assert from "assert/strict";
export declare function describe(suite: string, setup: (opts: {
    beforeEach: Function;
    afterEach: Function;
    test: Function;
}) => void): void;
export declare function runAll(): Promise<void>;
export declare function expect(a: unknown): {
    toBe(b: unknown): void;
    toEqual(b: unknown): void;
    toHaveLength(b: number): void;
    not: {
        toBe(b: unknown): void;
        toEquals(b: unknown): void;
    };
    toThrow(b: string | assert.AssertPredicate): void;
    rejects(b: string | assert.AssertPredicate): Promise<void>;
};
export type MockFunc = (() => void) & {
    mock: {
        calls: Array<unknown>;
    };
};
export declare function mockFunc(): MockFunc;
