export default class Iterator<T> {
    next: () => Promise<T>;
    constructor(next: () => Promise<T>);
    noRace(): Iterator<T>;
    keepWhile(cond: (value: T) => boolean): Iterator<T>;
    throttle(delay: number): Iterator<T>;
}
