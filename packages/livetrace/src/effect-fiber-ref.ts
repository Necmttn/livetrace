import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

interface FiberRefCell<A> {
    current: A;
}

/**
 * Minimal compatibility bridge for the v3 `FiberRef` surface that was removed
 * from Effect v4. It preserves the small API this package relies on
 * (`unsafeMake`, `get`, `set`, `unsafeGet`) on top of the v4 `Context.Reference`
 * primitive.
 *
 * Values are stored in the current Effect fiber context. Forked fibers inherit
 * the context snapshot, while unrelated Effect runs fall back to the initial
 * value.
 */
export interface FiberRef<A> {
    readonly reference: Context.Reference<FiberRefCell<A>>;
    readonly fallback: FiberRefCell<A>;
}

let nextId = 0;

export const unsafeMake = <A>(initial: A): FiberRef<A> => {
    const fallback: FiberRefCell<A> = { current: initial };
    return {
        fallback,
        reference: Context.Reference(`@livetrace/effect-fiber-ref/${nextId++}`, {
            defaultValue: () => fallback,
        }),
    };
};

export const get = <A>(self: FiberRef<A>): Effect.Effect<A> => Effect.map(self.reference, (cell) => cell.current);

export const set = <A>(self: FiberRef<A>, value: A): Effect.Effect<void> =>
    Effect.withFiber((fiber) =>
        Effect.sync(() => {
            fiber.setContext(Context.add(fiber.context, self.reference, { current: value }));
        }),
    );

export const unsafeGet = <A>(self: FiberRef<A>): A => {
    const fiber = Fiber.getCurrent();
    return fiber ? Context.getReferenceUnsafe(fiber.context, self.reference).current : self.fallback.current;
};
