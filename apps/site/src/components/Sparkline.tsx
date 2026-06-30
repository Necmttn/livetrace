/**
 * Tiny live sparkline.
 *
 * Both hooks read their input via a ref instead of an effect dependency,
 * because the input typically changes on every render. Putting it in
 * `useEffect` deps would tear down and re-create the interval before it
 * ever had a chance to fire - which is exactly what made the events/sec
 * readout sit at 0 during a running workflow.
 */
import { useEffect, useRef, useState } from "react";

export function useEventRate(eventCount: number, windowMs = 200): number {
    const [rate, setRate] = useState(0);

    // Latest count read inside the interval via a ref so the interval
    // itself doesn't need to re-mount when the count ticks.
    const countRef = useRef(eventCount);
    countRef.current = eventCount;

    const lastCountRef = useRef(eventCount);
    const lastTimeRef = useRef(Date.now());

    useEffect(() => {
        // Reset baselines on mount so the first tick measures a real delta.
        lastCountRef.current = countRef.current;
        lastTimeRef.current = Date.now();

        const id = setInterval(() => {
            const now = Date.now();
            const dt = now - lastTimeRef.current;
            const current = countRef.current;
            // The demo's visible trace list is TTL-pruned, so the aggregate
            // event count can drop when completed traces age out. A rate cannot
            // be negative; treat counter resets/prunes as zero new events.
            const delta = Math.max(0, current - lastCountRef.current);
            lastCountRef.current = current;
            lastTimeRef.current = now;
            const eps = dt > 0 ? (delta * 1000) / dt : 0;
            // Mild smoothing so the bar doesn't jitter on every micro-burst
            setRate((prev) => prev * 0.55 + eps * 0.45);
        }, windowMs);
        return () => clearInterval(id);
    }, [windowMs]);

    return rate;
}

export function useSparklineSamples(rate: number, capacity = 60): number[] {
    const [samples, setSamples] = useState<number[]>(() => new Array(capacity).fill(0));
    const rateRef = useRef(rate);
    rateRef.current = rate;

    useEffect(() => {
        const id = setInterval(() => {
            setSamples((prev) => {
                const next = prev.slice(1);
                next.push(rateRef.current);
                return next;
            });
        }, 120);
        return () => clearInterval(id);
    }, [capacity]);

    return samples;
}

export function Sparkline({ samples, width = 240, height = 24 }: { samples: ReadonlyArray<number>; width?: number; height?: number }) {
    const max = Math.max(...samples, 4);
    const step = width / Math.max(1, samples.length - 1);
    const points = samples.map((v, i) => {
        const x = i * step;
        const y = height - (v / max) * (height - 2) - 1;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    const linePath = `M ${points.join(" L ")}`;
    const areaPath = `${linePath} L ${width},${height} L 0,${height} Z`;

    return (
        <svg className="sparkline-svg" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
            <path className="area" d={areaPath} />
            <path className="line" d={linePath} />
        </svg>
    );
}
