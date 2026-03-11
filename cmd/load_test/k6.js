import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

export const http202 = new Counter("http_202");
export const http429 = new Counter("http_429");
export const http400 = new Counter("http_400");
export const http4xx = new Counter("http_4xx");
export const http5xx = new Counter("http_5xx");
export const http0 = new Counter("http_0");

export const options = {
    thresholds: {
        checks: ["rate>0.99"],
        http_req_duration: ["p(95)<300"],
    },
};

function hex(bytes) {
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

function uuidv7() {
    const rnd = new Uint8Array(16);
    crypto.getRandomValues(rnd);

    const ts = Date.now(); // 48-bit unix ms

    rnd[0] = (ts / 0x10000000000) & 0xff;
    rnd[1] = (ts / 0x100000000) & 0xff;
    rnd[2] = (ts / 0x1000000) & 0xff;
    rnd[3] = (ts / 0x10000) & 0xff;
    rnd[4] = (ts / 0x100) & 0xff;
    rnd[5] = ts & 0xff;

    rnd[6] = (0x70 | (rnd[6] & 0x0f)); // version 7
    rnd[8] = (0x80 | (rnd[8] & 0x3f)); // RFC 4122 variant

    const h = hex(rnd);
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

export default function () {
    const queue = __ENV.QUEUE || "queue1";
    const url = `http://localhost:8080/${queue}/newmessage`;
    const payload = JSON.stringify({ text: "load test" });

    const res = http.post(url, payload, {
        headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": uuidv7(),
        },
        timeout: "10s",
    });

    if (__VU === 1 && __ITER < 3) {
        console.log(`status=${res.status} body=${res.body}`);
    }

    if (res.status === 0) http0.add(1);
    else if (res.status === 202) http202.add(1);
    else if (res.status === 429) http429.add(1);
    else if (res.status === 400) http400.add(1);

    if (res.status >= 400 && res.status < 500) http4xx.add(1);
    else if (res.status >= 500) http5xx.add(1);

    check(res, {
        "accepted or busy": (r) => r.status === 202 || r.status === 429,
    });
}