const url = process.argv[2];
const deadline = Date.now() + 120_000;

async function check() {
    try {
        const r = await fetch(url, { method: "GET" });
        if (r.ok || r.status < 500) {
            console.log("Ready:", url);
            process.exit(0);
        }
    } catch {
        /* retry */
    }
    if (Date.now() > deadline) {
        console.error("Timeout waiting for:", url);
        process.exit(1);
    }
    setTimeout(check, 1500);
}

if (!url) {
    console.error("Usage: node wait-for-http.mjs <url>");
    process.exit(1);
}
check();
