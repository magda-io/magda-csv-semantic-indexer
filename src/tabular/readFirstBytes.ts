import { openSync, readSync, closeSync } from "fs";

export function readFirstBytes(localPath: string, bytes: number): string {
    const fd = openSync(localPath, "r");
    const buf = Buffer.alloc(bytes);
    const len = readSync(fd, buf, 0, bytes, 0);
    closeSync(fd);
    return buf.toString("utf8", 0, len);
}
