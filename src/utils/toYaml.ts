export function jsonToYaml(jsonStr: string) {
    const obj = JSON.parse(jsonStr);
    return toYaml(obj, 0);
}


export function toYaml(obj: any, indentLevel = 0): string {
    const indent = "  ".repeat(indentLevel);
    let yamlStr = "";

    if (Array.isArray(obj)) {
        for (const item of obj) {
            if (shouldSkip(item)) continue;
            if (typeof item === "object" && item !== null) {
                yamlStr += `${indent}-\n${toYaml(item, indentLevel + 1)}`;
            } else {
                yamlStr += `${indent}- ${formatValue(item)}\n`;
            }
        }
    } else if (typeof obj === "object" && obj !== null) {
        for (const key in obj) {
            const value = obj[key];
            if (shouldSkip(value)) continue;
            if (Array.isArray(value)) {
                yamlStr += `${indent}${key}:\n${toYaml(value, indentLevel + 1)}`;
            } else if (typeof value === "object") {
                yamlStr += `${indent}${key}:\n${toYaml(value, indentLevel + 1)}`;
            } else {
                yamlStr += `${indent}${key}: ${formatValue(value)}\n`;
            }
        }
    } else {
        yamlStr += `${indent}${formatValue(obj)}\n`;
    }

    return yamlStr;
}

function shouldSkip(value: any): boolean {
    if (value === undefined || value === null) return true;
    if (typeof value === "string" && value.trim() === "") return true;
    if (Array.isArray(value) && value.length === 0) return true;
    if (typeof value === "object" && Object.keys(value).length === 0) return true;
    return false;
}

function formatValue(value: any) {
    if (typeof value === "string") {
        if (value.includes("\n")) {
            return `|-\n  ${value.replace(/\n/g, "\n  ")}`;
        } else {
            return `"${value}"`;
        }
    } else if (typeof value === "number" || typeof value === "boolean") {
        return String(value);
    } else if (value === null) {
        return "null";
    } else {
        return String(value);
    }
}

export default toYaml;