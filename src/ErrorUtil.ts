import { serializeError } from "serialize-error";

// Reduces an error to a small plain object that is safe to log and to JSON.stringify. Axios errors
// carry the request, socket and agent, which are huge and contain circular references.
export function errorInfo(err: any): any {
    if (err && err.isAxiosError) {
        return {
            message: err.message,
            code: err.code,
            method: err.config?.method,
            url: err.config?.url,
            params: err.config?.params,
            status: err.response?.status,
            response: err.response?.data
        };
    }
    try {
        // Fails on circular structures that serializeError does not resolve, e.g. nested axios errors.
        return JSON.parse(JSON.stringify(serializeError(err)));
    } catch {
        return { message: String(err?.message ?? err) };
    }
}
